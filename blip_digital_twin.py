# -*- coding: utf-8 -*-
# !pip install -q transformers accelerate bitsandbytes pdfplumber torch sentencepiece flask flask-cors pyngrok
# !pip install torch flask-ngrok faiss-cpu regex
# !pip install flask flask-cors pyngrok
# !pip install boto3
# !pip install flask-sock simple-websocket websockets


import os
import json
import math
import time
import signal
import logging
import threading
import random
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError

try:
    from dateutil import parser as date_parser  # type: ignore
    _HAS_DATEUTIL = True
except ImportError:
    _HAS_DATEUTIL = False

# LOGGING

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
)
log = logging.getLogger("blip-digital-twin")


# CONFIG (all overridable via environment variables)

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
INPUT_STREAM = os.getenv("INPUT_STREAM", "telemetry-events")
OUTPUT_STREAM = os.getenv("OUTPUT_STREAM", "telemetry-output")

SHARD_DISCOVERY_INTERVAL_SEC = int(os.getenv("SHARD_DISCOVERY_INTERVAL_SEC", "60"))

# Kinesis allows 5 GetRecords calls per shard per second.
GET_RECORDS_LIMIT = int(os.getenv("GET_RECORDS_LIMIT", "500"))
GET_RECORDS_INTERVAL_SEC = float(os.getenv("GET_RECORDS_INTERVAL_SEC", "1.0"))
EMPTY_POLL_BACKOFF_SEC = float(os.getenv("EMPTY_POLL_BACKOFF_SEC", "1.0"))

# Kinesis PutRecords: max 500 records or 5 MB per call.
OUTPUT_BATCH_SIZE = int(os.getenv("OUTPUT_BATCH_SIZE", "100"))
OUTPUT_FLUSH_INTERVAL_SEC = float(os.getenv("OUTPUT_FLUSH_INTERVAL_SEC", "1.0"))
OUTPUT_MAX_RETRIES = int(os.getenv("OUTPUT_MAX_RETRIES", "5"))

# LATEST: only new records arriving after consumer start.
# TRIM_HORIZON: everything still retained in the stream.
STARTING_ITERATOR_TYPE = os.getenv("STARTING_ITERATOR_TYPE", "LATEST")


# BATTERY DEGRADATION MODEL

BATTERY_COEFFICIENTS = {
    "LFP": {
        "cycle_coeff": 0.000015, "calendar_coeff": 0.00000005, "activation_energy": 32000,
        "soh_fade_per_cycle": 0.005,  "calendar_fade_per_year": 1.5,
        "base_internal_resistance": 0.030,
        "cell_overvoltage": 3.65, "cell_undervoltage": 2.50,
    },
    "NMC": {
        "cycle_coeff": 0.000025, "calendar_coeff": 0.00000008, "activation_energy": 28000,
        "soh_fade_per_cycle": 0.0133, "calendar_fade_per_year": 2.0,
        "base_internal_resistance": 0.025,
        "cell_overvoltage": 4.25, "cell_undervoltage": 2.50,
    },
    "NCA": {
        "cycle_coeff": 0.00003,  "calendar_coeff": 0.0000001,  "activation_energy": 26000,
        "soh_fade_per_cycle": 0.020,  "calendar_fade_per_year": 2.5,
        "base_internal_resistance": 0.022,
        "cell_overvoltage": 4.25, "cell_undervoltage": 2.50,
    },
    "Others": {
        "cycle_coeff": 0.00002,  "calendar_coeff": 0.00000007, "activation_energy": 30000,
        "soh_fade_per_cycle": 0.010,  "calendar_fade_per_year": 2.0,
        "base_internal_resistance": 0.028,
        "cell_overvoltage": 4.25, "cell_undervoltage": 2.50,
    },
}

GAS_CONSTANT = 8.314  # J/(mol*K)

# Supported chemistries. Missing battery_type -> NMC default; any unsupported value -> Others.
SUPPORTED_CHEMISTRIES = ("LFP", "NMC", "NCA")
DEFAULT_CHEMISTRY = "NMC"

# Initial-SoH estimate floor. Cycle/calendar history is only a rough starting estimate,
INITIAL_SOH_FLOOR = 50.0

# Charging-state detection thresholds.
IDLE_CURRENT_THRESHOLD_A = 0.10   
FULL_SOC_THRESHOLD = 99.0         
SOC_TREND_THRESHOLD = 0.5         
VOLTAGE_TREND_THRESHOLD = 0.01   
VOLDIF_FAULT_MV = 500.0           

# Per-cell temperature estimation.
CELL_TEMP_DEV_GAIN = 300.0       
CELL_TEMP_MAX_SPREAD = 5.0        
CELL_TEMP_LOAD_GAIN = 0.10       
CELL_TEMP_LOAD_MAX = 6.0          
CELL_TEMP_SMOOTHING = 0.30        

# Per-cell internal-resistance estimation.
IR_GROWTH_K = 1.5                
IR_MIN_FACTOR = 0.5               
IR_MAX_FACTOR = 4.0               
IR_SMOOTHING = 0.30              

# Operating-condition (cycling) aging: scales the chemistry fade-per-cycle by stress.
CYCLE_TEMP_REF_C = 25.0
CYCLE_TEMP_SCALE_C = 20.0           # ~e-fold extra aging per this many degC above reference
CYCLE_DOD_K = 0.5
CYCLE_CRATE_K = 0.3
CYCLE_VSTRESS_K = 5.0
CYCLE_STRESS_CAP = 5.0             # never multiply nominal fade by more than this

# Capacity-based correction of pack SoH: gentle, deadbanded, time-bounded so that
CAP_SOH_DEADBAND_PCT = 3.0               
CAP_CORRECTION_FRACTION = 0.05           
CAP_CORRECTION_RATE_PCT_PER_HOUR = 0.5   

# Per-cell SoH = relative health index anchored to pack SoH (bounded, not accumulated).
CELL_R_PENALTY_PCT = 15.0                
CELL_V_PENALTY_PCT_PER_VOLT = 60.0      
CELL_SOH_MAX_PENALTY_PCT = 20.0          
CELL_SOH_SMOOTHING = 0.05             


def normalize_battery_type(value):
    """Missing/empty -> NMC default; LFP/NMC/NCA pass through; anything else -> Others."""
    if value is None:
        return DEFAULT_CHEMISTRY
    s = str(value).strip().upper()
    if not s:
        return DEFAULT_CHEMISTRY
    if s in SUPPORTED_CHEMISTRIES:
        return s
    return "Others"

DOD_STRESS_EXPONENT1 = 2.0      
CRATE_STRESS_SLOPE1 = 0.7       
CYCLE_TEMP_EA1 = 18000 
PLATING_THRESHOLD_C = 10.0    
PLATING_SEVERITY = 6.0 

def _parse_timestamp(value: Any) -> Optional[datetime]:
    """Parse ISO-8601 or legacy 'MM/DD/YYYY HH:MM:SS AM/PM'; return None if unparseable."""
    if not value:
        return None
    s = str(value).strip()
    if not s:
        return None

    try:
        normalized = s.replace("Z", "+00:00") if s.endswith("Z") else s
        return datetime.fromisoformat(normalized)
    except ValueError:
        pass

    try:
        return datetime.strptime(s, "%m/%d/%Y %I:%M:%S %p")
    except ValueError:
        pass

    if _HAS_DATEUTIL:
        try:
            return date_parser.parse(s)
        except (ValueError, TypeError):
            pass

    return None


def _plating_factor1(temp_c: float, c_rate: float, is_charging: bool) -> float:
    if not is_charging or temp_c >= PLATING_THRESHOLD_C:
        return 1.0
    # Linear in temperature deficit, linear in C-rate (empirical).
    temp_deficit = (PLATING_THRESHOLD_C - temp_c) / 30.0  # 0 at 10C, 1 at -20C
    severity = min(1.0, temp_deficit) * min(2.0, c_rate)
    return 1.0 + (PLATING_SEVERITY - 1.0) * severity


class BatteryDegradationEngine:

    def __init__(self, battery_type=None):
        self.battery_type = normalize_battery_type(battery_type)
        self.coeff = BATTERY_COEFFICIENTS.get(self.battery_type, BATTERY_COEFFICIENTS["Others"])

        # Estimated lazily on the first processed snapshot (CycleCount-driven).
        self.initial_soh: Optional[float] = None
        self.soh_state: Optional[float] = None  
        self.cycle_degradation = 0.0             
        self.calendar_degradation = 0.0          
        self.energy_throughput = 0.0             
        self.last_timestamp: Optional[datetime] = None
        self.last_cell_voltage: Dict[int, float] = {}
        self.cell_temp_smoothed: Dict[int, float] = {}
        self.cell_resistance_smoothed: Dict[int, float] = {}
        self.cell_soh_smoothed: Dict[int, float] = {}

    # Small input helpers
    @staticmethod
    def _to_float(value: Any, default: float) -> float:
        try:
            if value is None or value == "":
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _anchor_temp(self, data: Dict[str, Any]) -> float:
        """Best available pack-temperature anchor: MOS temp, else ambient, else 25C."""
        mos = data.get("MosTemp_c", data.get("MOSTemp_c"))
        t = self._to_float(mos, float("nan"))
        if not math.isnan(t):
            return t
        t = self._to_float(data.get("ambient_temp"), float("nan"))
        if not math.isnan(t):
            return t
        return 25.0

    def _cell_voltages(self, data: Dict[str, Any], total_cells: int) -> List[float]:
        """Collect cell voltages, falling back to last-known then the pack average."""
        pack_voltage = self._to_float(data.get("Voltage(V)", data.get("TotalVol")), 0.0)
        pack_avg = pack_voltage / total_cells if total_cells > 0 else 0.0
        voltages: List[float] = []
        for i in range(1, total_cells + 1):
            raw = data.get(f"Cell_{i}")
            if raw is not None and raw != "":
                try:
                    v = float(raw)
                    self.last_cell_voltage[i] = v
                except (TypeError, ValueError):
                    v = self.last_cell_voltage.get(i, pack_avg)
            else:
                v = self.last_cell_voltage.get(i, pack_avg)
            voltages.append(v)
        return voltages

    def _design_capacity(self, data: Dict[str, Any]) -> float:
        return max(self._to_float(data.get("DesignCap"), 0.0), 0.0)

    def _estimated_full_capacity(self, data: Dict[str, Any]) -> Optional[float]:
        """Approximate present full capacity from RemainCap and SOC (reliable mid-SOC)."""
        soc = self._to_float(data.get("SOC"), 0.0)
        remain = self._to_float(data.get("RemainCap"), 0.0)
        if soc <= 5.0 or remain <= 0.0:
            return None
        return remain / (soc / 100.0)

    def _elapsed_hours(self, data: Dict[str, Any]) -> float:
        """Hours since the previous snapshot, from the Time field. 0 on the first
        snapshot or when timestamps are out of order / non-advancing."""
        ts = _parse_timestamp(data.get("Time")) or datetime.now(timezone.utc)
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)
        if self.last_timestamp is None:
            self.last_timestamp = ts
            return 0.0
        elapsed = (ts - self.last_timestamp).total_seconds()
        if elapsed <= 0:
            if elapsed < 0:
                log.debug("Out-of-order timestamp (elapsed=%.2fs); treating as 0", elapsed)
            return 0.0
        self.last_timestamp = ts
        return elapsed / 3600.0

    # Initial SoH from cycle history (CycleCount is the primary indicator)
    def estimate_initial_soh(self, data: Dict[str, Any]) -> float:
        # Cycle aging: chemistry fade-per-cycle x CycleCount.
        cycle_count = self._to_float(data.get("CycleCount"), 0.0)
        cycle_fade = self.coeff["soh_fade_per_cycle"] * max(cycle_count, 0.0)

        # Calendar aging: degradation accrues with shelf/field age since the
        # production date even when CycleCount is 0.
        calendar_fade = 0.0
        prod = _parse_timestamp(data.get("production_date"))
        now = _parse_timestamp(data.get("Time")) or datetime.now(timezone.utc)
        if prod is not None:
            if prod.tzinfo is not None:
                prod = prod.astimezone(timezone.utc).replace(tzinfo=None)
            if now.tzinfo is not None:
                now = now.astimezone(timezone.utc).replace(tzinfo=None)
            age_years = max((now - prod).total_seconds(), 0.0) / (365.25 * 86400.0)
            calendar_fade = self.coeff["calendar_fade_per_year"] * age_years

        initial = 100.0 - cycle_fade - calendar_fade
        return max(INITIAL_SOH_FLOOR, min(100.0, initial))
    
    # Charging-state inference (current direction + trend + faults)
    def charging_state(self, current: float) -> str:
        """Charging when current is positive, Discharging when negative, and Idle
        when current is within a small dead-band around zero."""
        if current > IDLE_CURRENT_THRESHOLD_A:
            return "Charging"
        if current < -IDLE_CURRENT_THRESHOLD_A:
            return "Discharging"
        return "Idle"

    # Live operating-condition aging (returns the SoH decrement for this step)
    def cycling_increment(self, data: Dict[str, Any], elapsed_hours: float) -> float:
        if elapsed_hours <= 0:
            return 0.0
        current = abs(self._to_float(data.get("Current"), 0.0))
        if current <= 0:
            return 0.0

        cap_ref = self._estimated_full_capacity(data) or self._design_capacity(data) or 1.0
        cap_ref = max(cap_ref, 1e-6)

        delta_ah = current * elapsed_hours
        delta_efc = delta_ah / cap_ref                 # equivalent full cycles this step

        soc = self._to_float(data.get("SOC"), 50.0)
        dod = max(0.0, (100 - soc) / 100)
        c_rate = current / cap_ref
        max_vol = self._to_float(data.get("MaxVol"), 0.0)
        min_vol = self._to_float(data.get("MinVol"), 0.0)
        voltage_stress = max(0.0, max_vol - min_vol)
        anchor_temp = self._anchor_temp(data)
        is_charging = self._to_float(data.get("Current"), 0.0) > 0

        temp_factor = math.exp((anchor_temp - CYCLE_TEMP_REF_C) / CYCLE_TEMP_SCALE_C)
        dod_factor = 1 + CYCLE_DOD_K * dod
        crate_factor = 1 + CYCLE_CRATE_K * c_rate
        vstress_factor = 1 + CYCLE_VSTRESS_K * voltage_stress
        plating = _plating_factor1(anchor_temp, c_rate, is_charging)
        stress = min(CYCLE_STRESS_CAP,
                     temp_factor * dod_factor * crate_factor * vstress_factor * plating)

        inc = self.coeff["soh_fade_per_cycle"] * delta_efc * stress
        self.cycle_degradation += inc
        self.energy_throughput += delta_ah
        return inc

    def calendar_increment(self, data: Dict[str, Any], elapsed_hours: float) -> float:
        if elapsed_hours <= 0:
            return 0.0
        temp = self._anchor_temp(data)
        soc = self._to_float(data.get("SOC"), 50.0)
        temp_kelvin = temp + 273.15
        arrhenius = math.exp(-self.coeff["activation_energy"] / (GAS_CONSTANT * temp_kelvin))
        soc_factor = 1 + (soc / 100)
        inc = self.coeff["calendar_coeff"] * (elapsed_hours * 3600.0) * arrhenius * soc_factor
        self.calendar_degradation += inc
        return inc

    # Per-cell temperature estimate
    def estimate_cell_temperatures(self, voltages: List[float], avg_voltage: float,
                                   anchor_temp: float, current: float) -> Dict[int, float]:
        """Anchor on MOS temp (+ shared load heating); cells that deviate more from
        the average voltage are assumed to run slightly hotter. Offsets are clamped
        and EMA-smoothed to avoid an unrealistic spread."""
        load_rise = min(CELL_TEMP_LOAD_MAX, abs(current) * CELL_TEMP_LOAD_GAIN)
        base = anchor_temp + load_rise
        temps: Dict[int, float] = {}
        for idx, v in enumerate(voltages, start=1):
            offset = (v - avg_voltage) * CELL_TEMP_DEV_GAIN
            offset = max(-CELL_TEMP_MAX_SPREAD, min(CELL_TEMP_MAX_SPREAD, offset))
            raw = base + abs(offset)
            prev = self.cell_temp_smoothed.get(idx)
            smoothed = raw if prev is None else (
                CELL_TEMP_SMOOTHING * raw + (1 - CELL_TEMP_SMOOTHING) * prev
            )
            self.cell_temp_smoothed[idx] = smoothed
            temps[idx] = smoothed
        return temps

    # Per-cell internal resistance estimate (scaled off PACK SoH, not cell SoH)
    def estimate_cell_resistance(self, voltages: List[float], avg_voltage: float,
                                 current_signed: float, pack_soh: float) -> Dict[int, float]:
        """Chemistry base resistance, grown as the pack ages, plus a sign-aware load
        term: under load a cell that sags (discharge) or peaks (charge) relative to
        the average implies higher resistance. Clamped and EMA-smoothed."""
        base_r = self.coeff["base_internal_resistance"]
        growth = 1 + IR_GROWTH_K * max(0.0, (100.0 - pack_soh) / 100.0)
        base = base_r * growth
        resistances: Dict[int, float] = {}
        for idx, v in enumerate(voltages, start=1):
            r = base
            if abs(current_signed) > IDLE_CURRENT_THRESHOLD_A:
                r += (v - avg_voltage) / current_signed
            r = max(base_r * IR_MIN_FACTOR, min(base_r * IR_MAX_FACTOR, r))
            prev = self.cell_resistance_smoothed.get(idx)
            smoothed = r if prev is None else (
                IR_SMOOTHING * r + (1 - IR_SMOOTHING) * prev
            )
            self.cell_resistance_smoothed[idx] = smoothed
            resistances[idx] = smoothed
        return resistances

    # Per-cell SoH: bounded relative-health index anchored to pack SoH
    def calculate_cell_soh(self, voltages: List[float], avg_voltage: float,
                           resistances: Dict[int, float], pack_soh: float) -> Dict[int, float]:
        if resistances:
            avg_r = sum(resistances.values()) / len(resistances)
        else:
            avg_r = self.coeff["base_internal_resistance"]
        avg_r = max(avg_r, 1e-9)

        cell_soh: Dict[int, float] = {}
        for idx, cell_voltage in enumerate(voltages, start=1):
            r = resistances.get(idx, avg_r)
            r_excess = max(0.0, (r - avg_r) / avg_r)            # fractional R above pack avg
            v_below = max(0.0, avg_voltage - cell_voltage)      # volts under the average

            penalty = CELL_R_PENALTY_PCT * r_excess + CELL_V_PENALTY_PCT_PER_VOLT * v_below
            penalty = min(CELL_SOH_MAX_PENALTY_PCT, penalty)

            raw = max(0.0, min(pack_soh, pack_soh - penalty))
            prev = self.cell_soh_smoothed.get(idx)
            smoothed = raw if prev is None else (
                CELL_SOH_SMOOTHING * raw + (1 - CELL_SOH_SMOOTHING) * prev
            )
            self.cell_soh_smoothed[idx] = smoothed
            cell_soh[idx] = smoothed
        return cell_soh

    # Main entrypoint
    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        if self.initial_soh is None:
            self.initial_soh = self.estimate_initial_soh(data)
            self.soh_state = self.initial_soh
            log.info("Estimated initial SoH=%.2f%% (type=%s, cycles=%s)",
                     self.initial_soh, self.battery_type, data.get("CycleCount"))

        total_cells = int(self._to_float(data.get("total_cells"), 0))
        current_signed = self._to_float(data.get("Current"), 0.0)
        max_vol = self._to_float(data.get("MaxVol"), 0.0)
        min_vol = self._to_float(data.get("MinVol"), 0.0)
        anchor_temp = self._anchor_temp(data)

        # --- pack SoH: predict (degradation) then gently correct (capacity) ---
        elapsed_hours = self._elapsed_hours(data)
        self.soh_state -= self.cycling_increment(data, elapsed_hours)
        self.soh_state -= self.calendar_increment(data, elapsed_hours)

        design_cap = self._design_capacity(data)
        est_full = self._estimated_full_capacity(data)
        if est_full is not None and design_cap > 0 and elapsed_hours > 0:
            capacity_soh = max(0.0, min(110.0, (est_full / design_cap) * 100.0))
            soc = self._to_float(data.get("SOC"), 0.0)
            reliability = max(0.0, min(1.0, (soc - 20.0) / 40.0))
            diff = capacity_soh - self.soh_state
            if reliability > 0 and abs(diff) > CAP_SOH_DEADBAND_PCT:
                max_pull = CAP_CORRECTION_RATE_PCT_PER_HOUR * elapsed_hours
                pull = diff * CAP_CORRECTION_FRACTION * reliability
                pull = max(-max_pull, min(max_pull, pull))
                self.soh_state += pull

        self.soh_state = max(0.0, min(100.0, self.soh_state))
        pack_soh = self.soh_state

        # --- cells ---
        voltages = self._cell_voltages(data, total_cells)
        if voltages:
            avg_voltage = sum(voltages) / len(voltages)
        else:
            pack_voltage = self._to_float(data.get("Voltage(V)", data.get("TotalVol")), 0.0)
            avg_voltage = pack_voltage / total_cells if total_cells > 0 else 0.0

        cell_temps = self.estimate_cell_temperatures(voltages, avg_voltage, anchor_temp, current_signed)
        cell_res = self.estimate_cell_resistance(voltages, avg_voltage, current_signed, pack_soh)
        cell_soh = self.calculate_cell_soh(voltages, avg_voltage, cell_res, pack_soh)

        state = self.charging_state(current_signed)

        pack_voltage = self._to_float(data.get("Voltage(V)", data.get("TotalVol")), 0.0)
        est_pack_temp = (sum(cell_temps.values()) / len(cell_temps)) if cell_temps else anchor_temp
        amb_raw = data.get("ambient_temp")
        ambient_temp_c = float(amb_raw) if amb_raw not in (None, "") else None

        output: Dict[str, Any] = {
            "timestamp": str(data.get("Time", "")),
            "battery_type": self.battery_type,
            "charging_state": state,
            "soh": round(pack_soh, 4),
            "cycle_count": self._to_float(data.get("CycleCount"), 0.0),
            "soc": self._to_float(data.get("SOC"), 0.0),
            "voltage_v": pack_voltage,
            "current_a": current_signed,
            "power_w": self._to_float(data.get("Power(W)"), 0.0),
            "remaining_capacity_ah": self._to_float(data.get("RemainCap"), 0.0),
            "ambient_temp_c": ambient_temp_c,
            "estimated_pack_temp_c": round(est_pack_temp, 2),
            "total_cells": total_cells,
        }

        for i in range(1, total_cells + 1):
            output[f"cell_{i}_soh"] = round(cell_soh.get(i, pack_soh), 4)
            output[f"cell_{i}_internal_resistance_mohm"] = round(cell_res.get(i, 0.0) * 1000.0, 3)
            output[f"cell_{i}_temp_c"] = round(cell_temps.get(i, anchor_temp), 2)

        return output


def validate_snapshot(data: Dict[str, Any]) -> None:
    if "total_cells" not in data or data["total_cells"] is None:
        raise ValueError("Missing field: total_cells")

    try:
        n = int(data["total_cells"])
    except (TypeError, ValueError):
        raise ValueError(f"total_cells is not an integer: {data['total_cells']!r}")

    if n <= 0:
        raise ValueError(f"total_cells must be positive: {n}")

    missing_cells = [
        f"Cell_{i}" for i in range(1, n + 1)
        if f"Cell_{i}" not in data or data[f"Cell_{i}"] is None
    ]
    if missing_cells:
        raise ValueError(f"Missing cell voltages: {missing_cells}")

# ENGINE STORE — per-pack engine instances guarded by per-pack locks

class EngineStore:
    def __init__(self) -> None:
        self._engines: Dict[str, BatteryDegradationEngine] = {}
        self._pack_locks: Dict[str, threading.Lock] = {}
        self._registry_lock = threading.Lock()

    def _lock_for(self, pack_id: str) -> threading.Lock:
        lock = self._pack_locks.get(pack_id)
        if lock is not None:
            return lock
        with self._registry_lock:
            lock = self._pack_locks.get(pack_id)
            if lock is None:
                lock = threading.Lock()
                self._pack_locks[pack_id] = lock
            return lock

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        pack_id = str(data.get("pack_id") or data.get("hardware_version") or "default")
        lock = self._lock_for(pack_id)
        with lock:
            engine = self._engines.get(pack_id)
            if engine is None:
                engine = BatteryDegradationEngine(
                    battery_type=data.get("battery_type"),
                )
                self._engines[pack_id] = engine
                log.info("Created engine for pack_id=%s type=%s", pack_id, engine.battery_type)
            result = engine.process(data)
        result["pack_id"] = pack_id
        return result

# OUTPUT PUBLISHER — batched PutRecords with partial-failure retry

class OutputPublisher:

    def __init__(self, kinesis_client, stream_name: str, stop_event: threading.Event):
        self._kinesis = kinesis_client
        self._stream = stream_name
        self._stop = stop_event
        self._buffer: List[Dict[str, Any]] = []
        self._buffer_lock = threading.Lock()
        self._flusher = threading.Thread(target=self._flush_loop, name="output-flusher", daemon=True)

    def start(self) -> None:
        self._flusher.start()

    def enqueue(self, record: Dict[str, Any]) -> None:
        with self._buffer_lock:
            self._buffer.append(record)
            should_flush_now = len(self._buffer) >= OUTPUT_BATCH_SIZE
        if should_flush_now:
            self._flush_once()

    def _drain_buffer(self) -> List[Dict[str, Any]]:
        with self._buffer_lock:
            if not self._buffer:
                return []
            batch = self._buffer[:OUTPUT_BATCH_SIZE]
            self._buffer = self._buffer[OUTPUT_BATCH_SIZE:]
            return batch

    def _flush_once(self) -> None:
        batch = self._drain_buffer()
        if not batch:
            return
        entries = [
            {
                "Data": json.dumps(r).encode("utf-8"),
                "PartitionKey": str(r["pack_id"]),
            }
            for r in batch
        ]
        self._put_records_with_retry(entries)

    def _put_records_with_retry(self, entries: List[Dict[str, Any]]) -> None:
        attempt = 0
        pending = entries
        while pending:
            try:
                response = self._kinesis.put_records(
                    StreamName=self._stream,
                    Records=pending,
                )
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                    attempt += 1
                    if attempt > OUTPUT_MAX_RETRIES:
                        log.error("PutRecords gave up after %d retries; dropping %d records",
                                  attempt, len(pending))
                        return
                    wait = self._backoff(attempt)
                    log.warning("PutRecords throttled (%s); retrying in %.2fs (attempt %d)",
                                code, wait, attempt)
                    time.sleep(wait)
                    continue
                log.exception("PutRecords failed with non-retryable error; dropping %d records",
                              len(pending))
                return
            except Exception:
                log.exception("PutRecords unexpected error; dropping %d records", len(pending))
                return

            failed_count = response.get("FailedRecordCount", 0)
            if failed_count == 0:
                return

            new_pending = []
            for entry, result in zip(pending, response.get("Records", [])):
                if result.get("ErrorCode"):
                    new_pending.append(entry)
            pending = new_pending
            attempt += 1
            if attempt > OUTPUT_MAX_RETRIES:
                log.error("PutRecords gave up after %d retries; dropping %d records",
                          attempt, len(pending))
                return
            wait = self._backoff(attempt)
            log.warning("PutRecords had %d partial failures; retrying %d in %.2fs",
                        failed_count, len(pending), wait)
            time.sleep(wait)

    @staticmethod
    def _backoff(attempt: int) -> float:
        return min(30.0, (2 ** attempt) * 0.1) + random.uniform(0, 0.5)

    def _flush_loop(self) -> None:
        while not self._stop.is_set():
            self._stop.wait(OUTPUT_FLUSH_INTERVAL_SEC)
            try:
                while True:
                    with self._buffer_lock:
                        buf_len = len(self._buffer)
                    if buf_len == 0:
                        break
                    self._flush_once()
                    with self._buffer_lock:
                        if len(self._buffer) >= buf_len:
                            break
            except Exception:
                log.exception("Flush loop error")

        log.info("Output publisher: final flush on shutdown")
        while True:
            with self._buffer_lock:
                if not self._buffer:
                    break
            self._flush_once()

# SHARD CONSUMER — one thread per shard

class ShardConsumer(threading.Thread):
    """Polls a single shard, processes records, enqueues output for publishing."""

    def __init__(
        self,
        kinesis_client,
        stream_name: str,
        shard_id: str,
        engine_store: EngineStore,
        publisher: OutputPublisher,
        stop_event: threading.Event,
    ):
        super().__init__(name=f"shard-{shard_id.split('-')[-1]}", daemon=True)
        self._kinesis = kinesis_client
        self._stream = stream_name
        self.shard_id = shard_id
        self._engines = engine_store
        self._publisher = publisher
        # NOTE: do NOT name this attribute `_stop` — that shadows threading.Thread._stop
        # which is used internally by Thread.join() and breaks shutdown.
        self._stop_event = stop_event
        self.closed = False  # True when shard exhausted; manager picks up children

    def _get_initial_iterator(self) -> Optional[str]:
        try:
            resp = self._kinesis.get_shard_iterator(
                StreamName=self._stream,
                ShardId=self.shard_id,
                ShardIteratorType=STARTING_ITERATOR_TYPE,
            )
            return resp["ShardIterator"]
        except ClientError:
            log.exception("Failed to get initial iterator for shard %s", self.shard_id)
            return None

    def run(self) -> None:
        log.info("Shard consumer starting: %s", self.shard_id)
        iterator = self._get_initial_iterator()
        if iterator is None:
            self.closed = True
            return

        consecutive_errors = 0
        while not self._stop_event.is_set():
            try:
                response = self._kinesis.get_records(
                    ShardIterator=iterator,
                    Limit=GET_RECORDS_LIMIT,
                )
                consecutive_errors = 0
            except ClientError as e:
                code = e.response.get("Error", {}).get("Code", "")
                if code == "ExpiredIteratorException":
                    log.warning("Shard %s iterator expired; refreshing", self.shard_id)
                    iterator = self._get_initial_iterator()
                    if iterator is None:
                        break
                    continue
                if code in ("ProvisionedThroughputExceededException", "ThrottlingException"):
                    consecutive_errors += 1
                    wait = min(5.0, 0.5 * (2 ** consecutive_errors))
                    log.warning("Shard %s throttled (%s); sleeping %.2fs",
                                self.shard_id, code, wait)
                    time.sleep(wait)
                    continue
                log.exception("Shard %s GetRecords failed", self.shard_id)
                consecutive_errors += 1
                time.sleep(min(30.0, 2 ** consecutive_errors))
                continue
            except Exception:
                log.exception("Shard %s unexpected error", self.shard_id)
                consecutive_errors += 1
                time.sleep(min(30.0, 2 ** consecutive_errors))
                continue

            records = response.get("Records", [])
            for record in records:
                if self._stop_event.is_set():
                    break
                self._handle_record(record)

            next_iterator = response.get("NextShardIterator")
            if next_iterator is None:
                log.info("Shard %s closed; consumer exiting", self.shard_id)
                self.closed = True
                return

            iterator = next_iterator

            if not records:
                self._stop_event.wait(EMPTY_POLL_BACKOFF_SEC)
            else:
                self._stop_event.wait(GET_RECORDS_INTERVAL_SEC)

        log.info("Shard consumer stopping: %s", self.shard_id)

    def _handle_record(self, record: Dict[str, Any]) -> None:
        try:
            payload = json.loads(record["Data"].decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            log.error("Shard %s: invalid JSON payload (seq=%s): %s",
                      self.shard_id, record.get("SequenceNumber"), e)
            return

        try:
            validate_snapshot(payload)
        except ValueError as e:
            log.error("Shard %s: validation failed (pack_id=%s seq=%s): %s",
                      self.shard_id, payload.get("pack_id"), record.get("SequenceNumber"), e)
            return

        try:
            result = self._engines.process(payload)
        except Exception:
            log.exception("Shard %s: engine processing failed (pack_id=%s)",
                          self.shard_id, payload.get("pack_id"))
            return

        self._publisher.enqueue(result)

# CONSUMER MANAGER — shard discovery and lifecycle

class ConsumerManager:

    def __init__(self, kinesis_client, stream_name: str):
        self._kinesis = kinesis_client
        self._stream = stream_name
        self._engines = EngineStore()
        self._stop = threading.Event()
        self._consumers: Dict[str, ShardConsumer] = {}
        self._publisher = OutputPublisher(kinesis_client, OUTPUT_STREAM, self._stop)

    def _list_active_shards(self) -> List[str]:
        """Return all OPEN shard IDs (closed shards have an EndingSequenceNumber)."""
        shard_ids: List[str] = []
        next_token: Optional[str] = None
        while True:
            kwargs: Dict[str, Any] = {"MaxResults": 1000}
            if next_token:
                kwargs["NextToken"] = next_token
            else:
                kwargs["StreamName"] = self._stream
            try:
                resp = self._kinesis.list_shards(**kwargs)
            except ClientError:
                log.exception("list_shards failed")
                return shard_ids

            for shard in resp.get("Shards", []):
                seq_range = shard.get("SequenceNumberRange", {})
                if seq_range.get("EndingSequenceNumber") is None:
                    shard_ids.append(shard["ShardId"])

            next_token = resp.get("NextToken")
            if not next_token:
                break
        return shard_ids

    def _refresh_consumers(self) -> None:
        active = set(self._list_active_shards())
        if not active:
            log.warning("No active shards found for stream %s", self._stream)
            return

        for shard_id in list(self._consumers.keys()):
            c = self._consumers[shard_id]
            if c.closed or not c.is_alive():
                log.info("Reaping consumer for shard %s", shard_id)
                del self._consumers[shard_id]

        for shard_id in active:
            if shard_id in self._consumers:
                continue
            log.info("Starting consumer for new shard: %s", shard_id)
            consumer = ShardConsumer(
                kinesis_client=self._kinesis,
                stream_name=self._stream,
                shard_id=shard_id,
                engine_store=self._engines,
                publisher=self._publisher,
                stop_event=self._stop,
            )
            consumer.start()
            self._consumers[shard_id] = consumer

    def run(self) -> None:
        log.info("ConsumerManager starting on stream=%s region=%s", self._stream, AWS_REGION)
        self._publisher.start()
        self._refresh_consumers()

        while not self._stop.is_set():
            self._stop.wait(SHARD_DISCOVERY_INTERVAL_SEC)
            if self._stop.is_set():
                break
            try:
                self._refresh_consumers()
            except Exception:
                log.exception("Shard refresh failed")

        log.info("ConsumerManager: shutting down; waiting for shard consumers...")
        for consumer in self._consumers.values():
            consumer.join(timeout=10)
        log.info("ConsumerManager: stopped")

    def stop(self) -> None:
        self._stop.set()

# ENTRYPOINT

def build_kinesis_client():
    return boto3.client(
        "kinesis",
        region_name=AWS_REGION,
        config=Config(
            retries={"max_attempts": 5, "mode": "adaptive"},
            connect_timeout=5,
            read_timeout=30,
        ),
    )


def main() -> None:
    kinesis = build_kinesis_client()
    manager = ConsumerManager(kinesis, INPUT_STREAM)

    def _shutdown(signum, _frame):
        log.info("Received signal %s; initiating graceful shutdown", signum)
        manager.stop()

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    manager.run()

if __name__ == "__main__":
    main()
