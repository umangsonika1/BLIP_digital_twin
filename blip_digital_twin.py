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


# ============================================================================
# LOGGING
# ============================================================================

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s [%(levelname)s] [%(threadName)s] %(name)s: %(message)s",
)
log = logging.getLogger("blip-digital-twin")


# ============================================================================
# CONFIG (all overridable via environment variables)
# ============================================================================

AWS_REGION = os.getenv("AWS_REGION", "ap-south-1")
INPUT_STREAM = os.getenv("INPUT_STREAM", "telemetry-raw")
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


# ============================================================================
# BATTERY DEGRADATION MODEL
# ============================================================================

BATTERY_COEFFICIENTS = {
    "LFP":    {"cycle_coeff": 0.000015, "calendar_coeff": 0.00000005, "activation_energy": 32000},
    "NMC":    {"cycle_coeff": 0.000025, "calendar_coeff": 0.00000008, "activation_energy": 28000},
    "NCA":    {"cycle_coeff": 0.00003,  "calendar_coeff": 0.0000001,  "activation_energy": 26000},
    "Others": {"cycle_coeff": 0.00002,  "calendar_coeff": 0.00000007, "activation_energy": 30000},
}

GAS_CONSTANT = 8.314  # J/(mol*K)
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
    """
    Stateful per-pack degradation calculator. NOT thread-safe by itself —
    callers must hold a per-pack lock when calling process(). EngineStore
    handles that.
    """

    def __init__(self, battery_type: str = "Others", initial_soh: float = 100.0):
        self.battery_type = battery_type
        self.initial_soh = float(initial_soh)
        self.coeff = BATTERY_COEFFICIENTS.get(battery_type, BATTERY_COEFFICIENTS["Others"])
        self.cycle_degradation = 0.0
        self.calendar_degradation = 0.0
        self.energy_throughput = 0.0
        self.last_timestamp: Optional[datetime] = None
        self.temperature_history = deque(maxlen=1000)
        self.soc_history = deque(maxlen=1000)
        self.cell_degradation: Dict[int, float] = {}
        self.last_cell_voltage: Dict[int, float] = {}
        self.last_cell_temp: Dict[int, float] = {}

    @staticmethod
    def charging_state(current: float) -> str:
        if current > 0:
            return "Charging"
        if current < 0:
            return "Discharging"
        return "Idle"

    def cycling_aging(self, data: Dict[str, Any]) -> None:
        soc = float(data.get("SOC", 50))
        current = abs(float(data.get("Current", 0)))
        power = abs(float(data.get("Power(W)", 0)))
        remain_cap = float(data.get("RemainCap", 1))
        full_cap = float(data.get("FullCap", 1))
        max_vol = float(data.get("MaxVol", 0))
        min_vol = float(data.get("MinVol", 0))
        avg_temp = float(data.get("MTemp", 25))

        dod = (100 - soc) / 100
        c_rate = current / max(full_cap, 1)
        capacity_fade = max(0.0, (full_cap - remain_cap) / max(full_cap, 1))
        voltage_stress = max_vol - min_vol
        temperature_factor = math.exp((avg_temp - 25) / 30)

        self.energy_throughput += power / 3600
        throughput_factor = 1 + self.energy_throughput / (full_cap * max(max_vol, 1) * 1000)

        incremental_deg = (
            self.coeff["cycle_coeff"]
            * (1 + dod)
            * (1 + c_rate)
            * (1 + capacity_fade)
            * (1 + voltage_stress)
            * temperature_factor
            * throughput_factor
            * (power / 1000)
            / 3600
        )
        self.cycle_degradation += incremental_deg

    def calendar_aging(self, data: Dict[str, Any]) -> None:
        ts = _parse_timestamp(data.get("Time")) or datetime.now(timezone.utc)
        if ts.tzinfo is not None:
            ts = ts.astimezone(timezone.utc).replace(tzinfo=None)

        if self.last_timestamp is None:
            self.last_timestamp = ts
            return

        elapsed = (ts - self.last_timestamp).total_seconds()
        if elapsed < 0:
            log.debug("Out-of-order timestamp (elapsed=%.2fs); skipping calendar increment", elapsed)
            return
        self.last_timestamp = ts

        temp = float(data.get("MTemp", 25))
        soc = float(data.get("SOC", 50))
        temp_kelvin = temp + 273.15
        arrhenius = math.exp(-self.coeff["activation_energy"] / (GAS_CONSTANT * temp_kelvin))
        soc_factor = 1 + (soc / 100)

        self.calendar_degradation += (
            self.coeff["calendar_coeff"] * elapsed * arrhenius * soc_factor
        )

    def calculate_cell_soh(self, data: Dict[str, Any], pack_soh: float) -> Dict[str, Optional[float]]:
        total_cells = int(data.get("total_cells", 0))
        cell_soh: Dict[str, Optional[float]] = {}

        avg_voltage = float(data.get("AverageVol", 0))
        voltage_diff = float(data.get("Voldif", 0))
        mtemp = float(data.get("MTemp", 25))

        for i in range(1, total_cells + 1):

            cell_voltage_raw = data.get(f"Cell_{i}")
            cell_temp_raw = data.get(f"Temp{i}")

            # -------------------------------
            # Use last known values if missing
            # -------------------------------
            if cell_voltage_raw not in (None, ""):
                try:
                    cell_voltage = float(cell_voltage_raw)
                    self.last_cell_voltage[i] = cell_voltage
                except (TypeError, ValueError):
                    cell_voltage = self.last_cell_voltage.get(i)
            else:
                cell_voltage = self.last_cell_voltage.get(i)

            if cell_temp_raw not in (None, ""):
                try:
                    cell_temp = float(cell_temp_raw)
                    self.last_cell_temp[i] = cell_temp
                except (TypeError, ValueError):
                    cell_temp = self.last_cell_temp.get(i)
            else:
                cell_temp = self.last_cell_temp.get(i)

            # If no historical value exists yet, use pack averages
            if cell_voltage is None:
                cell_voltage = avg_voltage

            if cell_temp is None:
                cell_temp = mtemp

            if i not in self.cell_degradation:
                self.cell_degradation[i] = 0.0

            voltage_deviation = abs(cell_voltage - avg_voltage)
            temp_deviation = abs(cell_temp - mtemp)

            incremental = (0.000001 + voltage_deviation * 0.000002 + temp_deviation * 0.000001 + voltage_diff * 0.000001)
            self.cell_degradation[i] += incremental
            est = pack_soh - self.cell_degradation[i]
            est = max(0.0, min(pack_soh, est))

            cell_soh[f"cell_{i}_soh"] = round(est, 4)

        return cell_soh

    def process(self, data: Dict[str, Any]) -> Dict[str, Any]:
        self.cycling_aging(data)
        self.calendar_aging(data)

        # total_soh = self.initial_soh - self.cycle_degradation - self.calendar_degradation
        # total_soh = max(0.0, min(100.0, total_soh))

        model_soh = self.initial_soh - self.cycle_degradation - self.calendar_degradation
        design_cap = float(data.get("DesignCap", 1))
        # capacity_soh = (float(data.get("FullCap", 0))/ max(float(data.get("DesignCap", 1)), 1)) * 100
        if design_cap > 0:
            capacity_soh = (float(data.get("FullCap", 0))/ design_cap) * 100
        else:
            capacity_soh = model_soh
        total_soh = max(model_soh, capacity_soh)
        total_soh = max(0.0, min(100.0, total_soh))

        current = float(data.get("Current", 0))
        output: Dict[str, Any] = {
            "timestamp": str(data.get("Time", "")),
            "soh": round(total_soh, 4),
            "battery_capacity_ah": float(data.get("FullCap", 0)),
            "current_a": current,
            "power_w": float(data.get("Power(W)", 0)),
            "voltage_v": float(data.get("Voltage(V)", data.get("TotalVol", 0))),
            "temperature_c": float(data.get("MTemp", 0)),
            "charging_state": self.charging_state(current),
            "total_cells": int(data.get("total_cells", 0)),
        }
        output.update(self.calculate_cell_soh(data, total_soh))
        return output


def validate_snapshot(data: Dict[str, Any]) -> None:
    """Raise ValueError if the snapshot is missing required fields."""
    required = [
        "pack_id", "battery_type", "total_cells", "Time", "Current", "SOC",
        "FullCap", "RemainCap", "DesignCap", "Power(W)", "MTemp", "MaxVol", "MinVol",
        "AverageVol", "Voldif",
    ]
    missing = [f for f in required if f not in data or data[f] is None]
    if missing:
        raise ValueError(f"Missing fields: {missing}")

    try:
        n = int(data["total_cells"])
    except (TypeError, ValueError):
        raise ValueError(f"total_cells is not an integer: {data['total_cells']!r}")

    for i in range(1, n + 1):
        if f"Cell_{i}" not in data:
            raise ValueError(f"Missing Cell_{i}")
        if f"Temp{i}" not in data:
            raise ValueError(f"Missing Temp{i}")


# ============================================================================
# ENGINE STORE — per-pack engine instances guarded by per-pack locks
# ============================================================================

class EngineStore:
    """
    Thread-safe registry of per-pack engines. Each pack_id gets its own lock
    so different packs run in parallel across shard threads, while records
    for the same pack are processed serially.
    """

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
        pack_id = str(data["pack_id"])
        lock = self._lock_for(pack_id)
        with lock:
            engine = self._engines.get(pack_id)
            if engine is None:
                engine = BatteryDegradationEngine(
                    battery_type=data.get("battery_type", "Others"),
                    initial_soh=float(data.get("initial_soh", 100)),
                )
                self._engines[pack_id] = engine
                log.info("Created engine for pack_id=%s type=%s", pack_id, engine.battery_type)
            result = engine.process(data)
        result["pack_id"] = pack_id
        return result


# ============================================================================
# OUTPUT PUBLISHER — batched PutRecords with partial-failure retry
# ============================================================================

class OutputPublisher:
    """
    Buffers processed snapshots and flushes them to OUTPUT_STREAM in batches
    using put_records. A background thread flushes on a time threshold;
    enqueue() also triggers an immediate flush when the buffer reaches
    OUTPUT_BATCH_SIZE. Partial failures are retried with exponential
    backoff + jitter.
    """

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


# ============================================================================
# SHARD CONSUMER — one thread per shard
# ============================================================================

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


# ============================================================================
# CONSUMER MANAGER — shard discovery and lifecycle
# ============================================================================

class ConsumerManager:
    """
    Owns the set of ShardConsumer threads. Periodically re-lists shards and
    starts consumers for any new ones (e.g. children from a reshard). Reaps
    consumers whose shards have closed.
    """

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


# ============================================================================
# ENTRYPOINT
# ============================================================================

def build_kinesis_client():
    """
    Build a boto3 Kinesis client. Credentials are picked up from the standard
    AWS chain (env vars, EC2/ECS/Lambda role, ~/.aws/credentials). Adaptive
    retry mode handles transient throttling at the SDK layer.
    """
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
