# ─────────────────────────────────────────────────────────────────────────────
# Stage 1 — dependency builder
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS builder

WORKDIR /build

# Build-time OS deps (needed to compile some Python wheels)
RUN apt-get update && apt-get install -y --no-install-recommends \
        build-essential \
        gcc \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .

# Install into an isolated prefix so we can copy only what's needed
RUN pip install --upgrade pip \
 && pip install --prefix=/install --no-cache-dir -r requirements.txt


# ─────────────────────────────────────────────────────────────────────────────
# Stage 2 — runtime image
# ─────────────────────────────────────────────────────────────────────────────
FROM python:3.11-slim AS runtime

# libgomp1 is required at runtime by faiss-cpu / torch
RUN apt-get update && apt-get install -y --no-install-recommends \
        libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Non-root user for security
RUN useradd --create-home --shell /bin/bash appuser

WORKDIR /app

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy application source
COPY blip_digital_twin.py .

RUN chown -R appuser:appuser /app
USER appuser

# ─────────────────────────────────────────────────────────────────────────────
# Runtime configuration (override with -e or an env_file)
# ─────────────────────────────────────────────────────────────────────────────
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LOG_LEVEL=INFO \
    AWS_REGION=ap-south-1 \
    INPUT_STREAM=telemetry-raw \
    OUTPUT_STREAM=telemetry-processed \
    STARTING_ITERATOR_TYPE=LATEST \
    SHARD_DISCOVERY_INTERVAL_SEC=60 \
    GET_RECORDS_LIMIT=500 \
    GET_RECORDS_INTERVAL_SEC=1.0 \
    EMPTY_POLL_BACKOFF_SEC=1.0 \
    OUTPUT_BATCH_SIZE=100 \
    OUTPUT_FLUSH_INTERVAL_SEC=1.0 \
    OUTPUT_MAX_RETRIES=5

# No ports to expose — this is a pure background worker

ENTRYPOINT ["python", "blip_digital_twin.py"]
