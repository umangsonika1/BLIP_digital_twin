# syntax=docker/dockerfile:1.6

# ---------- Base image ----------
FROM python:3.11-slim AS base

# Prevent Python from writing .pyc files and enable unbuffered logging
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

# System deps (minimal). build-essential is only needed if a wheel must be compiled.
RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

# ---------- App setup ----------
WORKDIR /app

# Install Python dependencies first (better layer caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY blip_digital_twin.py .

# ---------- Runtime config ----------
# Default: production mode, no ngrok. Override with -e USE_NGROK=true -e NGROK_AUTH_TOKEN=...
ENV PORT=5008 \
    USE_NGROK=false

EXPOSE 5008

# Healthcheck hits the /health endpoint defined in the app
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -fsS http://localhost:${PORT}/health || exit 1

# Run with gunicorn + gevent-websocket worker so flask-sock WebSockets work.
# Single worker keeps in-memory engine state (client_engines dict) consistent.
CMD ["sh", "-c", "gunicorn -k geventwebsocket.gunicorn.workers.GeventWebSocketWorker -w 1 -b 0.0.0.0:${PORT} --timeout 120 blip_digital_twin:app"]
