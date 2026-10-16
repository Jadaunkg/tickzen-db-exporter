# TickZen Database Exporter - Dockerfile
# Runs one batch of database/cron_update_runner.py and exits. Built for an Azure Container Apps
# scheduled job (see deploy/azure/), but any scheduler that runs the container works.

# ============ Stage 1: Build dependencies ============
FROM python:3.11-slim AS builder

WORKDIR /build

# Build tools for C extensions (prophet, scipy, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ============ Stage 2: Final runtime image ============
FROM python:3.11-slim

WORKDIR /app

# libgomp: Prophet/scikit-learn runtime. tzdata: TZ below.
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

COPY . .

RUN mkdir -p generated_data/forecast_cache \
    generated_data/profile_cache \
    generated_data/insider_cache \
    generated_data/peer_cache \
    generated_data/data_cache \
    logs \
    database/logs

# The pipeline writes last_sync_date and other dates with the local clock, exactly as the Mac runner
# does in IST. Keep the container on the same clock so "synced today" means the same day.
ENV TZ=Asia/Kolkata \
    PYTHONUNBUFFERED=1

CMD ["python", "database/cron_update_runner.py"]
