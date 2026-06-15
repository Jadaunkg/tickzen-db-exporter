# Standalone TickZen Database Exporter - Dockerfile
# Multi-stage build to keep g++/gcc compilers out of the final image

# ============ Stage 1: Build dependencies ============
FROM python:3.11-slim AS builder

WORKDIR /build

# Install build tools needed for compiling C extensions (prophet, scipy, etc.)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    g++ \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements file
COPY requirements.txt ./

# Install Python packages into a temporary directory
RUN pip install --no-cache-dir --prefix=/install -r requirements.txt

# ============ Stage 2: Final runtime image ============
FROM python:3.11-slim

WORKDIR /app

# Install only runtime system libraries (libgomp is needed by Prophet/scikit-learn)
RUN apt-get update && apt-get install -y --no-install-recommends \
    libgomp1 \
    && rm -rf /var/lib/apt/lists/*

# Copy installed packages from builder
COPY --from=builder /install /usr/local

# Copy exporter code files
COPY . .

# Create necessary runtime data and cache directories
RUN mkdir -p generated_data/forecast_cache \
    generated_data/profile_cache \
    generated_data/insider_cache \
    generated_data/peer_cache \
    generated_data/data_cache \
    logs \
    database/logs

# Set environment variables
ENV PYTHONUNBUFFERED=1

# Expose port (required by Render Web Services to pass port scanning checks)
EXPOSE 10000

# Default command: Runs the cron batch update runner
CMD ["python", "database/cron_update_runner.py"]
