# syntax=docker/dockerfile:1.7

FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PYTHONPATH=/app \
    NUBRA_AUTH_DIR=/tmp/auth \
    TZ=Asia/Kolkata

WORKDIR /app

# System dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    curl \
    tzdata \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt .

RUN pip install --upgrade pip \
    && pip install --index-url https://test.pypi.org/simple/ \
       --extra-index-url https://pypi.org/simple \
       nubra-sdk \
    && pip install -r requirements.txt

# Copy app (instrument master CSV is optional; mount or let InstrumentManager fetch)
COPY app ./app
COPY jobs ./jobs
COPY bootstrap_auth.py ./bootstrap_auth.py
COPY instrument_master_cache.csv ./instrument_master_cache.csv

# Non-root user
RUN useradd --create-home --shell /bin/bash appuser \
    && mkdir -p /tmp/auth \
    && chown -R appuser:appuser /app /tmp/auth
USER appuser

EXPOSE 8080

HEALTHCHECK --interval=30s --timeout=5s --start-period=20s --retries=3 \
  CMD curl -fsS http://localhost:${PORT:-8080}/health || exit 1

CMD ["sh", "-c", "uvicorn app.main:app --host 0.0.0.0 --port ${PORT:-8080}"]
