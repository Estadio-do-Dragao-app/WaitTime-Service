# Autonomous Dockerfile for WaitTime-Service
FROM python:3.10-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE 1
ENV PYTHONUNBUFFERED 1
ENV PYTHONPATH=/app

WORKDIR /app

# Install system dependencies and create non-root user
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && useradd -m -u 1000 appuser

# Copy locked requirements
COPY requirements.lock.txt /app/requirements.lock.txt
RUN pip install --no-cache-dir -r /app/requirements.lock.txt

# Copy only necessary service files (avoid sensitive data leakage)
COPY app.py consumer.py schemas.py queueModel.py ./
COPY config/ ./config/
COPY db/ ./db/
COPY services/ ./services/

RUN chown -R appuser:appuser /app

# Switch to non-root user
USER appuser


# Default command
CMD ["python", "app.py"]
