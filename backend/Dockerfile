FROM python:3.12-slim

WORKDIR /app

# Install system dependencies (compiler utilities for performance optimizations)
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Copy python dependency requirements files first to leverage build cache
COPY requirements.txt ./requirements.txt

# Upgrade pip and install core agent and backend dependencies
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r ./requirements.txt

# Copy the entire backend directory (which now contains agent/)
COPY . .

# Expose FastAPI listening port
EXPOSE 8000

# Set PYTHONPATH to root /app and internal directories to resolve package paths natively
ENV PYTHONPATH=/app:/app/agent:/app/agent/providers

# Start the uvicorn API gateway listening on all network interfaces
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
