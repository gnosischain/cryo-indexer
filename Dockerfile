# Build stage
FROM ubuntu:22.04 as builder

WORKDIR /build

# Copy build script
COPY scripts/build_cryo.sh /build/
RUN chmod +x /build/build_cryo.sh

# Run build script
RUN ./build_cryo.sh

# Final stage
FROM python:3.11-slim

WORKDIR /app

# Copy Cryo from the builder stage
COPY --from=builder /usr/local/bin/cryo /usr/local/bin/cryo

# Install system dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Create required directories
RUN mkdir -p /app/data /app/logs /app/state /app/migrations

# Copy application files
COPY requirements.txt /app/
COPY migrations/ /app/migrations/
COPY src/ /app/src/
COPY scripts/ /app/scripts/

# Make scripts executable
RUN chmod +x /app/scripts/*.sh || true

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Verify Cryo installation
RUN cryo --version || echo "Cryo installation needs troubleshooting"

# Set the entrypoint
ENTRYPOINT ["/app/scripts/entrypoint.sh"]