# Use pre-built base image with Cryo already compiled
FROM ghcr.io/gnosischain/cryo-base:20250722

WORKDIR /app

# Create required directories (removed /app/state since we're stateless now)
RUN mkdir -p /app/data /app/logs /app/migrations && \
    chmod 777 /app/data /app/logs /app/migrations

# Copy Python requirements first (for better caching)
COPY requirements.txt /app/

# Install Python dependencies
RUN pip3 install --no-cache-dir --upgrade pip && \
    pip3 install --no-cache-dir -r requirements.txt

# Copy application files
COPY migrations/ /app/migrations/
COPY src/ /app/src/
COPY scripts/ /app/scripts/

# Make scripts executable and ensure they have proper line endings
RUN find /app/scripts -type f -name "*.sh" -exec chmod +x {} \; && \
    find /app/scripts -type f -name "*.sh" -exec sed -i 's/\r$//' {} \;

# Verify Cryo is available from base image
RUN cryo --version || echo "Cryo installation verification"

# Set the entrypoint
ENTRYPOINT ["/bin/bash", "/app/scripts/entrypoint.sh"]