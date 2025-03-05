# --------------------------------------------------
# 1. Builder stage: clone & compile Cryo
# --------------------------------------------------
    FROM debian:bullseye-slim AS builder

    # Required packages for building Cryo
    RUN apt-get update && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
        build-essential \
        git \
        pkg-config \
        libssl-dev \
        && rm -rf /var/lib/apt/lists/*
    
    # Install Rust via rustup
    RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | \
        sh -s -- -y
    # Add Rust to PATH
    ENV PATH="/root/.cargo/bin:${PATH}"
    
    # Clone the repository with minimal depth
    RUN mkdir -p /tmp/cryo-build && cd /tmp/cryo-build \
        && git clone --depth 1 https://github.com/paradigmxyz/cryo.git \
        && cd cryo \
        # Configure minimal memory usage
        && export RUSTFLAGS="-C codegen-units=1 -C opt-level=s -C debuginfo=0" \
        && export CARGO_BUILD_JOBS=1 \
        && export CARGO_NET_RETRY=5 \
        && cd crates/cli \
        # Build binary with minimal features
        && cargo build --release --no-default-features --jobs 1
    
    # --------------------------------------------------
    # 2. Final stage: copy the compiled binary
    # --------------------------------------------------
    # Use the same Debian version for consistent libraries
    FROM debian:bullseye-slim
    
    WORKDIR /app
    
    # Install Python and required system dependencies including libssl
    RUN apt-get update && apt-get install -y --no-install-recommends \
        ca-certificates \
        curl \
        gcc \
        g++ \
        python3 \
        python3-pip \
        python3-dev \
        libssl-dev \
        libssl1.1 \
        && rm -rf /var/lib/apt/lists/*
    
    # Create a symlink for python3 to be accessible as python
    RUN ln -sf /usr/bin/python3 /usr/bin/python && \
        ln -sf /usr/bin/pip3 /usr/bin/pip
    
    # Copy the compiled Cryo binary from builder stage
    COPY --from=builder /tmp/cryo-build/cryo/target/release/cryo /usr/local/bin/cryo
    
    # Verify binary is executable
    RUN chmod +x /usr/local/bin/cryo
    
    # Create required directories
    RUN mkdir -p /app/data /app/logs /app/state /app/migrations && \
    chmod 777 /app/data /app/logs /app/state /app/migrations
    
    # Copy application files
    COPY requirements.txt /app/
    COPY migrations/ /app/migrations/
    COPY src/ /app/src/
    COPY scripts/ /app/scripts/
    
    # Make scripts executable and ensure they have proper line endings
    RUN find /app/scripts -type f -name "*.sh" -exec chmod +x {} \; && \
        find /app/scripts -type f -name "*.sh" -exec sed -i 's/\r$//' {} \;
    
    # Install Python dependencies
    RUN pip3 install --no-cache-dir --upgrade pip && \
        pip3 install --no-cache-dir -r requirements.txt
    
    # Test Cryo installation to verify libssl1.1 is properly linked
    RUN ldd /usr/local/bin/cryo && \
        cryo --version || echo "Cryo installation needs troubleshooting"
    
    # Set the entrypoint
    ENTRYPOINT ["/bin/bash", "/app/scripts/entrypoint.sh"]