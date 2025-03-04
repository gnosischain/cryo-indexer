#!/bin/bash
set -e

# This script builds Cryo with extremely minimal memory usage
# Save this as build_cryo.sh

# Install dependencies
apt-get update
apt-get install -y --no-install-recommends \
    curl \
    ca-certificates \
    build-essential \
    git \
    pkg-config \
    libssl-dev

# Install Rust
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
source "$HOME/.cargo/env"

# Clone only the necessary parts of the repo
mkdir -p /tmp/cryo-build
cd /tmp/cryo-build
git clone --depth 1 --filter=blob:none --sparse https://github.com/paradigmxyz/cryo
cd cryo
git sparse-checkout set crates/cli

# Configure for minimal memory usage
export RUSTFLAGS="-C codegen-units=1 -C opt-level=s -C debuginfo=0"
export CARGO_BUILD_JOBS=1
export CARGO_NET_RETRY=5

# Build with minimal memory
cd crates/cli
cargo build --release --no-default-features --jobs 1

# Install the binary
cp ../../target/release/cryo /usr/local/bin/
chmod +x /usr/local/bin/cryo

# Verify installation
cryo --version

# Clean up
cd /
rm -rf /tmp/cryo-build
apt-get clean
rm -rf /var/lib/apt/lists/*