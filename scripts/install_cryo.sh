#!/bin/bash
set -e

# This script clones and builds Cryo with specific dependency versions
# to work around rust version compatibility issues

cd /tmp

# Clone the repository
git clone https://github.com/paradigmxyz/cryo.git
cd cryo

# Temporarily update the alloy dependency to a version compatible with rustc 1.73
# Find the main Cargo.toml file
if grep -q 'alloy-transport-http' Cargo.toml; then
    # Update the dependency version
    sed -i 's/alloy-transport-http = "0.6.4"/alloy-transport-http = "0.6.0"/' Cargo.toml
    
    # Also check in the workspace dependencies section
    if grep -q '\[workspace.dependencies\]' Cargo.toml; then
        sed -i 's/alloy-transport-http = "0.6.4"/alloy-transport-http = "0.6.0"/' Cargo.toml
    fi
fi

# Also check the cli crate's Cargo.toml
if [ -f "crates/cli/Cargo.toml" ] && grep -q 'alloy-transport-http' crates/cli/Cargo.toml; then
    sed -i 's/alloy-transport-http = "0.6.4"/alloy-transport-http = "0.6.0"/' crates/cli/Cargo.toml
fi

# Check the freeze crate's Cargo.toml
if [ -f "crates/freeze/Cargo.toml" ] && grep -q 'alloy-transport-http' crates/freeze/Cargo.toml; then
    sed -i 's/alloy-transport-http = "0.6.4"/alloy-transport-http = "0.6.0"/' crates/freeze/Cargo.toml
fi

# Downgrade all alloy dependencies
for file in $(find . -name "Cargo.toml"); do
    # Downgrade alloy version if it's 0.6.4
    sed -i 's/alloy = { version = "0.6.4"/alloy = { version = "0.6.0"/' $file
    sed -i 's/alloy = "0.6.4"/alloy = "0.6.0"/' $file
    
    # Downgrade all alloy-* crates 
    sed -i 's/alloy-[a-z-]* = "0.6.4"/alloy-\1 = "0.6.0"/' $file
done

# Update Cargo.lock
cargo update -p alloy-transport-http --precise 0.6.0
cargo update -p alloy --precise 0.6.0

# Build and install
cargo install --path ./crates/cli