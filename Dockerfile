# Build stage
FROM rust:1.82-slim-bullseye as builder

# Create a new empty shell project
WORKDIR /usr/src/indexer
COPY . .

# Install OpenSSL - required for HTTPS requests
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Build with release profile
RUN cargo build --release

# Runtime stage
FROM debian:bullseye-slim

# Install OpenSSL - required for HTTPS requests
RUN apt-get update && apt-get install -y \
    ca-certificates \
    openssl \
    pkg-config \
    libssl-dev \
    build-essential \
    && rm -rf /var/lib/apt/lists/*


# Copy the binary from builder
COPY --from=builder /usr/src/indexer/target/release/indexer /usr/local/bin/indexer
COPY --from=builder /usr/src/indexer/config.toml /usr/local/bin/config.toml


ENV RUST_LOG=info

# Create a non-root user
RUN useradd -ms /bin/bash indexer

# Switch to non-root user
USER indexer

# Copy .env file if needed
# COPY .env /usr/local/bin/.env

# Set the binary as the entrypoint
ENTRYPOINT ["/usr/local/bin/indexer"]

# Expose port 8085
EXPOSE 8085