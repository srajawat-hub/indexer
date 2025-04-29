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
COPY target/release/indexer /usr/local/bin/indexer
COPY config.toml /usr/local/bin/config.toml

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