# Use the official Rust image as a builder
FROM rust:slim AS builder

# Create a new empty shell project
WORKDIR /usr/src/lumedb
COPY Cargo.toml Cargo.lock ./
COPY src ./src

# Build for release
RUN cargo build --release --bin lumedb-server

# Use a minimal runtime image
FROM debian:bookworm-slim

# Install necessary runtime dependencies if any (usually libc)
RUN apt-get update && apt-get install -y ca-certificates && rm -rf /var/lib/apt/lists/*

# Copy the build artifact from the builder stage
COPY --from=builder /usr/src/lumedb/target/release/lumedb-server /usr/local/bin/lumedb-server

# Expose the default LumeDB port
EXPOSE 7070

# Set the data directory as a volume
VOLUME ["/var/lib/lumedb"]

# Run the binary
CMD ["lumedb-server", "--data-dir", "/var/lib/lumedb", "--host", "0.0.0.0", "--port", "7070"]
