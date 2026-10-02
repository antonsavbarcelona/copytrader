FROM rust:1-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/copytrader /usr/local/bin/copytrader
ENV RUST_BACKTRACE=1
# The state and every event go to Postgres (DATABASE_URL); /data is a local copy.
CMD ["copytrader", "run", "--data", "/data"]
