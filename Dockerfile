FROM rust:1-slim-bookworm AS build
WORKDIR /app
COPY Cargo.toml Cargo.lock ./
COPY src ./src
RUN cargo build --release --locked

FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=build /app/target/release/copytrader /usr/local/bin/copytrader
# Every wallet active since 2026-09-10 with enough history for the backtest (14,254), copied
# on paper next to the backtest's 172 picks (read back over REST too) as their control.
# The allocator keeps its books: wallets ranked by several scores on the backtest (backtest_stats.csv) and the paper.
COPY wallets /app/wallets
ENV RUST_BACKTRACE=1
# The state and every event go to Postgres (DATABASE_URL); /data is a local copy.
CMD ["copytrader", "run", "--data", "/data", "--no-scout", "--wallets-file", "/app/wallets/all.txt", "--poll-file", "/app/wallets/picks.txt", "--alloc-prior", "/app/wallets/backtest_stats.csv"]
