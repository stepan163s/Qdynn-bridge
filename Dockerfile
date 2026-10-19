# Stage 1: Build binary
FROM rust:1.94-alpine AS builder

RUN apk add --no-cache musl-dev

WORKDIR /app
COPY . .

RUN cargo build --release --locked -p qdynn-server

# Stage 2: Minimal runtime image
FROM alpine:3.20

RUN apk add --no-cache ca-certificates tzdata

WORKDIR /app
COPY --from=builder /app/target/release/qdynn-bridge /usr/local/bin/qdynn-bridge
COPY --from=builder /app/LICENSE /app/THIRD_PARTY_NOTICES.md /usr/share/doc/qdynn-bridge/

EXPOSE 8080

ENTRYPOINT ["/usr/local/bin/qdynn-bridge"]
CMD ["--listen", "0.0.0.0:8080"]
