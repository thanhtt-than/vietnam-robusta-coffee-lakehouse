# ==============================
# Stage 1: Build MinIO
# ==============================
FROM golang:1.25-bookworm AS builder

WORKDIR /src

COPY source/go.mod source/go.sum ./

RUN go mod download

COPY source/ ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -o /minio


# ==============================
# Stage 2: Runtime
# ==============================
FROM debian:bookworm-slim

COPY --from=builder /minio /usr/local/bin/minio

RUN chmod +x /usr/local/bin/minio

EXPOSE 9000 9001

VOLUME ["/data"]

ENTRYPOINT ["/usr/local/bin/minio"]