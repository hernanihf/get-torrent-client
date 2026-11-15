# ============================================================
# get-torrent  —  Multi-stage Docker build
# ============================================================
# Build:   docker build -t get-torrent .
# Run:     docker compose up -d
# ============================================================

# --- Stage 1: Build -----------------------------------------
FROM golang:1.23-alpine AS builder

RUN apk add --no-cache git ca-certificates

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o /get-torrent-daemon ./cmd/daemon

# --- Stage 2: Runtime ----------------------------------------
FROM alpine:latest

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S gettorrent && \
    adduser -S -G gettorrent gettorrent

COPY --from=builder /get-torrent-daemon /usr/local/bin/get-torrent-daemon

# Volumes
RUN mkdir -p /downloads /config && \
    chown -R gettorrent:gettorrent /downloads /config

USER gettorrent

# RPC (WebUI), BitTorrent TCP, BitTorrent UDP (DHT)
EXPOSE 9091 6881 6881/udp

ENV RPC_ADDR=0.0.0.0:9091 \
    DOWNLOAD_DIR=/downloads \
    CONFIG_DIR=/config \
    ANTIVIRUS_ADDR=clamav:3310

ENTRYPOINT ["get-torrent-daemon"]
CMD ["--rpc-addr", "0.0.0.0:9091", "--download-dir", "/downloads", "--config-dir", "/config"]
