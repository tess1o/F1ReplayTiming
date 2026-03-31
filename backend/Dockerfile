FROM golang:1.26 AS builder
WORKDIR /src

COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/f1-go-backend .

FROM python:3.14-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc g++ libheif-dev && \
    rm -rf /var/lib/apt/lists/*

COPY data-fetcher/requirements-worker.txt /tmp/requirements-worker.txt
RUN pip install --no-cache-dir -r /tmp/requirements-worker.txt

COPY data-fetcher /app/data-fetcher
COPY --from=builder /out/f1-go-backend /app/f1-go-backend

ENV PORT=8000
ENV DATA_DIR=/data
ENV PY_WORKER_PATH=/app/data-fetcher/worker_bridge.py

EXPOSE 8000

CMD sh -c "cp -n /app/data-fetcher/data/pit_loss.json /data/pit_loss.json 2>/dev/null; /app/f1-go-backend"
