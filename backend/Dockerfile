FROM golang:1.26 AS builder
WORKDIR /src

COPY backend/go.mod backend/go.sum ./
RUN go mod download
COPY backend/ ./
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o /out/f1-go-backend .

FROM gcr.io/distroless/static-debian12
WORKDIR /

COPY --from=builder /out/f1-go-backend /f1-go-backend

ENV PORT=8000
ENV DATA_DIR=/data
ENV PROCESSOR_MODE=go

EXPOSE 8000

CMD ["/f1-go-backend"]
