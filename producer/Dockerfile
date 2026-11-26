FROM golang:1.23-alpine AS builder

WORKDIR /app

COPY go.mod go.sum ./
RUN go mod download

COPY *.go ./
RUN go build -o producer .



FROM alpine:3.21

WORKDIR /app

COPY --from=builder /app/producer ./producer

ENTRYPOINT ["./producer"]

