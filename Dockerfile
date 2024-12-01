FROM golang:1.23.3-alpine AS builder
WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN go build -o main .

FROM golang:1.23.3-alpine AS installer
WORKDIR /app
COPY --from=builder /build/main /app/
COPY --from=builder /build/config.yaml /app/config.yaml
ENTRYPOINT ["/app/main"]
