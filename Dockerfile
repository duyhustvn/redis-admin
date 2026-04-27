FROM golang:1.25-alpine AS builder

ARG HTTP_PROXY
ARG HTTPS_PROXY

WORKDIR /

COPY go.mod go.sum ./
RUN go mod download

COPY internal ./internal
COPY cmd ./cmd
COPY docs/swagger ./docs/swagger

# Build the Go binary for Linux
# CGO_ENABLED=0 is important for static binaries that work with scratch/alpine images
RUN CGO_ENABLED=0 go build -o app cmd/server/main.go

FROM alpine

WORKDIR /

COPY --from=builder /app /app
COPY --from=builder /docs/swagger /docs/swagger

CMD ["/app"]