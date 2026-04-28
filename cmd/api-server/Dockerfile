# Build stage
FROM dhi.io/golang:1-alpine3.22-dev AS builder
WORKDIR /app

# Copy dependency files to leverage Docker cache
COPY go.mod go.sum ./
RUN go mod download

# Copy the source
COPY . .

# Build the application. Ensure statically linked binary for compatibility.
RUN CGO_ENABLED=0 go build -o /app/main ./cmd/api-server


# Create the runtime
FROM dhi.io/alpine-base:3.23 AS runner
WORKDIR /

# Copy on the compiled binary.
COPY --from=builder /app/main /main

USER nonroot

EXPOSE 8081

ENTRYPOINT ["/main"]
