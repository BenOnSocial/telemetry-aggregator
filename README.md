# High-Performance Telemetry Aggregator

A real-time distributed system designed to stream, aggregate, and process hardware health metrics (CPU, Memory, Disk) across thousands of remote agents. Built entirely in **Go**, this project demonstrates high-throughput ingestion, bounded concurrency, and efficient binary protocols.

## 🚀 Architectural Highlights

- **Binary Streaming Protocol:** Replaced bloated JSON/REST polling with **Protobuf over WebSockets**. This provides a full-duplex, persistent TCP connection with a minimal byte-footprint, drastically reducing network overhead for high-frequency telemetry.
- **Bounded Concurrency (Backpressure):** The Aggregator server utilizes a **Worker Pool** pattern reading from a buffered channel. This decouples ingestion from processing, ensuring that sudden spikes in telemetry traffic do not exhaust backend database connection pools or CPU resources.
- **Graceful Lifecycle Management:** Both the Agent and Aggregator strictly monitor OS interrupt signals (`SIGTERM`, `SIGINT`).
  - The **Agent** utilizes `select` blocks with `time.Ticker` to ensure non-blocking execution and clean socket closure.
  - The **Aggregator** utilizes `sync.WaitGroup` and Context timeouts to drain the in-memory buffered channels before terminating, guaranteeing zero data loss during deployments.
- **Production-Ready Containers:** Packaged using multi-stage Docker builds targeting lightweight Alpine images. Binaries are statically compiled (`CGO_ENABLED=0`) and execute under strict non-root user permissions for hardened security.

## 🛠 Tech Stack

- **Language:** Go 1.26
- **Transport:** WebSockets (`gorilla/websocket`)
- **Serialization:** Protocol Buffers (Protobuf)
- **Metrics:** `shirou/gopsutil`
- **Orchestration:** Docker & Docker Compose

## 📁 Project Structure

```text
├── cmd/
│   ├── agent/                 # Telemetry producer (Runs on edge nodes)
│   │   ├── main.go            # Ticker loop and WebSocket dialer
│   │   └── Dockerfile
│   └── aggregator/            # Telemetry consumer (Central server)
│       ├── main.go            # WebSocket upgrader and Worker Pool
│       └── Dockerfile
├── proto/
│   ├── telemetry.proto        # Schema definition
│   └── telemetry.pb.go        # Generated Go structs
└── docker-compose.yml         # Local cluster orchestration
```

## 🚀 Getting Started

### Prerequisites

- Docker
- Docker Compose

### Running the Cluster

Spin up the Aggregator server and a sample Agent node using Docker Compose:

```bash
docker-compose up --build
```

**What happens:**

1. The **Aggregator** boots on port `8080` and spins up 50 idle background workers.
2. The **Agent** boots, resolves the aggregator's internal DNS, and establishes a persistent WebSocket connection.
3. Every 5 seconds, the Agent scrapes the host container's CPU, Memory, and Disk usage, serializes it to Protobuf, and pushes it through the socket.
4. The Aggregator parses the binary payload and hands it off to the worker pool for processing.

### Simulating Graceful Shutdown

To observe the graceful shutdown sequence, press the stop button for the compose stack in Docker Desktop. You will see the Aggregator stop accepting new connections, wait for the workers to drain the data channel, and exit cleanly with code `0`.

---

_Developed as a demonstration of high-throughput Go architecture and distributed system design._
