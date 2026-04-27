package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	telemetry_aggregator "github.com/BenOnSocial/telemetry-aggregator/proto"
	"github.com/gorilla/websocket"
	"github.com/redis/go-redis/v9"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

var upgrader = websocket.Upgrader{}

const PRUNE_AND_ADD_SCRIPT = `
-- KEYS[1] = "telemetry:machine:[machine_id]"
-- ARGV[1] = "now" (current timestamp)
-- ARGV[2] = "window_seconds" (e.g., 1800 for 30 minutes)
-- ARGV[3] = "payload" (the serialized protobuf bytes)

local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local payload = ARGV[3]

-- 1. Remove points older than the window
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)

-- 2. Add the new point (using the timestamp as the score)
redis.call('ZADD', key, now, payload)

-- 3. Set a global TTL on the key so we don't leak memory for abandoned kiosks
redis.call('EXPIRE', key, window)

return 1
`

var telemetryScript = redis.NewScript(PRUNE_AND_ADD_SCRIPT)

func main() {
	log.SetFlags(0)

	// Root context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Listen for OS signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	// Cancel context on OS signal
	go func() {
		<-quit
		log.Println("Shutting down...")
		cancel()
	}()

	// Create shared cache connection
	cacheAddress, _ := os.LookupEnv("CACHE_ADDRESS")
	cache := redis.NewClient(&redis.Options{
		Addr:     cacheAddress,
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})

	// Create shared database connection
	databasePassword := getSecret("database_password")
	databaseHost, ok := os.LookupEnv("DATABASE_HOST")
	databasePort, ok := os.LookupEnv("DATABASE_PORT")
	databaseUser := "postgres"
	databaseName, ok := os.LookupEnv("POSTGRES_DB")
	connString := fmt.Sprintf("postgres://%s:%s@%s:%s/%s", databaseUser, databasePassword, databaseHost, databasePort, databaseName)
	dbPool, err := pgxpool.New(ctx, connString)
	if err != nil {
		log.Fatalln(err)
	}
	defer dbPool.Close()

	workerCount := 50
	val, ok := os.LookupEnv("WORKER_COUNT")
	if ok {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			workerCount = parsed
		}
	}

	// Data channel for passing raw payload to workers.
	dataChannel := make(chan []byte, 1000)
	var waitGroup sync.WaitGroup

	for i := 0; i < workerCount; i++ {
		waitGroup.Add(1)
		go worker(ctx, dataChannel, &waitGroup, cache, dbPool)
	}

	log.Printf("Started %d workers\n", workerCount)

	http.HandleFunc("/telemetry", func(w http.ResponseWriter, r *http.Request) {
		handleWebSocket(w, r, dataChannel)
	})

	server := &http.Server{Addr: ":8080"}

	go func() {
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	<-quit

	log.Println("Shutting down aggregator...")

	server.Shutdown(ctx)

	close(dataChannel)
	waitGroup.Wait()

	log.Println("All workers stopped. Aggregator exited cleanly.")
}

func worker(ctx context.Context, dataChan <-chan []byte, waitGroup *sync.WaitGroup, cache *redis.Client, dbPool *pgxpool.Pool) {
	defer waitGroup.Done()

	batchSize := 500
	val, ok := os.LookupEnv("DATABASE_COPY_BATCH_SIZE")
	if ok {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			batchSize = parsed
		}
	}
	timeoutSeconds := 30
	val, ok = os.LookupEnv("DATABASE_COPY_BATCH_TIMEOUT")
	if ok {
		parsed, err := strconv.Atoi(val)
		if err == nil {
			timeoutSeconds = parsed
		}
	}

	batch := make([]*telemetry_aggregator.DataPoint, 0, batchSize)

	// Flush the batch to the database, even if it's not full
	ticker := time.NewTicker(time.Duration(timeoutSeconds) * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			flushBatch(ctx, dbPool, batch)

			// Return out of the worker when main() calls cancel()
			return
		case <-ticker.C:
			if len(batch) > 0 {
				flushBatch(ctx, dbPool, batch)
				batch = batch[:0]
			}
		case payload, ok := <-dataChan:
			if !ok {
				// Channel was closed
				return
			}

			dataPoint := &telemetry_aggregator.DataPoint{}
			proto.Unmarshal(payload, dataPoint)
			cacheDataPoint(ctx, cache, dataPoint.MachineId, payload)
			batch = append(batch, dataPoint)

			if len(batch) >= batchSize {
				flushBatch(ctx, dbPool, batch)
				batch = batch[:0]
			}
		}
	}
}

func cacheDataPoint(ctx context.Context, cache *redis.Client, machineId string, payload []byte) {
	now := time.Now().Unix()
	window := 1800 // 30 minutes

	// Use hash tag around the machine ID to force related telemetry data to land on the same shard.
	key := fmt.Sprintf("telemetry:machine:{%s}", machineId)
	_, err := telemetryScript.Run(ctx, cache, []string{key}, now, window, payload).Result()
	if err != nil {
		log.Printf("Redis update failed: %v", err)
	}
}

func flushBatch(ctx context.Context, dbPool *pgxpool.Pool, batch []*telemetry_aggregator.DataPoint) {
	_, err := dbPool.CopyFrom(
		ctx,
		pgx.Identifier{"telemetry"},
		[]string{"machine_id", "measured_at", "cpu_usage", "memory_usage", "disk_usage"},
		pgx.CopyFromSlice(len(batch), func(i int) ([]any, error) {
			return []any{batch[i].MachineId, batch[i].MeasuredAt.AsTime(), batch[i].CpuUsage, batch[i].MemoryUsage, batch[i].DiskUsage}, nil
		}),
	)

	if err != nil {
		log.Println("Batch insert failed.", err)
		return
	}

	log.Printf("Batch insert complete: %d records.", len(batch))
}

func handleWebSocket(w http.ResponseWriter, r *http.Request, dataChan chan<- []byte) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Println("Upgrade error:", err)
		return
	}
	defer conn.Close()

	for {
		messageType, p, err := conn.ReadMessage()
		if err != nil {
			log.Println("Client disconnected:", err)
			break
		}

		if messageType == websocket.BinaryMessage {
			dataChan <- p
		}
	}
}

func getSecret(secretName string) string {
	data, err := os.ReadFile(fmt.Sprintf("/run/secrets/%s", secretName))
	if err != nil {
		log.Fatalf("Failed to read secret: %s", secretName)
	}

	return strings.TrimSpace(string(data))
}
