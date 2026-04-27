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

	telemetry_aggregator "github.com/BenOnSocial/telemetry-aggregator/proto"
	"github.com/gorilla/websocket"

	"github.com/jackc/pgx/v5/pgxpool"
	"google.golang.org/protobuf/proto"
)

var upgrader = websocket.Upgrader{}

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
		go worker(ctx, dataChannel, &waitGroup, dbPool)
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

func worker(ctx context.Context, dataChan <-chan []byte, waitGroup *sync.WaitGroup, dbPool *pgxpool.Pool) {
	defer waitGroup.Done()

	query := `INSERT INTO telemetry (machine_id, measured_at, cpu_usage, memory_usage, disk_usage) VALUES ($1, $2, $3, $4, $5)`

	for {
		select {
		case <-ctx.Done():
			// Return out of the worker when main() calls cancel()
			return
		case payload, ok := <-dataChan:
			if !ok {
				// Channel was closed
				return
			}

			dataPoint := &telemetry_aggregator.DataPoint{}
			err := proto.Unmarshal(payload, dataPoint)
			if err != nil {
				log.Println("Unmarshal error:", err)
				continue
			}

			_, err = dbPool.Exec(ctx, query, dataPoint.MachineId, dataPoint.MeasuredAt.AsTime(), dataPoint.CpuUsage, dataPoint.MemoryUsage, dataPoint.DiskUsage)
			if err != nil {
				log.Printf("Insert failed: %v", err)
			}

			// log.Printf("Data point inserted: %s", dataPoint.String())
		}
	}
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
