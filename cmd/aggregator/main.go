package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"time"

	telemetry_aggregator "github.com/BenOnSocial/telemetry-aggregator/proto"
	"github.com/gorilla/websocket"

	"google.golang.org/protobuf/proto"
)

var upgrader = websocket.Upgrader{}

func main() {
	log.SetFlags(0)

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
		go worker(dataChannel, &waitGroup)
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

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)
	<-quit

	log.Println("Shutting down aggregator...")

	context, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	server.Shutdown(context)

	close(dataChannel)
	waitGroup.Wait()

	log.Println("All workers stopped. Aggregator exited cleanly.")
}

func worker(dataChan <-chan []byte, waitGroup *sync.WaitGroup) {
	defer waitGroup.Done()

	for payload := range dataChan {
		dataPoint := &telemetry_aggregator.DataPoint{}
		err := proto.Unmarshal(payload, dataPoint)
		if err != nil {
			log.Println("Unmarshal error:", err)
			continue
		}

		log.Printf("%s", dataPoint.String())
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
