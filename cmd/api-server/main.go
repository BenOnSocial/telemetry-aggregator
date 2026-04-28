package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	telemetry_aggregator "github.com/BenOnSocial/telemetry-aggregator/proto"
	"github.com/redis/go-redis/v9"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type APIServer struct {
	cache *redis.Client
}

func main() {
	log.SetFlags(0)

	var cacheAddress, _ = os.LookupEnv("CACHE_ADDRESS")
	var cacheClient = redis.NewClient(&redis.Options{
		Addr:     cacheAddress,
		Password: "", // no password
		DB:       0,  // use default DB
		Protocol: 2,
	})
	defer cacheClient.Close()

	api := &APIServer{
		cache: cacheClient,
	}

	// Endpoints
	http.HandleFunc("/machines", api.handleListMachines)
	http.HandleFunc("/status/", api.handleStatus)

	// Listen for OS signals
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, os.Interrupt, syscall.SIGTERM)

	server := &http.Server{Addr: ":8081"}

	go func() {
		log.Println("API server started on port 8081.")
		err := server.ListenAndServe()
		if err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %s\n", err)
		}
	}()

	<-quit
	log.Println("Shutting down API server...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := server.Shutdown(shutdownCtx)
	if err != nil {
		log.Printf("Server forced to shutdown: %v", err)
	}

	log.Println("API server exited cleanly.")

}

func (s *APIServer) handleListMachines(w http.ResponseWriter, r *http.Request) {
	// ZREVRANGE fetches highest score (newest) to lowest score (oldest)
	// In go-redis v9, ZRange with ZRangeBy{Rev: true} is used, or just ZRangeByScore
	machines, err := s.cache.ZRangeArgs(r.Context(), redis.ZRangeArgs{
		Key:   "telemetry:active_machines",
		Start: 0,
		Stop:  -1,
		Rev:   true,
	}).Result()

	if err != nil {
		log.Printf("Failed to fetch active machines: %v", err)
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	// Serve as JSON array of strings
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"active_machines": machines,
		"count":           len(machines),
	})
}

func (s *APIServer) handleStatus(w http.ResponseWriter, r *http.Request) {
	machineId := r.URL.Path[len("/status/"):]
	key := fmt.Sprintf("telemetry:machine:{%s}", machineId)

	log.Printf("Searching for machine ID: %s", key)

	val, err := s.cache.ZRange(r.Context(), key, -1, -1).Result()
	if err != nil || len(val) == 0 {
		http.Error(w, "Not Found", http.StatusNotFound)
		return
	}

	dataPoint := &telemetry_aggregator.DataPoint{}
	err = proto.Unmarshal([]byte(val[0]), dataPoint)
	if err != nil {
		http.Error(w, "Internal Error", http.StatusInternalServerError)
		return
	}

	marshaller := protojson.MarshalOptions{
		EmitUnpopulated: true,
		UseProtoNames:   true,
	}

	bytes, err := marshaller.Marshal(dataPoint)
	if err != nil {
		http.Error(w, "JSON Encoding Error", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.Write(bytes)
}
