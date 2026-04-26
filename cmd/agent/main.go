package main

import (
	"log"
	"net/url"
	"os"
	"os/signal"
	"syscall"
	"time"

	telemetry_aggregator "github.com/BenOnSocial/telemetry-aggregator/proto"
	"github.com/gorilla/websocket"
	"github.com/shirou/gopsutil/v3/cpu"
	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)

	host, ok := os.LookupEnv("AGGREGATOR_ADDR")
	if !ok {
		host = "aggregator:8080"
	}
	url := url.URL{Scheme: "ws", Host: host, Path: "/telemetry"}

	conn, _, err := websocket.DefaultDialer.Dial(url.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer conn.Close()

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	log.Println("Agent started, streaming telemetry...")

	for {
		select {
		case <-interrupt:
			log.Println("Interrupt received, closing connection...")
			conn.WriteMessage(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
		case <-ticker.C:
			dataPoint := measureHealth()
			out, err := proto.Marshal(dataPoint)
			if err != nil {
				log.Fatalln("Failed to marshal data point:", err)
			}

			err = conn.WriteMessage(websocket.BinaryMessage, out)
			if err != nil {
				log.Fatal("Failed to send data point:", err)
			}
		}

	}
}

func measureHealth() (dataPoint *telemetry_aggregator.DataPoint) {
	machineId, _ := os.Hostname()
	cpu, _ := cpu.Percent(time.Second, false)
	v, _ := mem.VirtualMemory()
	disk, _ := disk.Usage("/")

	dataPoint = &telemetry_aggregator.DataPoint{
		MachineId:   machineId,
		MeasuredAt:  timestamppb.Now(),
		CpuUsage:    cpu[0],
		MemoryUsage: v.UsedPercent,
		DiskUsage:   disk.UsedPercent,
	}

	return
}
