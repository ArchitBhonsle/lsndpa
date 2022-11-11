package main

import (
	"context"
	"io"
	"log"
	"net/http"

	"github.com/ArchitBhonsle/lsndpa/gateway/options"
	"github.com/segmentio/kafka-go"
)

func main() {
	opts := options.ReadOptions()

	write, err := kafka.DialLeader(
		context.Background(),
		"tcp",
		opts.KafkaAddress,
		opts.KafkaTopic,
		opts.KafkaPartition,
	)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	http.HandleFunc("/", createHandler(write))

	log.Println("server starting")
	if err := http.ListenAndServe(":8000", nil); err != http.ErrServerClosed {
		log.Panicln(err)
	}

	if err := write.Close(); err != nil {
		log.Fatal("failed to close writer:", err)
	}
}

func createHandler(write *kafka.Conn) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		log.Println("request recieved")

		if r.Method != http.MethodPost {
			http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
			return
		}

		defer r.Body.Close()
		buf := make([]byte, 32)
		bytesRead, err := r.Body.Read(buf)
		if (err != nil && err != io.EOF) || bytesRead != 32 {
			http.Error(w, err.Error(), http.StatusBadRequest)
			log.Panicln(err, bytesRead)
		}

		_, err = write.WriteMessages(kafka.Message{Key: buf[:8], Value: buf})
		if err != nil {
			log.Println("failed to write messages:", err)
		}

		w.WriteHeader(http.StatusCreated)
	}
}
