package options

import (
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Options struct {
	KafkaTopic     string
	KafkaAddress   string
	KafkaPartition int
	HttpPort       int
}

func ReadOptions() Options {
	err := godotenv.Load()
	if err != nil {
		log.Panicln("failed to load .env:", err)
	}

	partition, err := strconv.Atoi(os.Getenv("KAFKA_PARTITION"))
	port, err := strconv.Atoi(os.Getenv("HTTP_PORT"))

	return Options{
		KafkaTopic:     os.Getenv("KAFKA_TOPIC"),
		KafkaAddress:   os.Getenv("KAFKA_ADDRESS"),
		KafkaPartition: partition,
		HttpPort:       port,
	}
}
