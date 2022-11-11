package options

import (
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/joho/godotenv"
)

type Options struct {
	KafkaFromTopic     string
	KafkaFromAddress   string
	KafkaFromPartition int
	KafkaToTopic       string
	KafkaToAddress     string
	KafkaToPartition   int
	RedisAddress       string
	RedisDB            int
	NWorkers           int
}

func readIntEnv(name string) int {
	val, err := strconv.Atoi(os.Getenv(name))

	if err != nil {
		log.Fatalln(fmt.Sprintf("%v not found: ", name), err)
	}

	return val
}

func ReadOptions() Options {
	err := godotenv.Load()
	if err != nil {
		log.Panicln("failed to load .env:", err)
	}

	return Options{
		KafkaFromTopic:     os.Getenv("KAFKA_FROM_TOPIC"),
		KafkaFromAddress:   os.Getenv("KAFKA_FROM_ADDRESS"),
		KafkaFromPartition: readIntEnv("KAFKA_FROM_PARTITION"),
		KafkaToTopic:       os.Getenv("KAFKA_TO_TOPIC"),
		KafkaToAddress:     os.Getenv("KAFKA_TO_ADDRESS"),
		KafkaToPartition:   readIntEnv("KAFKA_TO_PARTITION"),
		RedisAddress:       os.Getenv("REDIS_ADDRESS"),
		RedisDB:            readIntEnv("REDIS_DB"),
		NWorkers:           readIntEnv("N_WORKERS"),
	}
}
