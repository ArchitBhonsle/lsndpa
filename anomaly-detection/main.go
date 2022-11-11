package main

import (
	"context"
	"log"
	"math"
	"strconv"
	"time"

	"github.com/go-redis/redis/v8"

	"github.com/ArchitBhonsle/lsndpa/anomaly-detection/message"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
)

const nWorkers = 5
const anomalyDetectionRedisDB = 1
const thresholdAcceleration = 5

func store(kMessage *message.Message, cache *redis.Client) {
	out, err := proto.Marshal(kMessage)
	if err != nil {
		log.Panicln(err)
	}

	err = cache.Set(context.TODO(), strconv.FormatUint(kMessage.Id, 10), out, 0).Err()
	if err != nil {
		log.Panicln(err)
	}
}

func detect(kMessage kafka.Message, cache *redis.Client) bool {
	kValue := kMessage.Value
	currMessage := &message.Message{}
	unmashallingErr := proto.Unmarshal(kValue, currMessage)
	if unmashallingErr != nil {
		log.Fatal("failed to unmarshal message:", unmashallingErr)
	}

	prevBuffer, cacheGetErr := cache.Get(context.TODO(), strconv.FormatUint(currMessage.Id, 10)).Bytes()
	if cacheGetErr == redis.Nil {
		store(currMessage, cache)
		return false
	}
	if cacheGetErr != nil {
		log.Panicln(cacheGetErr)
	}

	prevMessage := &message.Message{}
	unmashallingErr = proto.Unmarshal(prevBuffer, prevMessage)
	if unmashallingErr != nil {
		log.Panicln(unmashallingErr)
	}

	currTime, prevTime := currMessage.Timestamp.AsTime(), prevMessage.Timestamp.AsTime()
	if currTime.Before(prevTime) {
		return false
	}

	acceleration := math.Abs(currMessage.Speed-prevMessage.Speed) / float64(currTime.Sub(prevTime)/time.Second)
	log.Println(currMessage.Id, currMessage.Speed, prevMessage.Speed, acceleration)
	if acceleration >= thresholdAcceleration {
		return true
	}

	store(currMessage, cache)

	return false
}

func main() {
	read := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "parsed",
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})

	cache := redis.NewClient(&redis.Options{
		Addr:     "localhost:6379",
		Password: "",
		DB:       anomalyDetectionRedisDB,
	})

	queue := make(chan kafka.Message)
	worker := func() {
		for {
			kMessage := <-queue
			if detect(kMessage, cache) {
				log.Println("anomaly")
			}
		}
	}
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	for {
		kMessage, err := read.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", kMessage)
			break
		}

		go func() { queue <- kMessage }()
	}

	if err := read.Close(); err != nil {
		log.Println("failed to close reader:", err)
	}
}
