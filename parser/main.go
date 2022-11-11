package main

import (
	"context"
	"encoding/binary"
	"log"
	"math"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/segmentio/kafka-go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/ArchitBhonsle/lsndpa/parser/distance"
	"github.com/ArchitBhonsle/lsndpa/parser/message"
	"github.com/ArchitBhonsle/lsndpa/parser/options"
)

func setupConnections(opts options.Options) (*kafka.Reader, *kafka.Conn, *redis.Client) {
	read := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{opts.KafkaFromAddress},
		Topic:     opts.KafkaFromTopic,
		Partition: opts.KafkaFromPartition,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})

	write, err := kafka.DialLeader(
		context.Background(),
		"tcp", opts.KafkaToAddress,
		opts.KafkaFromTopic,
		opts.KafkaToPartition,
	)
	if err != nil {
		log.Fatal("failed to dial leader:", err)
	}

	cache := redis.NewClient(&redis.Options{
		Addr:     opts.RedisAddress,
		Password: "",
		DB:       opts.RedisDB,
	})

	return read, write, cache
}

func closeConnections(read *kafka.Reader, write *kafka.Conn, cache *redis.Client) {
	if err := read.Close(); err != nil {
		log.Println("failed to close reader:", err)
	}
	if err := write.Close(); err != nil {
		log.Println("failed to close reader:", err)
	}
	if err := cache.Close(); err != nil {
		log.Println("failed to close cache:", err)
	}
}

func parseAndWrite(msg kafka.Message, w *kafka.Conn, r *redis.Client) {
	// parse the binary encoded raw message

	value := msg.Value

	id := binary.BigEndian.Uint64(value[0:])
	unixNanos := int64(binary.BigEndian.Uint64(value[8:]))
	timestamp := time.Unix(0, unixNanos)
	latitude := math.Float64frombits(binary.BigEndian.Uint64(value[16:]))
	longitude := math.Float64frombits(binary.BigEndian.Uint64(value[24:]))

	protoMessage := &message.Message{
		Timestamp: timestamppb.New(timestamp),
		Id:        id,
		Latitude:  latitude,
		Longitude: longitude,
	}

	speed := distance.CalculateSpeed(protoMessage, r)
	protoMessage.Speed = speed

	out, err := proto.Marshal(protoMessage)
	if err != nil {
		log.Fatalln("protobuf marhsalling failed:", err)
	}

	keyBuf := make([]byte, 8)
	binary.BigEndian.PutUint64(keyBuf[0:], id)

	w.WriteMessages(kafka.Message{Key: keyBuf, Value: out})

	log.Println(protoMessage.Id, protoMessage.Latitude, protoMessage.Longitude, protoMessage.Speed)
}

func spawnWorkers(read *kafka.Reader, write *kafka.Conn, cache *redis.Client, nWorkers int) chan<- kafka.Message {
	queue := make(chan kafka.Message)

	worker := func() {
		for {
			msg := <-queue
			parseAndWrite(msg, write, cache)
		}
	}
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	return queue
}

func main() {
	opts := options.ReadOptions()

	read, write, cache := setupConnections(opts)
	queue := spawnWorkers(read, write, cache, opts.NWorkers)

	for {
		msg, err := read.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", msg)
			break
		}

		go func() { queue <- msg }()
	}

	closeConnections(read, write, cache)
}
