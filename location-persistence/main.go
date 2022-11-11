package main

import (
	"context"
	"log"

	"github.com/ArchitBhonsle/lsndpa/location-persistence/message"
	"github.com/segmentio/kafka-go"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"google.golang.org/protobuf/proto"
)

const uri = "mongodb://root:root@localhost:27017/"
const nWorkers = 5

func unmarshalAndStore(msg kafka.Message, coll *mongo.Collection) {
	val := msg.Value
	protoMessage := &message.Message{}

	err := proto.Unmarshal(val, protoMessage)
	if err != nil {
		log.Fatal("failed to unmarshal message:", err)
	}

	doc := bson.M{
		"id":        protoMessage.Id,
		"timestamp": protoMessage.Timestamp,
		"latitude":  protoMessage.Latitude,
		"logitude":  protoMessage.Longitude,
	}

	_, err = coll.InsertOne(context.TODO(), doc)

	log.Println(protoMessage.Id, protoMessage.Latitude, protoMessage.Longitude, protoMessage.Speed)

	if err != nil {
		log.Println("failed to write document:", err)
	}
}

func main() {
	client, err := mongo.Connect(context.TODO(), options.Client().ApplyURI(uri))

	read := kafka.NewReader(kafka.ReaderConfig{
		Brokers:   []string{"localhost:9092"},
		Topic:     "parsed",
		Partition: 0,
		MinBytes:  10e3,
		MaxBytes:  10e6,
	})

	coll := client.Database("speed").Collection("location")

	// settings up the queue and workers
	queue := make(chan kafka.Message)
	worker := func() {
		for {
			msg := <-queue
			unmarshalAndStore(msg, coll)
		}
	}
	for i := 0; i < nWorkers; i++ {
		go worker()
	}

	for {
		msg, err := read.ReadMessage(context.Background())
		if err != nil {
			log.Println("failed to read message:", msg)
			break
		}

		go func() { queue <- msg }()
	}

	if err := read.Close(); err != nil {
		log.Println("failed to close reader:", err)
	}

	if err = client.Disconnect(context.TODO()); err != nil {
		log.Fatal("failed to disconnect database:", err)
	}
}
