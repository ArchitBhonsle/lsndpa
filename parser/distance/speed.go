package distance

import (
	"context"
	"log"
	"strconv"
	"time"

	"github.com/ArchitBhonsle/lsndpa/parser/cache"
	"github.com/ArchitBhonsle/lsndpa/parser/message"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

func CalculateSpeed(curr *message.Message, r *redis.Client) float64 {
	prevBytes, err := r.Get(context.TODO(), strconv.FormatUint(curr.Id, 10)).Bytes()

	if err == redis.Nil {
		storeErr := cache.Store(curr, r)
		if storeErr != nil {
			log.Panic(storeErr)
		}
		return 0.0
	}

	if err != nil {
		log.Panic(err)
	}

	prev := &message.Message{}
	err = proto.Unmarshal(prevBytes, prev)
	if err != nil {
		log.Panic(err)
	}

	currTime := curr.Timestamp.AsTime()
	prevTime := prev.Timestamp.AsTime()

	if currTime.Before(prevTime) {
		return prev.Speed
	}

	d := Distance(
		curr.Latitude,
		curr.Longitude,
		prev.Latitude,
		prev.Longitude,
	)
	t := float64(currTime.Sub(prevTime) / time.Second)
	speed := d / t

	cache.Store(curr, r)

	return speed
}
