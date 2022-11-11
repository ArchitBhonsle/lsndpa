package cache

import (
	"context"
	"strconv"

	"github.com/ArchitBhonsle/lsndpa/parser/message"
	"github.com/go-redis/redis/v8"
	"google.golang.org/protobuf/proto"
)

func Store(m *message.Message, r *redis.Client) error {
	data, err := proto.Marshal(m)
	if err != nil {
		return err
	}

	err = r.Set(context.TODO(), strconv.FormatUint(m.Id, 10), data, 0).Err()
	if err != nil {
		return err
	}

	return nil
}
