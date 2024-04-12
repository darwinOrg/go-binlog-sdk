package binlogsdk

import (
	"context"
	"github.com/redis/go-redis/v9"
	"time"
)

var redisClient *redis.Client

func setRedisClient(client *redis.Client) {
	redisClient = client
}

func xGroupCreateMkStream(stream string, group string) (string, error) {
	return redisClient.XGroupCreateMkStream(context.TODO(), stream, group, "$").Result()
}

func xReadGroup(stream string, group string, consumer string, block time.Duration, count int64) ([]redis.XStream, error) {
	return redisClient.XReadGroup(context.TODO(), &redis.XReadGroupArgs{
		Group:    group,
		Consumer: consumer,
		Streams:  []string{stream, ">"},
		Block:    block,
		Count:    count,
	}).Result()
}

func xAck(stream string, group string, messageId string) (int64, error) {
	return redisClient.XAck(context.TODO(), stream, group, messageId).Result()
}
