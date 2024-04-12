package binlogsdk

import (
	"github.com/redis/go-redis/v9"
	"os"
)

func InitSdk(client *redis.Client, group string) {
	setRedisClient(client)
	innerGroup = group
	innerConsumer = os.Getenv("HOSTNAME")
}
