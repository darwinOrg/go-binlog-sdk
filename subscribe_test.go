package binlogsdk_test

import (
	binlogsdk "github.com/darwinOrg/binlog-sdk"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/model"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/redis/go-redis/v9"
	"log"
	"os"
	"os/signal"
	"syscall"
	"testing"
)

type userBody struct {
	Id     int64  `json:"id"`
	Name   string `json:"name"`
	Mobile string `json:"mobile"`
}

var streamUser = &model.StringCodeNamePair{
	Code: "startrek.user",
	Name: "用户",
}

func TestSubscribe(t *testing.T) {
	binlogsdk.InitSdk(redis.NewClient(&redis.Options{
		Addr:     "127.0.0.1:6379",
		Password: "",
		DB:       0,
	}), "test")

	binlogsdk.Subscribe[userBody](streamUser,
		func(ctx *dgctx.DgContext, message *binlogsdk.Message[userBody]) error {
			dglogger.Infof(ctx, "message: %s", utils.MustConvertBeanToJsonString(message))

			return nil
		})
	s := make(chan os.Signal, 1)
	signal.Notify(s, os.Kill, os.Interrupt, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	sin := <-s
	log.Printf("application stoped，signal: %s \n", sin.String())
}
