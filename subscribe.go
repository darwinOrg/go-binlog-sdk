package binlogsdk

import (
	"github.com/darwinOrg/go-common/constants"
	dgctx "github.com/darwinOrg/go-common/context"
	"github.com/darwinOrg/go-common/model"
	"github.com/darwinOrg/go-common/utils"
	dglogger "github.com/darwinOrg/go-logger"
	"github.com/google/uuid"
	"strconv"
)

type MessageAction string

const (
	MessageActionInsert MessageAction = "insert"
	MessageActionUpdate MessageAction = "update"
	MessageActionDelete MessageAction = "delete"
)

var (
	innerGroup    string
	innerConsumer string
)

type Message[T any] struct {
	Id        string        `json:"id"`
	Action    MessageAction `json:"action"`
	Timestamp int64         `json:"timestamp"`
	Data      *T            `json:"data"`
}

type Acceptor[T any] func(ctx *dgctx.DgContext, message *Message[T]) error

func Subscribe[T any](stream *model.StringCodeNamePair, acceptor Acceptor[T]) {
	ctx := &dgctx.DgContext{TraceId: uuid.NewString()}
	dglogger.Infof(ctx, "Subscribe Stream | stream:%s | group:%s | consumer:%s ", stream.Code, innerGroup, innerConsumer)

	_, groupErr := xGroupCreateMkStream(stream.Code, innerGroup)
	if groupErr != nil {
		dglogger.Debugf(ctx, "XGROUPCREATEMKSTREAM 错误 | err:%v", groupErr)
	}

	go func() {
		for {
			xstreams, readErr := xReadGroup(stream.Code, innerGroup, innerConsumer, 0, 10)
			if readErr != nil {
				dglogger.Errorf(ctx, "XREADGROUP 错误 | err:%v", readErr)
				continue
			}

			for _, xstream := range xstreams {
				for _, xmessage := range xstream.Messages {
					values := xmessage.Values
					data := values["data"].(string)
					timestamp, _ := strconv.ParseInt(values["timestamp"].(string), 10, 64)

					message := &Message[T]{
						Id:        xmessage.ID,
						Action:    MessageAction(values["action"].(string)),
						Timestamp: timestamp,
						Data:      utils.MustConvertJsonStringToBean[T](data),
					}

					dc := &dgctx.DgContext{TraceId: values[constants.TraceId].(string)}
					dglogger.Infof(dc, "Accept Message | stream:%s | group:%s | consumer:%s | message:%s", stream.Code, innerGroup, innerConsumer, data)
					acceptorErr := acceptor(dc, message)

					if acceptorErr != nil {
						dglogger.Errorf(dc, "Accept Message 错误 | err:%v", acceptorErr)
						continue
					}

					_, ackErr := xAck(stream.Code, innerGroup, xmessage.ID)
					if ackErr != nil {
						dglogger.Errorf(dc, "Accept Message Ack 错误 | err:%v", ackErr)
					}
				}
			}
		}
	}()
}
