package influx

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	prettyLog "github.com/my-dev-lib/pretty-log-go"
	"sync"
	"time"
	"venu-data/config"
	"venu-data/consumer/base"
)

const (
	topicWrite        = "influx_write_switch"
	topicWriteGroupId = "influx_write_switch_group_0"
	pollSize          = 80
)

var sharedDbPool = make(map[string]*Pool)
var poolLock = sync.Mutex{}

func obtainPool(dbName string) *Pool {
	poolLock.Lock()
	defer poolLock.Unlock()

	pool, ok := sharedDbPool[dbName]
	if !ok {
		cfg := config.Get().InfluxDb
		pool = NewPool(config.GetBaseConfig().InfluxPoolSize, dbName, cfg.Host, cfg.Port, false)
		sharedDbPool[dbName] = pool
	}

	return pool
}

type WriteMessage struct {
	DbName      string            `json:"db_name"`
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags"`
	Fields      map[string]any    `json:"fields"`
	Timestamp   string            `json:"timestamp"`
}

type WriteConsumer struct {
	log *prettyLog.Log
	id  string
}

func NewSwitchWriteConsumer() *WriteConsumer {
	return &WriteConsumer{
		log: prettyLog.NewLog("IIC"),
		//id:  topicWriteGroupId + "_" + fmt.Sprintf("%d", time.Now().Nanosecond()),
		id: topicWriteGroupId + "_" + uuid.New().String(),
	}
}

func (ic *WriteConsumer) GroupId() string {
	return topicWriteGroupId
}
func (ic *WriteConsumer) Id() string {
	return ic.id
}

func (ic *WriteConsumer) Topic() string {
	return topicWrite
}

func (ic *WriteConsumer) handle(dbName string, msg *WriteMessage) {
	pool := obtainPool(dbName)

	t, _ := time.Parse(time.RFC3339Nano, msg.Timestamp)
	err := pool.writeToInfluxDb(msg.Measurement, msg.Tags, msg.Fields, t)
	if err != nil {
		ic.log.W("写入 influxdb 失败：%v", err)
	}
}

func (ic *WriteConsumer) Consume(msg *base.DataMessage) error {
	// ic.log.D("influx.WriteConsumer 开始消费：%v", string(msg.Value))

	var iwMsg WriteMessage
	err := json.Unmarshal(msg.Value, &iwMsg)

	if err != nil {
		return fmt.Errorf("json 解析错误：%v", err)
	}

	ic.handle(iwMsg.DbName, &iwMsg)
	return nil
}
