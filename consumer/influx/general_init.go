package influx

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	pretty_log "github.com/my-dev-lib/pretty-log-go"
	"time"
	"venu-data/config"
	"venu-data/consumer/base"
)

// 定义接口
type DataConsumer interface {
	Topic() string
	GroupId() string
	Id() string
	Consume(msg *DataMessage) error
	SetTopic(topic string)
	SetGroupId(groupId string)
}

// 定义消息结构
type DataMessage struct {
	Content string
}

// 实现接口的结构体
type ReaderConsumer struct {
	log     *pretty_log.Log
	topic   string
	groupId string
	id      string
}

// 构造函数，用于初始化 WriteConsumer2 并设置初始值
func NewInfluxReaderConsumer(topicConf config.TopicConfig) *ReaderConsumer {
	return &ReaderConsumer{
		log:     pretty_log.NewLog("IIC"),
		topic:   topicConf.Name,
		groupId: topicConf.GroupID,
		id:      topicConf.GroupID + "_" + uuid.New().String(),
	}
}

func (rc *ReaderConsumer) Topic() string {
	return rc.topic
}

func (rc *ReaderConsumer) GroupId() string {
	return rc.groupId
}

func (rc *ReaderConsumer) Id() string {
	return rc.id
}

func (rc *ReaderConsumer) handlePlus(dbName string, msg *WriteMessage) {
	pool := obtainPool(dbName)

	t, _ := time.Parse(time.RFC3339Nano, msg.Timestamp)
	err := pool.writeToInfluxDb(msg.Measurement, msg.Tags, msg.Fields, t)
	if err != nil {
		rc.log.W("写入 influxdb 失败：%v", err)
	}
}

func (rc *ReaderConsumer) Consume(msg *base.DataMessage) error {
	// ic.log.D("influx.WriteConsumer 开始消费：%v", string(msg.Value))

	var iwMsg WriteMessage
	err := json.Unmarshal(msg.Value, &iwMsg)

	if err != nil {
		return fmt.Errorf("json 解析错误：%v", err)
	}

	rc.handlePlus(iwMsg.DbName, &iwMsg)
	return nil
}
