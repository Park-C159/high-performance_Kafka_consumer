package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	pretty_log "github.com/my-dev-lib/pretty-log-go"
	"venu-data/config"
	"venu-data/consumer/base"
)

type ReaderConsumer struct {
	log     *pretty_log.Log
	pools   map[string]*Pool
	topic   string
	groupId string
	id      string
}

func NewMysqlReaderConsumer(topicConf config.TopicConfig) *ReaderConsumer {
	return &ReaderConsumer{
		log:     pretty_log.NewLog("IIC"),
		pools:   make(map[string]*Pool),
		topic:   topicConf.Name,
		groupId: topicConf.GroupID,
		id:      topicConf.GroupID + "_" + uuid.New().String(),
	}
}

//func (cc *CreateConsumer) init() {
//	cc.pools = make(map[string]*Pool)
//}

func (mc *ReaderConsumer) Topic() string {
	return mc.topic
}
func (mc *ReaderConsumer) GroupId() string {
	return mc.groupId
}

func (mc *ReaderConsumer) Id() string {
	return mc.id
}

func (mc *ReaderConsumer) handlePlus(msg *InsertMessage) {
	dbName := msg.DbName
	pool, ok := mc.pools[dbName]
	if !ok {
		cfg := config.Get().MysqlDb
		poolSize := config.GetBaseConfig().MysqlPoolSize
		pool = NewPool(poolSize, dbName, cfg.Host, cfg.Port, cfg.User, cfg.Pwd, false)
		pool.init()
		mc.pools[dbName] = pool
	}
	createSql := GenerateCreateTableSQL(msg)
	err := pool.createTable(createSql)
	if err != nil {
		mc.log.E("创建数据库%s表%s失败: %v", msg.DbName, msg.TableName, err)
	}

	err = pool.writeToMysqlDb(msg.TableName, msg.Data)
	if err != nil {
		mc.log.W("写入数据库失败：%v", err)
	}
}

func (mc *ReaderConsumer) Consume(msg *base.DataMessage) error {
	// ic.log.D("mysql.InsertConsumer 开始消费：%v", string(msg.Value))

	var miMsg InsertMessage
	err := json.Unmarshal(msg.Value, &miMsg)
	if err != nil {
		return fmt.Errorf("#InsertConsumer.Consume json 解析错误：%v", err)
	}

	mc.handlePlus(&miMsg)
	return nil
}
