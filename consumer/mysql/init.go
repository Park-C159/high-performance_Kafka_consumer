package mysql

import (
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	prettyLog "github.com/my-dev-lib/pretty-log-go"
	"sync"
	"venu-data/config"
	"venu-data/consumer/base"
)

const (
	topicInsert             = "mysql_insert_switch"
	topicCreateTable        = "mysql_create_table_switch"
	topicInsertGroupId      = "mysql_insert_switch_group_0"
	topicCreateTableGroupId = "mysql_create_table_switch_group_0"
	poolSize                = 80
)

var (
	createTableLock    sync.Mutex
	createTableOnceMap = make(map[string]bool)
)

type CreateTableMessage struct {
	DbName    string `json:"db_name"`
	TableName string `json:"table_name"`
	Sql       string `json:"sql"`
}

type CreateConsumer struct {
	log   *prettyLog.Log
	pools map[string]*Pool
	id    string
}

func (cc *CreateConsumer) init() {
	cc.pools = make(map[string]*Pool)
}

func (cc *CreateConsumer) Topic() string {
	return topicCreateTable
}

func (cc *CreateConsumer) GroupId() string {
	return topicCreateTableGroupId
}

func (cc *CreateConsumer) Id() string {
	return cc.id
}

func (cc *CreateConsumer) handle(msg *CreateTableMessage) {
	dbName := msg.DbName
	pool, ok := cc.pools[dbName]
	if !ok {
		cfg := config.Get().MysqlDb
		pool = NewPool(poolSize, dbName, cfg.Host, cfg.Port, cfg.User, cfg.Pwd, false)
		cc.pools[dbName] = pool
	}

	err := pool.createTable(msg.Sql)
	cc.log.D("创建表成功 %s", msg.TableName)
	if err != nil {
		cc.log.W("创建表失败：%v", err)
	}

	createTableLock.Lock()
	createTableOnceMap[msg.TableName] = true
	createTableLock.Unlock()
}

func (cc *CreateConsumer) Consume(msg *base.DataMessage) error {
	// cc.log.D("mysql.CreateConsumer 开始消费：%v", string(msg.Value))

	var ctMsg CreateTableMessage
	err := json.Unmarshal(msg.Value, &ctMsg)
	if err != nil {
		return fmt.Errorf("#CreateConsumer.Consume json 解析错误：%v", err)
	}

	createTableLock.Lock()
	_, ok := createTableOnceMap[ctMsg.TableName]
	if ok {
		createTableLock.Unlock()
		return nil
	}

	createTableLock.Unlock()

	cc.handle(&ctMsg)
	return nil
}

func NewCreateConsumer() *CreateConsumer {
	return &CreateConsumer{
		log:   prettyLog.NewLog("MCTC"),
		pools: make(map[string]*Pool),
		id:    topicCreateTableGroupId + "_" + uuid.New().String(),
	}
}

type InsertMessage struct {
	DbName    string         `json:"db_name"`
	TableName string         `json:"table_name"`
	Data      map[string]any `json:"data"`
}

type InsertConsumer struct {
	pools map[string]*Pool
	log   *prettyLog.Log
	id    string
}

func (ic *InsertConsumer) GroupId() string {
	return topicInsertGroupId
}

func (ic *InsertConsumer) Topic() string {
	return topicInsert
}

func (ic *InsertConsumer) Id() string {
	return ic.id
}

func (ic *InsertConsumer) handle(msg *InsertMessage) {
	dbName := msg.DbName
	pool, ok := ic.pools[dbName]
	if !ok {
		cfg := config.Get().MysqlDb
		pool = NewPool(1, dbName, cfg.Host, cfg.Port, cfg.User, cfg.Pwd, false)
		pool.init()
		ic.pools[dbName] = pool
	}

	err := pool.writeToMysqlDb(msg.TableName, msg.Data)
	if err != nil {
		ic.log.W("写入数据库失败：%v", err)
	}
}

func (ic *InsertConsumer) Consume1(msg *base.DataMessage) error {
	// ic.log.D("mysql.InsertConsumer 开始消费：%v", string(msg.Value))

	var miMsg InsertMessage
	err := json.Unmarshal(msg.Value, &miMsg)
	if err != nil {
		return fmt.Errorf("#InsertConsumer.Consume json 解析错误：%v", err)
	}

	ic.handle(&miMsg)
	return nil
}

func NewInsertConsumer() *InsertConsumer {
	return &InsertConsumer{
		log:   prettyLog.NewLog("MIC"),
		pools: make(map[string]*Pool),
		//id:    topicInsertGroupId + "_" + fmt.Sprintf("%d", time.Now().Nanosecond()),
		id: topicInsertGroupId + "_" + uuid.New().String(),
	}
}
