package mysql

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/google/uuid"
	pretty_log "github.com/my-dev-lib/pretty-log-go"
	"time"
	"venu-data/config"
	"venu-data/consumer/base"
)

type ServeResourceReaderConsumer struct {
	log     *pretty_log.Log
	pools   map[string]*Pool
	topic   string
	groupId string
	id      string
}
type ServerResource struct {
	Hostname     string `json:"hostname"`
	IPv4         string `json:"ipv4"`
	Manufacturer string `json:"manufacturer"`
	SerialNumber string `json:"serial_number"`
	ProductName  string `json:"product_name"`
	BootTime     string `json:"boot_time"`
	BootCount    int    `json:"boot_count"`
	StaffID      int    `json:"staff_id"`
	Status       string `json:"status"`
}

func NewMysqlServeResourceReaderConsumer(topicConf config.TopicConfig) *ServeResourceReaderConsumer {
	return &ServeResourceReaderConsumer{
		log:     pretty_log.NewLog("IIC"),
		pools:   make(map[string]*Pool),
		topic:   topicConf.Name,
		groupId: topicConf.GroupID,
		id:      topicConf.GroupID + "_" + uuid.New().String(),
	}
}
func (mc *ServeResourceReaderConsumer) Topic() string {
	return mc.topic
}
func (mc *ServeResourceReaderConsumer) GroupId() string {
	return mc.groupId
}

func (mc *ServeResourceReaderConsumer) Id() string {
	return mc.id
}

func (mc *ServeResourceReaderConsumer) handlePlus(msg *InsertMessage) {
	dbName := msg.DbName
	pool, ok := mc.pools[dbName]
	cfg := config.Get().MysqlDb
	poolSize := config.GetBaseConfig().MysqlPoolSize
	if !ok {
		pool = NewPool(poolSize, dbName, cfg.Host, cfg.Port, cfg.User, cfg.Pwd, false)
		pool.init()
		mc.pools[dbName] = pool
	}
	createSql := `CREATE TABLE IF NOT EXISTS server_resource
			(
				hostname        VARCHAR(255),
				ipv4            VARCHAR(255),
				manufacturer    VARCHAR(255),
				serial_number   VARCHAR(255),
				product_name    VARCHAR(255),
				boot_time       DATETIME,
				boot_count      INT DEFAULT 0,
				staff_id        INT,
				status          VARCHAR(255),
				PRIMARY KEY (hostname, serial_number)
			);`
	err := pool.createTable(createSql)
	if err != nil {
		mc.log.E("创建数据库%s表%s失败: %v", msg.DbName, msg.TableName, err)
	}

	pool2, ok2 := mc.pools["venus_master"]
	if !ok2 {
		pool2 = NewPool(poolSize, "venus_master", cfg.Host, cfg.Port, cfg.User, cfg.Pwd, false)
		pool2.init()
		mc.pools["venus_master"] = pool2
	}
	hostname := msg.Data["hostname"].(string)
	serialNumber := msg.Data["serial_number"].(string)
	bootTime := msg.Data["boot_time"].(string)

	ip, err := pool2.FindIPv4("map_table", hostname)
	if err != nil {
		mc.log.E("查询ip失败：%v", err)
	}
	msg.Data["ipv4"] = ip

	// 获取当前记录的开机时间和开机次数
	var currentBootTimeStr string
	var bootCount int
	currentBootTimeStr, bootCount, err = pool.getCount(msg.TableName, hostname, serialNumber)

	if err != nil {
		if err == sql.ErrNoRows {
			// 如果没有记录，设置bootCount为1
			bootCount = 1
		} else {
			mc.log.E("Failed to query boot time and boot count: ", err)
		}
	} else {
		var currentBootTime time.Time
		if currentBootTimeStr != "" {
			currentBootTime, err = time.Parse("2006-01-02 15:04:05", currentBootTimeStr)
			if err != nil {
				mc.log.E("Failed to parse boot time: ", err)
			}
		}
		boot_time, _ := time.Parse("2006-01-02 15:04:05", bootTime)
		// 如果开机时间有变化，更新开机时间并增加开机次数
		if !currentBootTime.Equal(boot_time) {
			bootCount++
		}
	}

	msg.Data["boot_count"] = bootCount

	err = pool.writeToMysqlDb(msg.TableName, msg.Data)
	if err != nil {
		mc.log.W("写入数据库失败：%v", err)
	}
}

func (mc *ServeResourceReaderConsumer) Consume(msg *base.DataMessage) error {
	var miMsg InsertMessage
	err := json.Unmarshal(msg.Value, &miMsg)
	if err != nil {
		return fmt.Errorf("#InsertConsumer.Consume json 解析错误：%v", err)
	}

	mc.handlePlus(&miMsg)
	return nil
}
