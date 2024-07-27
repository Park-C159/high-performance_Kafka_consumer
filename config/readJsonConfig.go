package config

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
)

type BaseConfig struct {
	MysqlPoolSize        uint32 `json:"mysql_pool_size"`
	MysqlMaxBufferSize   int    `json:"mysql_max_buffer_size"`
	MysqlMaxIntervalTime int    `json:"mysql_max_interval_time"`
	MysqlPoolChannelSize uint32 `json:"mysql_pool_channel_size"`

	InfluxPoolSize        uint32 `json:"influx_pool_size"`
	InfluxMaxBufferSize   int    `json:"influx_max_buffer_size"`
	InfluxMaxIntervalTime int    `json:"influx_max_interval_time"`
	InfluxPoolChannelSize uint32 `json:"influx_pool_channel_size"`
}
type TopicConfig struct {
	Name        string `json:"name"`
	GroupID     string `json:"group_id"`
	StorageType string `json:"storage_type"`
	ConsumeNum  int    `json:"consume_num"`
}

func LoadConfigFromFile(filename string) error {
	file, err := ioutil.ReadFile(filename)
	if err != nil {
		return fmt.Errorf("读取配置文件失败：%v", err)
	}

	var fileConfig struct {
		Base   BaseConfig    `json:"base"`
		Topics []TopicConfig `json:"topics"`
		VenusDataConfig
	}

	err = json.Unmarshal(file, &fileConfig)
	if err != nil {
		return fmt.Errorf("解析配置文件失败：%v", err)
	}

	config.Base = &fileConfig.Base
	config.Topics = fileConfig.Topics
	config.content = &fileConfig.VenusDataConfig
	return nil
}

func GetBaseConfig() BaseConfig {
	return *config.Base
}

func GetTopicsConfig() []TopicConfig {
	return config.Topics
}
