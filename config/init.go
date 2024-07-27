package config

import (
	"fmt"
	"strings"
)

type InfluxDbConfig struct {
	Host string
	Port string
}
type MysqlDbConfig struct {
	Host string
	Port string
	User string
	Pwd  string
}
type VenusDataConfig struct {
	KafkaBrokers []string
	InfluxDb     InfluxDbConfig
	MysqlDb      MysqlDbConfig
}

type Config struct {
	content *VenusDataConfig
	Base    *BaseConfig
	Topics  []TopicConfig
}

var config = &Config{}

// 初始化连接kafka和influxdb，mysql
func Init(kafka string, mysql string, influx string) error {
	var conf = &VenusDataConfig{}

	conf.KafkaBrokers = strings.Split(kafka, ";")
	influxDbConfig := strings.Split(influx, ":")
	if len(influxDbConfig) != 2 {
		return fmt.Errorf("InfluxDB 配置不正确：%s", influx)
	}

	conf.InfluxDb = InfluxDbConfig{
		Host: influxDbConfig[0], Port: influxDbConfig[1],
	}

	mysqlFull := strings.Split(mysql, "@")
	if mysqlFull == nil || len(mysqlFull) != 2 {
		return fmt.Errorf("mysql 配置有误：%v", mysql)
	}

	mysqlIpCfg := strings.Split(mysqlFull[0], ":")
	mysqlPwdCfg := strings.Split(mysqlFull[1], "/")
	if mysqlIpCfg == nil || len(mysqlIpCfg) != 2 || mysqlPwdCfg == nil || len(mysqlPwdCfg) != 2 {
		return fmt.Errorf("mysql 配置有误：%v", mysql)
	}

	conf.MysqlDb = MysqlDbConfig{
		Host: mysqlIpCfg[0],
		Port: mysqlIpCfg[1],
		User: mysqlPwdCfg[0],
		Pwd:  mysqlPwdCfg[1],
	}

	config.content = conf
	return nil
}

func Get() VenusDataConfig {
	return *config.content
}
