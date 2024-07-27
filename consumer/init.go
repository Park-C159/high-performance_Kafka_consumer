package consumer

import (
	"context"
	"fmt"
	prettyLog "github.com/my-dev-lib/pretty-log-go"
	"github.com/segmentio/kafka-go"
	log2 "log"
	"venu-data/config"
	"venu-data/consumer/base"
	"venu-data/consumer/influx"
	"venu-data/consumer/mysql"
	"venu-data/internal/argparser"
)

const (
	version = "1.0.0"
)

type DataConsumer interface {
	Topic() string
	GroupId() string
	Id() string
	Consume(msg *base.DataMessage) error
}

type VenusConsumer struct {
	debug     bool
	log       *prettyLog.Log
	reader    *kafka.Reader
	consumers map[string]DataConsumer
}

func (vc *VenusConsumer) Init() {
	vc.log = prettyLog.NewLog("VD")
	vc.consumers = make(map[string]DataConsumer)

	topicsConf := config.GetTopicsConfig()

	for _, c := range vc.getConsumers2(topicsConf) {
		vc.log.I("消费者已注册：topic[%s]，group[%s], id[%s]", c.Topic(), c.GroupId(), c.Id())
		vc.RegisterDataConsumer(c)
	}
}

func (vc *VenusConsumer) RegisterDataConsumer(consumer DataConsumer) {
	key := consumer.Id()
	if _, ok := vc.consumers[key]; ok {
		panic(fmt.Errorf("已存在 consumer：%s", key))
	}

	vc.consumers[key] = consumer
}

func (vc *VenusConsumer) UnRegisterDataConsumer(consumer DataConsumer) {
	key := consumer.Id()
	if _, ok := vc.consumers[key]; ok {
		delete(vc.consumers, key)
	}
}

func (vc *VenusConsumer) Handle(consume DataConsumer) {
	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: config.Get().KafkaBrokers,
		Topic:   consume.Topic(),
		GroupID: consume.GroupId(),
	})
	for {
		msg, err := reader.ReadMessage(context.Background())

		if err != nil {
			vc.log.E("r.ReadMessage %v", err)
			continue
		}
		// vc.log.D("收到消息：%v", string(msg.Key))
		err = consume.Consume(&base.DataMessage{Message: msg})
		if err != nil {
			vc.log.E("消费出错(%v, t[%s]，gid[%s])：%v", consume, consume.Topic(), consume.GroupId(), err)
		}
	}
}

func (vc *VenusConsumer) Start() {
	for _, consumer := range vc.consumers {
		go vc.Handle(consumer)
	}
}

func (vc *VenusConsumer) Close() {
	_ = vc.reader.Close()
}

func (*VenusConsumer) getConsumers2(topicsConf []config.TopicConfig) []DataConsumer {
	var dataConsumers []DataConsumer

	for _, conf := range topicsConf {
		if conf.StorageType == "mysql" {
			if conf.Name == "serverResource" {
				for i := 0; i < conf.ConsumeNum; i++ {
					dataConsumers = append(dataConsumers, mysql.NewMysqlServeResourceReaderConsumer(conf))
				}
			} else {
				for i := 0; i < conf.ConsumeNum; i++ {
					dataConsumers = append(dataConsumers, mysql.NewMysqlReaderConsumer(conf))
				}
			}
		} else if conf.StorageType == "influxdb" {
			for i := 0; i < conf.ConsumeNum; i++ {
				dataConsumers = append(dataConsumers, influx.NewInfluxReaderConsumer(conf))
			}
		}
	}
	return dataConsumers
}

var log = prettyLog.NewLog("VD")

type StartArg struct {
	influx string
	mysql  string
	kafka  string
}

// 获取命令行配置，初始化
func handleArgs() StartArg {
	kafkaArgName := "kafka"
	mysqlArgName := "mysql"
	influxArgName := "influx"

	argParser := argparser.NewArgParser([][]any{
		{kafkaArgName, argparser.TypeString, "Kafka 地址，格式为 192.168.1.1:9092;192.168.1.2:9092", ""},
		{mysqlArgName, argparser.TypeString, "mysql 地址，格式为 192.168.1.1:3306@root/123456", ""},
		{influxArgName, argparser.TypeString, "influx 地址，格式为 192.168.1.1:8086", ""},
	})

	ret, err := argParser.Parse("venus-data")
	if err != nil {
		log2.Fatalf("命令行参数解析失败：%v", err)
	}

	if ret[kafkaArgName] == nil || ret[mysqlArgName] == nil || ret[influxArgName] == nil {
		log2.Fatal("请提供 kafka/mysql/influx 参数")
	}

	arg := StartArg{
		influx: ret[influxArgName].(string),
		mysql:  ret[mysqlArgName].(string),
		kafka:  ret[kafkaArgName].(string),
	}

	return arg
}

func Start() {
	err := config.LoadConfigFromFile("config/config.json")
	if err != nil {
		log.E("加载配置文件失败：%v", err)
	}
	arg := handleArgs()
	err = config.Init(arg.kafka, arg.mysql, arg.influx)
	if err != nil {
		log2.Fatalf("命令行参数解析失败：%v", err)
	}

	log.I("\n" + prettyLog.GetHighlightLine(fmt.Sprintf("消费程序已启动 v%s", version), 30))

	var vc VenusConsumer
	vc.Init()
	vc.Start()
}
