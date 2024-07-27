package influx

import (
	"errors"
	"fmt"
	log "github.com/my-dev-lib/pretty-log-go"
	"sync"
	"sync/atomic"
	"time"

	client "github.com/influxdata/influxdb1-client/v2"
)

const (
	dbStatusOk   = 1
	dbStatusErr  = 2
	dbStatusInit = 3
)

type Point struct {
	Measurement string            `json:"measurement"`
	Tags        map[string]string `json:"tags"`
	Fields      map[string]any    `json:"fields"`
	Timestamp   time.Time         `json:"timestamp"`
}

type Client struct {
	dbClient client.Client
	status   atomic.Uint32
	lock     sync.Mutex

	database string
	host     string
	port     string

	debug bool
	log   *log.Log
}

func NewClient(database string, host string, port string, debug bool) *Client {
	c := &Client{}
	c.init(database, host, port, debug)

	c.log = log.NewLog("IDC")
	c.log.SetFlag(log.FlagColorEnabled)
	return c
}

func (dc *Client) init(database string, host string, port string, debug bool) {
	dc.database = database
	dc.host = host
	dc.port = port
	dc.debug = debug
}

func (dc *Client) connect() error {
	url := fmt.Sprintf("http://%s:%s", dc.host, dc.port)
	c, err := client.NewHTTPClient(client.HTTPConfig{Addr: url, Timeout: 1 * time.Minute})
	dc.dbClient = c
	if err != nil {
		fmt.Print(err)
		return err
	}

	return nil
}

func (dc *Client) ensureDatabase() error {
	//noinspection ALL
	q := client.NewQuery("create database "+dc.database, "", "")
	var c = dc.dbClient
	if response, err := c.Query(q); err == nil && response.Error() == nil {
		if dc.debug {
			dc.log.I("数据库创建成功 %s", dc.database)
		}
	} else {
		if dc.debug {
			dc.log.I("数据库创建失败：", err.Error())
		}

		return fmt.Errorf("创建数据库失败：%s", dc.database)
	}

	return nil
}

func (dc *Client) initDb() error {
	err := dc.connect()
	if err != nil {
		return fmt.Errorf("connect %v", err)
	}

	err = dc.ensureDatabase()
	if err != nil {
		return fmt.Errorf("ensureDatabase %v", err)
	}

	return nil
}

func (dc *Client) Init() error {
	if dc.status.Load() == dbStatusOk {
		return nil
	}

	if dc.status.Load() == dbStatusInit {
		return errors.New("influxdb init")
	}

	dc.lock.Lock()
	defer dc.lock.Unlock()

	if dc.status.Load() == dbStatusOk {
		return nil
	}

	if dc.status.Load() == dbStatusInit {
		return errors.New("influxdb init")
	}

	dc.status.Store(dbStatusInit)
	if err := dc.initDb(); err == nil {
		dc.status.Store(dbStatusOk)
		return nil
	} else {
		dc.status.Store(dbStatusErr)
		return errors.New("influxdb init error")
	}
}

func (dc *Client) WriteBatch(records *[]Point) error {
	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{Database: dc.database})
	// 重置时间，避免重复
	startTime := time.Now()
	for _, r := range *records {
		p, err := client.NewPoint(r.Measurement, r.Tags, r.Fields, startTime)
		startTime.Add(time.Duration(1))
		if err != nil {
			return err
		}

		bp.AddPoint(p)
	}

	if err := dc.dbClient.Write(bp); err != nil {
		return err
	}

	if dc.debug {
		dc.log.D("写入数据 %d 条", len(bp.Points()))
	}

	return nil
}

// Write 注意写入不支持 uint32/64 类型
func (dc *Client) Write(measurement string, tags map[string]string,
	fields map[string]any, timestamp time.Time) error {

	bp, _ := client.NewBatchPoints(client.BatchPointsConfig{Database: dc.database})
	p, err := client.NewPoint(measurement, tags, fields, timestamp)
	if err != nil {
		return err
	}

	bp.AddPoint(p)

	if err := dc.dbClient.Write(bp); err != nil {
		return err
	}

	if dc.debug {
		dc.log.D("写入数据 %d 条", len(bp.Points()))
	}

	return nil
}
