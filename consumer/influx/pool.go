package influx

import (
	log "github.com/my-dev-lib/pretty-log-go"
	"sync"
	"time"
	"venu-data/config"
)

const (
	// writeMaxBufferSize 20000 满足大约每 100 台交换机
	writeMaxBufferSize   = 5000
	writeMaxIntervalTime = 5 * time.Second
	poolChannelSize      = 100
)

type Handler struct {
	client  *Client
	channel chan Point
}

type Pool struct {
	dbHandlers    []*Handler
	currentIndex  int
	lastWriteTime time.Time
	handlerLock   sync.Mutex
	db            string
	host          string
	port          string
	debug         bool
	log           *log.Log
}

func NewPool(poolSize uint32, dbname string, host string, port string, debug bool) *Pool {
	idp := &Pool{dbHandlers: make([]*Handler, poolSize), db: dbname, host: host, port: port, debug: debug}
	idp.log = log.NewLog("IP")
	idp.init()
	return idp
}

func (idp *Pool) init() {
	idp.lastWriteTime = time.Now()
	for i := 0; i < len(idp.dbHandlers); i++ {
		client := NewClient(idp.db, idp.host, idp.port, idp.debug)

		element := &Handler{
			client: client, channel: make(chan Point, config.GetBaseConfig().InfluxPoolChannelSize),
		}

		idp.dbHandlers[i] = element
		go idp.handleInfluxDbChan(element)
	}
}

func (idp *Pool) obtainHandler() *Handler {
	if idp.currentIndex >= len(idp.dbHandlers) {
		idp.currentIndex = 0
	}

	handler := idp.dbHandlers[idp.currentIndex]
	idp.currentIndex = idp.currentIndex + 1

	return handler
}

func (idp *Pool) writeToInfluxDb(measurement string, tags map[string]string,
	fields map[string]any, timestamp time.Time) error {

	// 深拷贝tags
	copiedTags := make(map[string]string)
	for k, v := range tags {
		copiedTags[k] = v
	}

	// 深拷贝fields
	copiedFields := make(map[string]any)
	for k, v := range fields {
		copiedFields[k] = v
	}

	idp.obtainHandler().channel <- Point{
		Measurement: measurement,
		Tags:        copiedTags,
		Fields:      copiedFields,
		Timestamp:   timestamp,
	}

	return nil
}

func (idp *Pool) handleInfluxDbChan(handler *Handler) {
	var writeBuffer []Point
	for {
		value := <-handler.channel

		var buffer []Point
		idp.handlerLock.Lock()
		writeBuffer = append(writeBuffer, value)
		if !idp.canWriteBatch(writeBuffer) {
			idp.handlerLock.Unlock()
			continue
		}

		buffer = writeBuffer
		idp.handlerLock.Unlock()

		_ = handler.client.Init()
		err := handler.client.WriteBatch(&buffer)
		if err != nil {
			idp.log.E("influxDbClient.Write: %v", err)
		}

		idp.handlerLock.Lock()
		idp.lastWriteTime = time.Now()
		writeBuffer = []Point{}
		idp.handlerLock.Unlock()
	}
}

func (idp *Pool) canWriteBatch(writeBuffer []Point) bool {
	return len(writeBuffer) >= config.GetBaseConfig().InfluxMaxBufferSize || time.Now().After(idp.lastWriteTime.Add(time.Duration(config.GetBaseConfig().InfluxMaxIntervalTime)*time.Second))
}
