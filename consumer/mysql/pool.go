package mysql

import (
	"database/sql"
	"fmt"
	log "github.com/my-dev-lib/pretty-log-go"
	"sync"
	"time"
	"venu-data/config"
)

const (
	handleMaxBufferSize  = 10
	writeMaxIntervalTime = 10 * time.Second
	poolChannelSize      = 100
)

type Handler struct {
	client  *Client
	channel chan InsertRequest
}

type DbInfo struct {
	name string
	host string
	port string
	user string
	pwd  string
}

type Pool struct {
	dbHandlers    []*Handler
	currentIndex  int
	lastWriteTime time.Time
	handlerLock   sync.Mutex
	dbInfo        *DbInfo
	debug         bool
	log           *log.Log
}

func NewPool(poolSize uint32, db string, host string, port string, user string, pwd string, debug bool) *Pool {
	mdp := &Pool{
		dbHandlers: make([]*Handler, poolSize),
		dbInfo: &DbInfo{
			name: db,
			host: host,
			port: port,
			user: user,
			pwd:  pwd,
		},
		debug: debug,
	}
	mdp.log = log.NewLog("MP")

	// 记录连接池创建信息
	//mdp.log.D("创建连接池: %s@%s:%s/%s", user, host, port, db)
	mdp.init()
	return mdp
}

func (mdp *Pool) init() {
	for i := 0; i < len(mdp.dbHandlers); i++ {
		client := NewClient(mdp.dbInfo.name, mdp.dbInfo.host, mdp.dbInfo.port, mdp.dbInfo.user, mdp.dbInfo.pwd, mdp.debug)
		element := &Handler{
			client: client, channel: make(chan InsertRequest, config.GetBaseConfig().MysqlPoolChannelSize),
		}

		mdp.dbHandlers[i] = element
		go mdp.handleMysqlDbChan(element)
	}
}

func (mdp *Pool) obtainHandler() *Handler {
	if mdp.currentIndex >= len(mdp.dbHandlers) {
		mdp.currentIndex = 0
	}

	handler := mdp.dbHandlers[mdp.currentIndex]
	mdp.currentIndex = mdp.currentIndex + 1

	return handler
}

func (mdp *Pool) FindIPv4(table string, hostName string) (string, error) {
	// 获取一个数据库处理器
	handler := mdp.obtainHandler()
	// SQL 查询语句
	sqlQuery := fmt.Sprintf("SELECT IP FROM %s WHERE name = ?", table)

	// 初始化数据库连接
	err := handler.client.Init()
	if err != nil {
		mdp.log.E("初始化客户端失败: %v", err)
		return "", fmt.Errorf("初始化客户端失败: %v", err)
	}

	// 查询数据库
	var ip string
	err = handler.client.QueryRow(sqlQuery, hostName).Scan(&ip)
	if err != nil {
		if err == sql.ErrNoRows {
			mdp.log.W("没有找到主机名对应的 IP: %s", hostName)
			return "", nil
		}
		mdp.log.E("查询 IP 失败: %v", err)
		return "", fmt.Errorf("查询 IP 失败: %v", err)
	}

	return ip, nil
}

func (mdp *Pool) writeToMysqlDb(table string, data map[string]any) error {
	copiedData := make(map[string]any)

	for k, v := range data {
		copiedData[k] = v
	}
	//mdp.log.D("copedData:", copiedData)

	handler := mdp.obtainHandler()
	handler.channel <- InsertRequest{
		Table: table,
		Data:  copiedData,
	}
	return nil
}

func (mdp *Pool) createTable(sqlStatement string) error {
	element := mdp.obtainHandler()
	err := element.client.Init()
	if err != nil {
		return fmt.Errorf("初始化客户端失败: %v", err)
	}

	err = element.client.CreateTable(sqlStatement)
	if err != nil {
		return fmt.Errorf("创建表失败: %v", err)
	}

	return nil
}

func (mdp *Pool) getCount(table string, hostname string, serialNumber string) (string, int, error) {
	handler := mdp.obtainHandler()
	sqlQuery := fmt.Sprintf("SELECT boot_time, boot_count FROM %s WHERE hostname = ? AND serial_number = ?", table)

	// 初始化数据库连接
	err := handler.client.Init()
	if err != nil {
		mdp.log.E("初始化客户端失败: %v", err)
		return "", -1, fmt.Errorf("初始化客户端失败: %v", err)
	}

	// 查询数据库
	var bootTime string
	var bootCount int
	err = handler.client.QueryRow(sqlQuery, hostname, serialNumber).Scan(&bootTime, &bootCount)
	if err != nil {
		return "", -1, err
	}

	return bootTime, bootCount, nil
}

func (mdp *Pool) handleMysqlDbChan(handler *Handler) {
	var writeBuffer []InsertRequest
	for {
		value := <-handler.channel

		var buffer []InsertRequest
		mdp.handlerLock.Lock()
		writeBuffer = append(writeBuffer, value)
		// 不满足条件解锁,继续接收缓冲区消息
		if !mdp.canInsertBatch(writeBuffer) {
			mdp.handlerLock.Unlock()
			continue
		}

		buffer = writeBuffer
		mdp.handlerLock.Unlock()

		err := handler.client.Init()
		if err != nil {
			mdp.log.E("初始化客户端失败: %v", err)
			continue
		}
		err = handler.client.RequestBatch(&buffer)
		if err != nil {
			mdp.log.E("批量请求失败: %v", err)
			continue
		}

		mdp.handlerLock.Lock()
		mdp.lastWriteTime = time.Now() // 更新最后一次写入时间
		writeBuffer = []InsertRequest{}
		mdp.handlerLock.Unlock()
	}
}

func (mdp *Pool) canInsertBatch(writeBuffer []InsertRequest) bool {

	return len(writeBuffer) >= config.GetBaseConfig().MysqlMaxBufferSize || time.Now().After(mdp.lastWriteTime.Add(time.Duration(config.GetBaseConfig().MysqlMaxIntervalTime)*time.Second))
}
