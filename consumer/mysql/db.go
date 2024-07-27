package mysql

import (
	"database/sql"
	"errors"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	log "github.com/my-dev-lib/pretty-log-go"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

const (
	dbStatusOk   = 1
	dbStatusErr  = 2
	dbStatusInit = 3
)

type InsertRequest struct {
	Table string
	Data  map[string]any
}

type Client struct {
	dbClient *sql.DB
	status   atomic.Uint32
	lock     sync.Mutex

	database string
	host     string
	port     string
	user     string
	pwd      string

	debug    bool
	sqlDebug bool
	dbLog    *log.Log
}

func NewClient(database string, host string, port string, user string, pwd string, debug bool) *Client {
	dbLog := log.NewLog("MDC")
	dbLog.SetFlag(log.FlagColorEnabled)
	return &Client{
		database: database,
		host:     host,
		port:     port,
		user:     user,
		pwd:      pwd,
		dbLog:    dbLog,
		debug:    debug,
	}
}

func (dc *Client) connDb() error {
	sourcePrefix := "%s:%s@tcp(%s:%s)/"
	source := fmt.Sprintf(sourcePrefix, dc.user, dc.pwd, dc.host, dc.port)
	if dc.debug {
		dc.dbLog.D("ConnDb open %s", source)
	}

	db, err := sql.Open("mysql", source)
	if err != nil {
		return err
	}

	dc.dbClient = db
	err = dc.initDb()
	if err != nil {
		return err
	}

	_ = db.Close()

	source = fmt.Sprintf(sourcePrefix+"%s", dc.user, dc.pwd, dc.host, dc.port, dc.database)
	db, err = sql.Open("mysql", source)
	if err != nil {
		return err
	}

	// 设置数据库客户端
	dc.dbClient = db
	// 设置连接池参数
	db.SetMaxOpenConns(100) // 最大打开连接数
	db.SetMaxIdleConns(10)  // 最大空闲连接数
	db.SetConnMaxLifetime(2 * time.Minute)
	return db.Ping()
}

func (dc *Client) initDb() error {
	//noinspection ALL
	query := "create database if not exists " + dc.database
	if dc.debug {
		dc.dbLog.D("initDb exec %s", query)
	}

	_, err := dc.dbClient.Exec(query)
	return err
}

func (dc *Client) Init() error {
	if dc.status.Load() == dbStatusOk {
		return nil
	}

	if dc.status.Load() == dbStatusInit {
		return errors.New("mysqldb init")
	}

	dc.lock.Lock()
	defer dc.lock.Unlock()

	if dc.status.Load() == dbStatusOk {
		return nil
	}

	if dc.status.Load() == dbStatusInit {
		return errors.New("mysqldb init")
	}

	dc.status.Store(dbStatusInit)
	if err := dc.connDb(); err == nil {
		dc.status.Store(dbStatusOk)
		return nil
	} else {
		dc.status.Store(dbStatusErr)
		return errors.New("mysqldb init error")
	}
}

func (dc *Client) CreateTable(sqlStatement string) error {
	_, err := dc.dbClient.Exec(sqlStatement)
	return err
}
func (dc *Client) QueryRow(query string, args ...interface{}) *sql.Row {
	return dc.dbClient.QueryRow(query, args...)
}

func (dc *Client) Query(table string, names []string, filter string) ([]map[string]any, error) {
	var selectColumns string
	if names == nil || len(names) <= 0 {
		selectColumns = "*"
	} else {
		selectColumns = strings.Join(names, ", ")
	}

	//noinspection ALL
	querySql := fmt.Sprintf("select %s from %s.%s", selectColumns, dc.database, table)

	if filter != "" {
		querySql += " WHERE " + filter
	}

	if dc.debug && dc.sqlDebug {
		dc.dbLog.D("Query exec: %s", querySql)
	}

	rows, err := dc.dbClient.Query(querySql)
	if err != nil {
		return nil, err
	}

	defer func(rows *sql.Rows) {
		_ = rows.Close()
	}(rows)

	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	var results []map[string]any
	for rows.Next() {
		values := make([]interface{}, len(columns))
		scanResults := make([]any, len(columns))

		for i := range columns {
			values[i] = &scanResults[i]
		}

		if err := rows.Scan(values...); err != nil {
			return nil, err
		}

		rowData := make(map[string]any)
		for i, colName := range columns {
			// Convert scanResult value to its underlying type
			switch v := scanResults[i].(type) {
			case []uint8:
				rowData[colName] = string(v)
			default:
				rowData[colName] = v
			}
		}

		results = append(results, rowData)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}

func (dc *Client) WriteToDbBatch(table string, data []map[string]any) error {
	if len(data) == 0 {
		return nil
	}

	var columns []string
	for column := range data[0] {
		column = fixDbName(column)
		columns = append(columns, column)
	}

	var placeholderGroups []string
	var updates []string
	var values []interface{}
	for _, column := range columns {
		updates = append(updates, fmt.Sprintf("%s = VALUES(%s)", column, column))
	}

	for _, row := range data {
		var placeholders []string
		for _, column := range columns {
			values = append(values, row[column])
			placeholders = append(placeholders, "?")
		}

		placeholderGroups = append(placeholderGroups, fmt.Sprintf("(%s)", strings.Join(placeholders, ", ")))
	}

	stmt := fmt.Sprintf("INSERT INTO %s.%s (%s) VALUES %s ON DUPLICATE KEY UPDATE %s",
		dc.database, table, strings.Join(columns, ", "), strings.Join(placeholderGroups, ", "), strings.Join(updates, ", "))

	if dc.debug && dc.sqlDebug {
		dc.dbLog.D("WriteToDbBatch exec: %s, values: %v", stmt, values)
	}

	_, err := dc.dbClient.Exec(stmt, values...)
	return err
}

func (dc *Client) WriteToDb(table string, data map[string]any) error {
	return dc.WriteToDbBatch(table, []map[string]any{data})
}

func (dc *Client) UpdateDb(table string, data map[string]any, query map[string]any) error {
	var queryParts []string
	for key, val := range query {
		queryParts = append(queryParts, fmt.Sprintf("%s = '%v'", key, val))
	}

	queryStr := strings.Join(queryParts, " AND ")

	results, err := dc.Query(table, nil, queryStr)
	if err != nil {
		return err
	}

	if len(results) <= 0 {
		// 没有数据则插入
		return dc.WriteToDb(table, data)
	}

	// 有数据则更新
	var updateParts []string
	var values []interface{}
	for key, val := range data {
		updateParts = append(updateParts, fmt.Sprintf("%s = ?", key))
		values = append(values, val)
	}

	//noinspection ALL
	updateSql := fmt.Sprintf("UPDATE %s.%s SET %s WHERE %s", dc.database, table, strings.Join(updateParts, ", "), queryStr)

	if dc.debug && dc.sqlDebug {
		dc.dbLog.D("UpdateDb exec: %s, values: %v", updateSql, values)
	}

	_, err = dc.dbClient.Exec(updateSql, values...)
	return err
}

func (dc *Client) clearInvalidData(table string) error {
	if dc.database == "venusdb" {
		return nil
	}

	//noinspection ALL
	deleteStmt := fmt.Sprintf("DELETE FROM %s.%s WHERE update_at < NOW() - INTERVAL 24 HOUR", dc.database, table)
	if _, err := dc.dbClient.Exec(deleteStmt); err != nil {
		dc.dbLog.W("Error deleting old records: %v", err)
		return err
	}

	return nil
}

func (dc *Client) RequestBatch(requests *[]InsertRequest) error {
	requestMap := make(map[string][]map[string]any)

	for _, req := range *requests {
		if _, exists := requestMap[req.Table]; !exists {
			requestMap[req.Table] = []map[string]any{}
		}

		requestMap[req.Table] = append(requestMap[req.Table], req.Data)
	}

	for table, dataMaps := range requestMap {
		if dc.debug {
			dc.dbLog.D("插入 %s 表，%d 条数据", table, len(dataMaps))
		}

		err := dc.WriteToDbBatch(table, dataMaps)
		if err != nil {
			return fmt.Errorf("Failed to write to %s: %v", table, err)
		}

		err = dc.clearInvalidData(table)
		if err != nil {
			dc.dbLog.W("clearInvalidData: %v", err)
		}
	}

	return nil
}

// utils:

// Helper function to determine the SQL column type based on Go data type
func getColumnType(value any, key bool) string {
	switch v := value.(type) {
	case int, int8, int16, int32, int64:
		return "BIGINT"
	case uint, uint8, uint16, uint32, uint64:
		return "BIGINT UNSIGNED"
	case float32, float64:
		return "FLOAT"
	case bool:
		return "BOOLEAN"
	case string:
		if key {
			return "VARCHAR(180)"
		} else {
			return "VARCHAR(255)"
		}
	default:
		fmt.Printf("Unrecognized type for key %v: %T. Using VARCHAR(255).\n", v, v)
		return "VARCHAR(255)"
	}
}

func GetMysqlCreateTableSqlUnionKey(tableName string, data map[string]any, extra map[string]any, unionKeys []string) string {
	var columns []string
	var primaryKeyParts []string

	for key, value := range data {
		key = fixDbName(key)
		isKey := false
		if contains(unionKeys, key) {
			isKey = true
			primaryKeyParts = append(primaryKeyParts, fmt.Sprintf("`%s`", key))
		}

		columnType := getColumnType(value, isKey)
		column := fmt.Sprintf("`%s` %s", key, columnType)
		columns = append(columns, column)
	}

	if extra != nil {
		for key, value := range extra {
			key = fixDbName(key)
			isKey := false
			if contains(unionKeys, key) {
				isKey = true
				primaryKeyParts = append(primaryKeyParts, fmt.Sprintf("`%s`", key))
			}

			columnType := getColumnType(value, isKey)
			column := fmt.Sprintf("`%s` %s", key, columnType)
			columns = append(columns, column)
		}
	}

	// 添加 create_at 和 update_at 字段
	columns = append(columns, "`create_at` DATETIME DEFAULT CURRENT_TIMESTAMP")
	columns = append(columns, "`update_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

	primaryKeyStatement := ""
	if len(primaryKeyParts) > 0 {
		primaryKeyStatement = fmt.Sprintf(", PRIMARY KEY (%s)", strings.Join(primaryKeyParts, ", "))
	}

	createTableStatement := fmt.Sprintf("CREATE TABLE IF NOT EXISTS `%s` (%s%s);", tableName, strings.Join(columns, ", "), primaryKeyStatement)
	return createTableStatement
}

// Helper function to check if a slice contains a particular element
func contains(slice []string, element string) bool {
	for _, v := range slice {
		if v == element {
			return true
		}
	}

	return false
}

// util
func fixDbName(str string) string {
	str = strings.ReplaceAll(str, " ", "_")
	str = strings.ReplaceAll(str, "-", "_")
	str = strings.ReplaceAll(str, "/", "_")
	return str
}

// getColumnType 根据 Go 数据类型返回相应的 SQL 数据类型
func getColumnType1(value any) string {
	switch value.(type) {
	case int, int8, int16, int32, int64:
		return "BIGINT"
	case uint, uint8, uint16, uint32, uint64:
		return "BIGINT UNSIGNED"
	case float32, float64:
		return "DOUBLE"
	case bool:
		return "BOOLEAN"
	case string:
		return "VARCHAR(255)"
	default:
		return "VARCHAR(255)"
	}
}

// generateCreateTableSQL 生成建表语句
func GenerateCreateTableSQL(msg *InsertMessage) string {
	var columns []string

	for key, value := range msg.Data {
		columnType := getColumnType1(value)
		column := fmt.Sprintf("`%s` %s", key, columnType)
		columns = append(columns, column)
	}

	// 添加 create_at 和 update_at 字段
	columns = append(columns, "`create_at` DATETIME DEFAULT CURRENT_TIMESTAMP")
	columns = append(columns, "`update_at` DATETIME DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP")

	// 生成建表语句
	createTableSQL := fmt.Sprintf(
		"CREATE TABLE IF NOT EXISTS `%s` (%s);",
		msg.TableName, strings.Join(columns, ", "))

	return createTableSQL
}
