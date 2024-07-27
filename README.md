# Kafka Message Subscription Program Documentation

## Table of Contents
- [Introduction](#introduction)
- [How to Run](#how-to-run)
- [Message Structure](#message-structure)
  - [InsertMessage](#insertmessage)
  - [WriteMessage](#writemessage)
- [Common Configuration](#common-configuration)
- [Program Workflow](#program-workflow)
- [Database Configuration](#database-configuration)
  - [MySQL](#mysql)
  - [InfluxDB](#influxdb)
- [Performance Notes](#performance-notes)

## Introduction
This program is written in Go and is designed to subscribe to messages from Kafka topics and parse them into two types: `InsertMessage` and `WriteMessage`. These messages are then written to MySQL and InfluxDB databases, respectively. For large batches of Kafka message queues, it effectively reduces the number of connections to the InfluxDB and MySQL databases, allowing for batch writing and queries, thereby significantly improving performance.

## How to Run
You can run this program using the following command line method:
```bash
go run main.go -mysql 127.0.0.1:3306@root/123456 -influx 127.0.0.1:8086 -kafka 127.0.0.1:9092
```
- -mysql: Specifies the MySQL database connection information in the format host
@username/password.
- -influx: Specifies the InfluxDB database connection information in the format host.
- -kafka: Specifies the Kafka server connection information in the format host.

This will start the program using the provided database and message broker configurations to subscribe to the appropriate Kafka topics and process the data.

## Message Structure
### InsertMessage
Message structure for insertion into MySQL database:

```go
type InsertMessage struct {
    DbName    string         `json:"db_name"`
    TableName string         `json:"table_name"`
    Data      map[string]any `json:"data"`
}
```

### WriteMessage
Message structure for writing to InfluxDB database:
```go
type WriteMessage struct {
    DbName      string            `json:"db_name"`
    Measurement string            `json:"measurement"`
    Tags        map[string]string `json:"tags"`
    Fields      map[string]any    `json:"fields"`
    Timestamp   string            `json:"timestamp"`
}
```

## Common Configuration
Example of the program's configuration file:
```json
{
  "base": {
    "version": "2.0.0",
    "mysql_pool_size": 10,
    "mysql_max_buffer_size": 100,
    "mysql_max_interval_time": 30,
    "mysql_pool_channel_size": 100,

    "influx_pool_size": 100,
    "influx_max_buffer_size": 5000,
    "influx_max_interval_time": 30,
    "influx_pool_channel_size": 100
  },
  "topics": [
    {
      "name": "MySQL",
      "group_id": "mysql_group_0",
      "storage_type": "mysql",
      "consume_num": 1
    },{
      "name": "InfluxDB",
      "group_id": "InfluxDB_group_0",
      "storage_type": "influxdb",
      "consume_num": 1
    }
  ]
}
```

### Configuration Parameters Description
- base.version: Program version number. 
- mysql_pool_size: MySQL connection pool size. 
- mysql_max_buffer_size: Maximum buffer size for MySQL. 
- mysql_max_interval_time: Maximum interval time for MySQL (seconds). 
- mysql_pool_channel_size: Channel size for the MySQL connection pool. 
- influx_pool_size: InfluxDB connection pool size. 
- influx_max_buffer_size: Maximum buffer size for InfluxDB, enough for about 100 switches. 
- influx_max_interval_time: Maximum interval time for InfluxDB (seconds). 
- influx_pool_channel_size: Channel size for the InfluxDB connection pool.

### Program Workflow
1. Start the program and read the configuration file.
2. Subscribe to the Kafka topics specified in the topics field of the configuration file.
3. Depending on the message type, write InsertMessage to the MySQL database and WriteMessage to the InfluxDB database.
4. Use connection pools and buffering mechanisms to optimize database writing performance.

### Database Configuration
#### MySQL
The program subscribes to MySQL topics and writes parsed InsertMessage messages to the specified MySQL database tables. Example configuration:
```javascript
{
  "name": "MySQL",
  "group_id": "mysql_group_0",
  "storage_type": "mysql",
  "consume_num": 1 // MySQL consumer number, can be increased for improved efficiency if the number of node_agents increases
}
```
#### InfluxDB
The program subscribes to InfluxDB topics and writes parsed WriteMessage messages to the specified InfluxDB database. Example configuration:
```javascript
{
  "name": "InfluxDB",
  "group_id": "InfluxDB_group_0",
  "storage_type": "influxdb",
  "consume_num": 1 // InfluxDB consumer number, can be increased for improved efficiency if the number of node_agents increases
}
```

## Performance Notes
The program performs excellently when processing Kafka messages, taking about 500 milliseconds to process 1000 messages. This indicates that the program can operate efficiently under high concurrency and large data volumes.

## Special Features

### Feature Introduction

To accommodate special data formats in Kafka messages, the system can be extended by implementing new interfaces. This method allows the program to handle various types of data formats and structures flexibly, ensuring that messages from different sources or with unique requirements can be integrated seamlessly.

You can refer to the message topic 'server_resource'.