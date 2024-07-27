package main

import (
	"venu-data/consumer"
)

func main() {
	consumer.Start()
	select {}
}
