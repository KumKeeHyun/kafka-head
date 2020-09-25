package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/KumKeeHyun/kafka-head/app/consumer"
)

func main() {
	kafkaClient := consumer.SaramaConsumer{}
	kafkaClient.Setup()

	err := kafkaClient.Start()
	if err != nil {
		panic(err)
	}

	sigterm := make(chan os.Signal, 1)
	signal.Notify(sigterm, syscall.SIGINT, syscall.SIGTERM)
	<-sigterm
	kafkaClient.Stop()
}
