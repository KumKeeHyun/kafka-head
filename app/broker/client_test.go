package broker_test

import (
	"testing"
	"time"

	"github.com/KumKeeHyun/kafka-head/app/broker"
)

func TestFetchBrokerInfo(t *testing.T) {
	brokerClient := broker.SaramaBroker{}
	brokerClient.Setup()

	err := brokerClient.Start()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(13 * time.Second)
	brokerClient.Stop()
}
