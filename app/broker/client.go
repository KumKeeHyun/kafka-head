package broker

import (
	"github.com/KumKeeHyun/kafka-head/app/helper"
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type saramaBroker struct {
	kafkaBrokers []string
	saramaClient sarama.Client
	saramaConfig *sarama.Config
}

func (kafka *saramaBroker) Setup() {
	kafka.kafkaBrokers = helper.SplitStringSlice(viper.GetStringSlice("brokers"))
	kafka.saramaConfig = sarama.NewConfig()
	kafka.saramaConfig.Version = helper.KafkaVersions[viper.GetString("version")]

}

func (kafka *saramaBroker) Stop() {

}

func (kafka *saramaBroker) Start() error {
	client, err := sarama.NewClient(kafka.kafkaBrokers, kafka.saramaConfig)
	if err != nil {
		// log
		return err
	}
	kafka.saramaClient = client

	return nil
}
