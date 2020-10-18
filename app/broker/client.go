package broker

import (
	"context"
	"fmt"
	"log"
	"time"

	"github.com/KumKeeHyun/kafka-head/app/helper"
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type Partition struct {
	PartitionID int32
	Leader      int32
	Replicas    []int32
	Offset      int64
}

type TopicPartitions map[string][]Partition

func (tp TopicPartitions) String() string {
	res := ""
	for topic, partitions := range tp {
		res += fmt.Sprintf("topic : %s(%d)\n", topic, len(partitions))
		for _, partition := range partitions {
			res += fmt.Sprintf("	id:%d, leader:%d, replicas:%v\n", partition.PartitionID, partition.Leader, partition.Replicas)
		}
	}
	return res
}

type SaramaBroker struct {
	kafkaBrokers    []string
	topicPartitions TopicPartitions
	fetchTicker     *time.Ticker
	saramaClient    sarama.Client
	saramaConfig    *sarama.Config
	brokerCtx       context.Context
	stopBroker      context.CancelFunc
}

func (kafka *SaramaBroker) Setup() {
	kafka.kafkaBrokers = helper.SplitStringSlice(viper.GetStringSlice("brokers"))
	kafka.saramaConfig = sarama.NewConfig()
	kafka.saramaConfig.Version = helper.KafkaVersions[viper.GetString("version")]

	kafka.topicPartitions = make(map[string][]Partition)
	kafka.brokerCtx, kafka.stopBroker = context.WithCancel(context.Background())
	kafka.fetchTicker = time.NewTicker(5 * time.Second)
}

func (kafka *SaramaBroker) Stop() {
	kafka.stopBroker()
	kafka.fetchTicker.Stop()
	log.Printf("broker stoped")
}

func (kafka *SaramaBroker) Start() error {
	client, err := sarama.NewClient(kafka.kafkaBrokers, kafka.saramaConfig)
	if err != nil {
		// log
		return err
	}
	kafka.saramaClient = client

	go kafka.startFetchBrokerInfo()

	return nil
}

func (kafka *SaramaBroker) startFetchBrokerInfo() {
	for {
		select {
		case <-kafka.fetchTicker.C:
			kafka.fetchBrokerInfo()
		case <-kafka.brokerCtx.Done():
			return
		}
	}
}

func (kafka *SaramaBroker) fetchBrokerInfo() {
	kafka.saramaClient.RefreshMetadata()

	topics, err := kafka.saramaClient.Topics()
	if err != nil {
		log.Println("failed to fetch topics")
		return
	}

	topicPartitions := make(map[string][]Partition)
	for _, topic := range topics {
		partitions, err := kafka.saramaClient.Partitions(topic)
		if err != nil {
			log.Printf("failed to fetch %s's partitions", topic)
			continue
		}

		topicPartitions[topic] = make([]Partition, 0, len(partitions))
		for _, partitionID := range partitions {
			partition := Partition{PartitionID: partitionID}
			broker, err := kafka.saramaClient.Leader(topic, partitionID)
			if err != nil {
				log.Printf("failed to fetch %s:%d's leader", topic, partitionID)
			} else {
				partition.Leader = broker.ID()
				if replicas, err := kafka.saramaClient.Replicas(topic, partitionID); err == nil {
					partition.Replicas = replicas
				}
			}
			topicPartitions[topic] = append(topicPartitions[topic], partition)
		}
	}

	kafka.topicPartitions = topicPartitions
	fmt.Println(kafka.topicPartitions)
}
