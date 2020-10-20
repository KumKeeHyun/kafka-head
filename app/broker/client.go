package broker

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/KumKeeHyun/kafka-head/app/helper"
	"github.com/Shopify/sarama"
	"github.com/spf13/viper"
)

type Partition struct {
	PartitionID int32
	Leader      int32
	Replicas    []int32
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

func (tp *TopicPartitions) getOffsetRequest(client sarama.Client) (map[int32]*sarama.OffsetRequest, map[int32]*sarama.Broker) {
	requests := make(map[int32]*sarama.OffsetRequest)
	brokers := make(map[int32]*sarama.Broker)

	for topic, partitions := range *tp {
		for _, partition := range partitions {
			broker, err := client.Leader(topic, partition.PartitionID)
			if err != nil {
				continue
			}
			if _, ok := requests[broker.ID()]; !ok {
				requests[broker.ID()] = &sarama.OffsetRequest{}
			}
			brokers[broker.ID()] = broker
			requests[broker.ID()].AddBlock(topic, partition.PartitionID, sarama.OffsetNewest, 1)
		}
	}
	return requests, brokers
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

	go kafka.startFetchBroker()
	// kafka.startFetchBrokerTest()

	return nil
}

func (kafka *SaramaBroker) startFetchBrokerTest() {
	kafka.fetchBrokerInfo()
	// kafka.fetchOffset()
	kafka.requestOffset()

	kafka.Stop()
}

func (kafka *SaramaBroker) startFetchBroker() {
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

func (kafka *SaramaBroker) fetchOffset() {
	wg := sync.WaitGroup{}

	fetchOffsetTopic := func(t string, ps []Partition) {
		defer wg.Done()

		for _, p := range ps {
			offset, err := kafka.saramaClient.GetOffset(t, p.PartitionID, sarama.OffsetNewest)
			if err != nil {
				log.Printf("failed to getoffset : %s(%d)\n", t, p.PartitionID)
				continue
			}
			log.Printf("topic : %s, partition : %d, offset : %d\n", t, p.PartitionID, offset)
		}
	}

	for topic, partitions := range kafka.topicPartitions {
		wg.Add(1)
		go fetchOffsetTopic(topic, partitions)
	}
	wg.Wait()
}

func (kafka *SaramaBroker) requestOffset() {
	requests, brokers := kafka.topicPartitions.getOffsetRequest(kafka.saramaClient)
	wg := sync.WaitGroup{}

	getBrokerOffsets := func(brokerID int32, request *sarama.OffsetRequest) {
		defer wg.Done()
		response, err := brokers[brokerID].GetAvailableOffsets(request)
		if err != nil {
			brokers[brokerID].Close()
			return
		}
		for topic, partitions := range response.Blocks {
			for partition, offsetResponse := range partitions {
				if offsetResponse.Err != sarama.ErrNoError {
					continue
				}
				log.Printf("topic : %s, partition : %d, offset : %d\n", topic, partition, offsetResponse.Offsets[0])
			}
		}
	}

	for brokerID, request := range requests {
		wg.Add(1)
		go getBrokerOffsets(brokerID, request)
	}

	wg.Wait()
}
