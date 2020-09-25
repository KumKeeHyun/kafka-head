package consumer

import (
	"context"
	"fmt"
	"log"
	"sync"

	"github.com/KumKeeHyun/kafka-head/app/helper"
	"github.com/spf13/viper"

	"github.com/Shopify/sarama"
)

var kafkaVersions = map[string]sarama.KafkaVersion{
	"":         sarama.V0_10_2_0,
	"0.8.0":    sarama.V0_8_2_0,
	"0.8.1":    sarama.V0_8_2_1,
	"0.8.2":    sarama.V0_8_2_2,
	"0.8":      sarama.V0_8_2_0,
	"0.9.0.0":  sarama.V0_9_0_0,
	"0.9.0.1":  sarama.V0_9_0_1,
	"0.9.0":    sarama.V0_9_0_0,
	"0.9":      sarama.V0_9_0_0,
	"0.10.0.0": sarama.V0_10_0_0,
	"0.10.0.1": sarama.V0_10_0_1,
	"0.10.0":   sarama.V0_10_0_0,
	"0.10.1.0": sarama.V0_10_1_0,
	"0.10.1":   sarama.V0_10_1_0,
	"0.10.2.0": sarama.V0_10_2_0,
	"0.10.2.1": sarama.V0_10_2_0,
	"0.10.2":   sarama.V0_10_2_0,
	"0.10":     sarama.V0_10_0_0,
	"0.11.0.1": sarama.V0_11_0_0,
	"0.11.0.2": sarama.V0_11_0_0,
	"0.11.0":   sarama.V0_11_0_0,
	"1.0.0":    sarama.V1_0_0_0,
	"1.1.0":    sarama.V1_1_0_0,
	"1.1.1":    sarama.V1_1_0_0,
	"2.0.0":    sarama.V2_0_0_0,
	"2.0.1":    sarama.V2_0_0_0,
	"2.1.0":    sarama.V2_1_0_0,
	"2.2.0":    sarama.V2_2_0_0,
	"2.2.1":    sarama.V2_2_0_0,
	"2.3.0":    sarama.V2_3_0_0,
	"2.4.0":    sarama.V2_4_0_0,
	"2.5.0":    sarama.V2_5_0_0,
	"2.6.0":    sarama.V2_6_0_0,
}

type SaramaConsumer struct {
	KafkaBrokers  []string
	KafkaTopic    string
	saramaClient  sarama.Client
	saramaConfig  *sarama.Config
	consumersGrp  sync.WaitGroup
	consumersCtx  context.Context
	stopConsumers context.CancelFunc
}

func (kafka *SaramaConsumer) Setup() {
	kafka.KafkaBrokers = helper.SplitStringSlice(viper.GetStringSlice("brokers"))
	kafka.KafkaTopic = viper.GetString("topic")

	kafka.saramaConfig = sarama.NewConfig()
	kafka.saramaConfig.Version = kafkaVersions[viper.GetString("version")]
	kafka.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	kafka.consumersCtx, kafka.stopConsumers = context.WithCancel(context.Background())
}

func (kafka *SaramaConsumer) Start() error {
	log.Println("start kafka clinet")
	client, err := sarama.NewClient(kafka.KafkaBrokers, kafka.saramaConfig)
	if err != nil {
		// log
		return err
	}
	kafka.saramaClient = client

	consumer, err := sarama.NewConsumerFromClient(kafka.saramaClient)
	if err != nil {
		// log
		return err
	}

	partitions, err := kafka.saramaClient.Partitions(kafka.KafkaTopic)
	if err != nil {
		// log
		return err
	}
	offset := kafka.saramaConfig.Consumer.Offsets.Initial
	log.Printf("partitons : %d\n", len(partitions))

	for _, partition := range partitions {
		partitionConsumer, err := consumer.ConsumePartition(kafka.KafkaTopic, partition, offset)
		if err != nil {
			// log
			kafka.saramaClient.Close()
			return err
		}

		kafka.consumersGrp.Add(1)
		go kafka.startPartitionConsumer(partitionConsumer, func(msg *sarama.ConsumerMessage) error {
			fmt.Printf("partition(%d) offset(%d) >  %s\n", msg.Partition, msg.Offset, string(msg.Value))
			return nil
		})
	}

	return nil
}

func (kafka *SaramaConsumer) startPartitionConsumer(consumer sarama.PartitionConsumer, handle func(*sarama.ConsumerMessage) error) {
	defer kafka.consumersGrp.Done()
	defer consumer.AsyncClose()

	log.Println("start prtition consumer")
	for {
		select {
		case msg := <-consumer.Messages():
			if err := handle(msg); err != nil {
				return
			}
		case <-consumer.Errors():
			// log
			return
		case <-kafka.consumersCtx.Done():
			log.Printf("partitionConsumer stoped")
			return
		}
	}
}

func (kafka *SaramaConsumer) Stop() {
	kafka.stopConsumers()
	kafka.consumersGrp.Wait()
	log.Printf("all partitionConsumer stoped")
}
