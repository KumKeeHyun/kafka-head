package consumer

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"log"
	"sync"

	"github.com/KumKeeHyun/kafka-head/app/helper"
	"github.com/spf13/viper"

	"github.com/Shopify/sarama"
)

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
	kafka.saramaConfig.Version = helper.KafkaVersions[viper.GetString("version")]
	kafka.saramaConfig.Consumer.Offsets.Initial = sarama.OffsetOldest

	kafka.consumersCtx, kafka.stopConsumers = context.WithCancel(context.Background())
}

func (kafka *SaramaConsumer) Stop() {
	kafka.stopConsumers()
	kafka.consumersGrp.Wait()
	log.Printf("all partitionConsumer stoped")
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
		go kafka.startPartitionConsumer(partitionConsumer, (*SaramaConsumer).printOffsetCommitAndGroupMetadata)
	}

	return nil
}

func (kafka *SaramaConsumer) startPartitionConsumer(consumer sarama.PartitionConsumer, handle func(*SaramaConsumer, *sarama.ConsumerMessage)) {
	defer kafka.consumersGrp.Done()
	defer consumer.AsyncClose()

	log.Println("start prtition consumer")
	for {
		select {
		case msg := <-consumer.Messages():
			handle(kafka, msg)
		case <-consumer.Errors():
			// log
			return
		case <-kafka.consumersCtx.Done():
			log.Printf("partitionConsumer stoped")
			return
		}
	}
}

func (kafka *SaramaConsumer) printByBytes(msg *sarama.ConsumerMessage) {
	fmt.Println("\nkey")
	fmt.Println(msg.Key)
	fmt.Println("value")
	fmt.Println(msg.Value)
	fmt.Println()
}

func (kafka *SaramaConsumer) printOffsetCommit(msg *sarama.ConsumerMessage) {
	var keyVersion, valueVersion int16

	fmt.Printf("partition(%d) offset(%d)\n", int(msg.Partition), msg.Offset)

	keyBuf := bytes.NewBuffer(msg.Key)
	if err := binary.Read(keyBuf, binary.BigEndian, &keyVersion); err != nil {
		// log
		return
	}

	switch keyVersion {
	case OffsetCommitVersion:
		offsetKey := OffsetCommitKey{}
		if _, err := offsetKey.Parse(keyBuf); err != nil {
			log.Println(err)
		}

		valueBuf := bytes.NewBuffer(msg.Value)
		if err := binary.Read(valueBuf, binary.BigEndian, &valueVersion); err != nil {
			return
		}
		offsetValue := OffsetCommitValue{}
		if _, err := offsetValue.Parse(valueBuf, valueVersion); err != nil {
			return
		}

		fmt.Printf("key(%d)   : %v\n", keyVersion, offsetKey)
		fmt.Printf("value(%d) : %v\n\n", valueVersion, offsetValue)
		return
	default:
		return
	}
}

func (kafka *SaramaConsumer) printOffsetCommitAndGroupMetadata(msg *sarama.ConsumerMessage) {
	var keyVersion, valueVersion int16

	fmt.Printf("partition(%d) offset(%d)\n", int(msg.Partition), msg.Offset)

	keyBuf := bytes.NewBuffer(msg.Key)
	if err := binary.Read(keyBuf, binary.BigEndian, &keyVersion); err != nil {
		// log
		return
	}

	switch keyVersion {
	case OffsetCommitVersion:
		offsetKey := OffsetCommitKey{}
		if _, err := offsetKey.Parse(keyBuf); err != nil {
			return
		}

		valueBuf := bytes.NewBuffer(msg.Value)
		if err := binary.Read(valueBuf, binary.BigEndian, &valueVersion); err != nil {
			return
		}
		offsetValue := OffsetCommitValue{}
		if _, err := offsetValue.Parse(valueBuf, valueVersion); err != nil {
			return
		}

		fmt.Printf("key(%d)   : %v\n", keyVersion, offsetKey)
		fmt.Printf("value(%d) : %v\n\n", valueVersion, offsetValue)
		return
	case GroupMetadataVersion:
		metadataKey := GroupMetadataKey{}
		if _, err := metadataKey.Parse(keyBuf); err != nil {
			return
		}

		valueBuf := bytes.NewBuffer(msg.Value)
		if err := binary.Read(valueBuf, binary.BigEndian, &valueVersion); err != nil {
			return
		}
		metadataHeader := GroupMetadataHeader{}
		if _, err := metadataHeader.Parse(valueBuf, valueVersion); err != nil {
			return
		}

		var numOfMember int32
		if err := binary.Read(valueBuf, binary.BigEndian, &numOfMember); err != nil {
			log.Println("numOfMember", err)
			return
		}
		if numOfMember <= 0 {
			fmt.Printf("key(%d)    : %v\n", keyVersion, metadataKey)
			fmt.Printf("header(%d) : %v\n", valueVersion, metadataHeader)
		}
		for i := 0; i < int(numOfMember); i++ {
			member := MemberMetadata{}
			if where, err := member.Parse(valueBuf, valueVersion); err != nil {
				log.Println(where, err)
				return
			} else {
				fmt.Printf("key(%d)    : %v\n", keyVersion, metadataKey)
				fmt.Printf("header(%d) : %v\n", valueVersion, metadataHeader)
				fmt.Printf("member     : %v\n\n", member)
			}
		}

	default:
		return
	}
}
