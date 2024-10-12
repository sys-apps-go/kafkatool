package internal

import (
	"fmt"
	"os"
	"sync"
	"strings"

	"github.com/IBM/sarama"
)

func (k *KafkaClient) FetchFromPartition(topic string, partition int32, compressionType string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(k.Meta.BrokerAddress, config)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return err
	}
	defer consumer.Close()

	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("Error creating partition consumer: %v\n", err)
		return err
	}
	defer partitionConsumer.Close()

	fmt.Printf("Fetching messages from topic '%s', partition %d:\n", topic, partition)
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			msgStr := string(msg.Value)
			if compressionType != "none" {
				msgStrBytes, _ := decompress(msg.Value, compressionType)
				msgStr = string(msgStrBytes)
			}
			msgStr = strings.ReplaceAll(msgStr, "\n", "")
			fmt.Printf("Offset: %d, Value: %s\n", msg.Offset, msgStr)
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error: %v\n", err)
		}
	}
	return nil
}

func (k *KafkaClient) FetchFromAllPartitions(topic string, compressionType string) error {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer(k.Meta.BrokerAddress, config)
	if err != nil {
		fmt.Printf("Error creating consumer: %v\n", err)
		return err
	}
	defer consumer.Close()

	partitions, err := consumer.Partitions(topic)
	if err != nil {
		fmt.Printf("Error creating partition consumer: %v\n", err)
		return err
	}

	var wg sync.WaitGroup
	for _, partition := range partitions {
		wg.Add(1)
		go func(p int32) {
			defer wg.Done()
			fetchAndWritePartition(consumer, topic, p, compressionType)
		}(partition)
	}

	wg.Wait()
	return nil
}

func fetchAndWritePartition(consumer sarama.Consumer, topic string, partition int32, compressionType string) {
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, sarama.OffsetOldest)
	if err != nil {
		fmt.Printf("Error creating partition consumer for partition %d: %v\n", partition, err)
		return
	}
	defer partitionConsumer.Close()

	fileName := fmt.Sprintf("%v-%v-partition-%d.txt", kafkaOutFile, topic, partition)
	file, err := os.Create(fileName)
	if err != nil {
		fmt.Printf("Error creating file for partition %d: %v\n", partition, err)
		return
	}
	defer file.Close()

	fmt.Printf("Writing to file %v\n", fileName)

	for {
		select {
		case msg := <-partitionConsumer.Messages():
			msgStr := string(msg.Value)
			if compressionType != "none" {
				msgStrBytes, _ := decompress(msg.Value, compressionType)
				msgStr = string(msgStrBytes)
			}
			msgStr = strings.ReplaceAll(msgStr, "\n", "")
			_, err := file.WriteString(fmt.Sprintf("Offset: %d, Value: %s\n", msg.Offset, msgStr))
			if err != nil {
				fmt.Printf("Error writing to file for partition %d: %v", partition, err)
			}
		case err := <-partitionConsumer.Errors():
			fmt.Printf("Error from partition %d: %v\n", partition, err)
		}
	}
}

