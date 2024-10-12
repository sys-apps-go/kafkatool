package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

func (k *KafkaClient) ProduceToTopicPartition(topic string, partition int, messageCount int, keyInput, messageInput string,
	messageSize int, maxBufferSize int, compressionType string) error {

	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true

	// Create a new client
	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		return fmt.Errorf("Error creating client: %v", err)
	}
	defer client.Close()

	// Configure producer
	config.Producer.MaxMessageBytes = maxBufferSize
	config.Producer.Compression = getCompressionType(compressionType)
	config.Producer.Partitioner = sarama.NewManualPartitioner

	producer, err := sarama.NewSyncProducer(k.Meta.BrokerAddress, config)
	if err != nil {
		return fmt.Errorf("Failed to create producer: %v", err)
	}
	defer producer.Close()

	message := messageInput
	newValue := message
	if message != "" {
		if compressionType != "none" {
			newValue1, _ := compress([]byte(newValue), compressionType)
			newValue = string(newValue1)
		}
	}
	message = newValue
	// Generate and send messages
	for i := 0; i < messageCount; i++ {
		key := keyInput
		if message == "" {
			message = generateMessage(messageSize)
			newValue := message
			if message != "" {
				if compressionType != "none" {
					newValue1, _ := compress([]byte(newValue), compressionType)
					newValue = string(newValue1)
				}
			}
			message = newValue
		}

		msg := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(partition),
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(message),
			Timestamp: time.Now(),
		}

		partitionWrote, offset, err := producer.SendMessage(msg)
		if err != nil {
			fmt.Println("Failed to send message: %v\n", err)
		} else {
			if k.Verbose {
				fmt.Printf("Message produced to topic '%s', partition %d at offset %d\n", topic, partitionWrote, offset)
			}
		}
	}
	return nil
}

func (k *KafkaClient) StreamDataFromFileToKafka(topic string, partitions int, keyInput string, maxBufferSize int, compressionType string) {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0
	config.Producer.Return.Successes = true

	// Create a new client
	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		fmt.Printf("Error creating client: %v\n", err)
		return
	}
	defer client.Close()

	// Configure producer
	config.Producer.Return.Successes = true
	config.Producer.MaxMessageBytes = maxBufferSize
	config.Producer.Partitioner = sarama.NewRoundRobinPartitioner
	config.Producer.Compression = getCompressionType(compressionType)

	producer, err := sarama.NewSyncProducer(k.Meta.BrokerAddress, config)
	if err != nil {
		fmt.Printf("Failed to create producer: %v\n", err)
		return
	}
	defer producer.Close()

	// Get message from channel and send messages
	partition := 0
	count := 0
	for message := range k.MessageChannel {
		key := fmt.Sprintf("%s%d", keyInput, count)
		count++

		msgBlock := &sarama.ProducerMessage{
			Topic:     topic,
			Partition: int32(partition),
			Key:       sarama.StringEncoder(key),
			Value:     sarama.StringEncoder(message),
			Timestamp: time.Now(),
		}

		partitionWrote, offset, err := producer.SendMessage(msgBlock)
		if err != nil {
			fmt.Println("Failed to send message: %v\n", err)
		} else {
			if k.Verbose {
				fmt.Printf("Message produced to topic '%s', partition %d at offset %d\n", topic, partitionWrote, offset)
			}
		}
		partition = (partition + 1) % partitions
	}
}

// Generate a message with a base string and a random string of a specified size
func generateMessage(size int) string {
	randomStr, _ := generateRandomString(size)
	return randomStr
}

func getCompressionType(compressionType string) sarama.CompressionCodec {
	switch strings.ToLower(compressionType) {
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	case "lz4":
		return sarama.CompressionLZ4
	case "zstd":
		return sarama.CompressionZSTD
	default:
		return sarama.CompressionNone
	}
}
