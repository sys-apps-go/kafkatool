package internal

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	mqtt "github.com/eclipse/paho.mqtt.golang"
	"github.com/golang/snappy"
	"github.com/klauspost/compress/zstd"
	"github.com/pierrec/lz4"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"strings"
	"time"
	"os/user"
)

func (k *KafkaClient) StreamFileToKafka(path, topic, key, compressionType string, partitions int, apiOption bool) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer file.Close() // Ensure the file is closed when we're done

	// Create a buffered reader
	reader := bufio.NewReader(file)

	partition := 0
	for {
		// Read a line from the file
		line, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				time.Sleep(300 * time.Millisecond)
				continue
			} else {
				fmt.Printf("Produce batch exited with error: %v\n", err)
				return err
			}
		}
		line = strings.TrimSuffix(line, "\n")
		if apiOption {
			k.MessageChannel <- line
		} else {
			k.Produce(topic, partition, key, line, compressionType)
		}
		partition = (partition + 1) % partitions
	}
	return nil
}

func GenerateRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	str := hex.EncodeToString(bytes)
	if len(str) >= length {
		return str[:length], nil
	} 
	return "", fmt.Errorf("Random string generation failed: %v\n", str)
}

// PublishMessagesToPulsar fetches messages from Kafka and publishes them to Pulsar
func (k *KafkaClient) PublishMessagesToPulsar(pulsarURL, pulsarTopic string) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarURL,
	})
	if err != nil {
		return fmt.Errorf("error creating Pulsar client: %v", err)
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: pulsarTopic,
	})
	if err != nil {
		return fmt.Errorf("error creating Pulsar producer: %v", err)
	}
	defer producer.Close()

	ctx := context.Background()

	for record := range k.PulsarChannel {
		if msgID, err := producer.Send(ctx, &pulsar.ProducerMessage{
			Payload: []byte(record),
		}); err != nil {
			return fmt.Errorf("error sending message to Pulsar: %v", err)
		} else {
			if k.Verbose {
				fmt.Printf("Published message to Pulsar: %v\n", msgID)
			}
		}
	}

	return nil
}

// PublishMessagesFromPulsar fetches messages from Pulsar and publishes them to Kafka
func (k *KafkaClient) PublishMessagesFromPulsar(pulsarURL, pulsarTopic, kafkaTopic string, partitionIndex int, key string) error {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL: pulsarURL,
	})
	if err != nil {
		return err
	}

	defer client.Close()

	channel := make(chan pulsar.ConsumerMessage, 1024)

	options := pulsar.ConsumerOptions{
		Topic:            pulsarTopic,
		SubscriptionName: "my-subscription",
		Type:             pulsar.Shared,
	}

	options.MessageChannel = channel

	consumer, err := client.Subscribe(options)
	if err != nil {
		return err
	}

	defer consumer.Close()

	for cm := range channel {
		msg := cm.Message
		if k.Verbose {
			fmt.Printf("%v", string(msg.Payload()))
		}
		consumer.Ack(msg)
		k.ProduceFromPulsar(kafkaTopic, partitionIndex, key, string(msg.Payload()))
	}
	return nil
}

var topicMap map[string]bool

func (k *KafkaClient) MQTTTopicsToKafka(topicFilter string, mqttBroker string) error {
	broker := mqttBroker
	topicMap = make(map[string]bool)

	opts := mqtt.NewClientOptions()
	opts.AddBroker(broker)
	opts.SetProtocolVersion(uint(4)) // Set the protocol version to 4 (3.1.1)
	opts.SetClientID("mqttSubClientPublishToKafka")
	// Create the MQTT client
	client := mqtt.NewClient(opts)
	if token := client.Connect(); token.Wait() && token.Error() != nil {
		err := fmt.Errorf("Error connecting to MQTT broker:", token.Error())
		return err
	}
	defer client.Disconnect(250)

	kafkaChannel := make(chan msgData, 1024)
	for i := 0; i < 32; i++ {
		go k.writeToKafka(kafkaChannel)
	}

	token := client.Subscribe(topicFilter, 1, func(client mqtt.Client, msg mqtt.Message) {
		topic1 := strings.Replace(msg.Topic(), "/", "_", -1)
		_, exists := topicMap[topic1]
		if !exists {
			k.CreateTopic(topic1, 1, 1)
			topicMap[topic1] = true
		}
		message := msg.Payload()
		m := msgData{}
		m.msg = string(message)
		m.topic = topic1
		kafkaChannel <- m
	})
	if token.Wait() && token.Error() != nil {
		err := fmt.Errorf("Error subscribing to topic:", token.Error())
		return err
	}
	select {}
	return nil
}

func (k *KafkaClient) writeToKafka(kchan chan msgData) {
	for m := range kchan {
		k.ProduceFromMQTT(m.topic, 0, "key", m.msg)
	}
}

// decompresses msg based on the compression type
func decompress(msg []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "gzip":
		return decompressGzip(msg)
	case "snappy":
		return decompressSnappy(msg)
	case "lz4":
		return decompressLZ4(msg)
	case "zstd":
		return decompressZstd(msg)
	case "none":
		return msg, nil // No decompression needed
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// decompressGzip decompresses gzip-compressed data
func decompressGzip(data []byte) ([]byte, error) {
	reader, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, err
	}
	defer reader.Close()

	return ioutil.ReadAll(reader)
}

// decompressSnappy decompresses snappy-compressed data
func decompressSnappy(data []byte) ([]byte, error) {
	return snappy.Decode(nil, data)
}

// decompressLZ4 decompresses lz4-compressed data
func decompressLZ4(data []byte) ([]byte, error) {
	reader := lz4.NewReader(bytes.NewReader(data))
	return ioutil.ReadAll(reader)
}

// decompressZstd decompresses zstd-compressed data
func decompressZstd(data []byte) ([]byte, error) {
	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, err
	}
	defer decoder.Close()

	return decoder.DecodeAll(data, nil)
}

func compress(msg []byte, compressionType string) ([]byte, error) {
	switch compressionType {
	case "gzip":
		return compressGzip(msg)
	case "snappy":
		return compressSnappy(msg)
	case "lz4":
		return compressLZ4(msg)
	case "zstd":
		return compressZstd(msg)
	case "none":
		return msg, nil // No compression needed
	default:
		return nil, fmt.Errorf("unsupported compression type: %s", compressionType)
	}
}

// Gzip compression
func compressGzip(msg []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := gzip.NewWriter(&buf)
	if _, err := writer.Write(msg); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil { // Close to flush data to buffer
		return nil, err
	}

	return buf.Bytes(), nil
}

// Snappy compression
func compressSnappy(msg []byte) ([]byte, error) {
	return snappy.Encode(nil, msg), nil
}

// LZ4 compression
func compressLZ4(msg []byte) ([]byte, error) {
	var buf bytes.Buffer
	writer := lz4.NewWriter(&buf)
	if _, err := writer.Write(msg); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil { // Close to flush data to buffer
		return nil, err
	}

	return buf.Bytes(), nil
}

// Zstd compression
func compressZstd(msg []byte) ([]byte, error) {
	encoder, err := zstd.NewWriter(nil)
	if err != nil {
		return nil, err
	}
	defer encoder.Close()

	return encoder.EncodeAll(msg, nil), nil
}

func generateRandomString(length int) (string, error) {
	bytes := make([]byte, length)
	if _, err := rand.Read(bytes); err != nil {
		return "", err
	}
	return hex.EncodeToString(bytes)[:length], nil
}

func murmur2(data []byte) int32 {
	length := len(data)
	seed := uint32(0x9747b28c)
	m := uint32(0x5bd1e995)
	r := uint32(24)

	h := uint32(seed ^ uint32(length))
	length4 := length / 4

	for i := 0; i < length4; i++ {
		i4 := i * 4
		k := uint32(data[i4+0]&0xff) + (uint32(data[i4+1]&0xff) << 8) + (uint32(data[i4+2]&0xff) << 16) + (uint32(data[i4+3]&0xff) << 24)
		k *= m
		k ^= k >> r
		k *= m
		h *= m
		h ^= k
	}

	switch length % 4 {
	case 3:
		h ^= uint32(data[length-3]&0xff) << 16
		fallthrough
	case 2:
		h ^= uint32(data[length-2]&0xff) << 8
		fallthrough
	case 1:
		h ^= uint32(data[length-1] & 0xff)
		h *= m
	}

	h ^= h >> 13
	h *= m
	h ^= h >> 15

	return int32(h)
}

func getPartition(key string, numPartitions int32) int32 {
	if key == "" {
		return 0
	}
	hash := murmur2([]byte(key))
	return abs(hash) % numPartitions
}

func abs(n int32) int32 {
	if n < 0 {
		return -n
	}
	return n
}

// isRoot checks if the current user has root privileges
func isRoot() (bool, error) {
	currentUser, err := user.Current()
	if err != nil {
		return false, fmt.Errorf("error getting current user: %w", err)
	}

	if currentUser.Uid != "0" {
		return false, nil
	}

	return true, nil
}

func NewTrie() *Trie {
	return &Trie{
		root: &TrieNode{
			children: make(map[rune]*TrieNode),
		},
	}
}

func (t *Trie) Insert(command string) {
	node := t.root
	for _, char := range command {
		if _, exists := node.children[char]; !exists {
			node.children[char] = &TrieNode{
				children: make(map[rune]*TrieNode),
			}
		}
		node = node.children[char]
	}
	node.isEnd = true
	node.command = command
}

func (t *Trie) SearchPrefix(prefix string) []string {
	node := t.root
	for _, char := range prefix {
		if _, exists := node.children[char]; !exists {
			return []string{}
		}
		node = node.children[char]
	}
	return t.collectCommands(node, prefix)
}

func (t *Trie) collectCommands(node *TrieNode, prefix string) []string {
	var commands []string
	if node.isEnd {
		commands = append(commands, node.command)
	}
	for char, child := range node.children {
		commands = append(commands, t.collectCommands(child, prefix+string(char))...)
	}
	return commands
}
