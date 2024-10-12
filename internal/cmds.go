package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"net"
	"os"
	"strings"
	"time"
)

func (k *KafkaClient) CreateTopic(topic string, partitions int, replicas int) error {
	_, ok := k.Meta.Topics[topic]
	if ok {
		err := fmt.Errorf("Topic %v already exists\n", topic)
		return err
	}
	if err := k.processCreateTopicRequest(topic, partitions, replicas); err != nil {
		errStr := fmt.Errorf("Error encoding: %v\n", err)
		return errStr
	}

	time.Sleep(time.Second)
	k.UpdateMetadata()
	return nil
}

func sendRequestToBroker(k *KafkaClient, brokerAddr string, data []byte, topic string, cmd string) (bool, error) {
	var err error
	var errorCode int16

	connBroker, err := net.DialTimeout("tcp", brokerAddr, 5*time.Second)
	if err != nil {
		fmt.Printf("failed to connect to Kafka broker: %v", err)
		return false, err
	}
	defer connBroker.Close()

	err = k.writeCommand(connBroker, data)
	if err != nil {
		return false, err
	}

	response, err := k.receiveResponse(connBroker)
	if err != nil {
		fmt.Println("Error Metadata request:", err)
		return false, err
	}

	switch cmd {
	case "createtopic":
		errorCode, err = k.decodeCreateTopicResponse(topic, response)
		if err != nil {
			fmt.Println("Error decoding:", err)
			return false, err
		}
	case "deletetopic":
		errorCode, err = k.decodeDeleteTopicResponse(topic, response)
		if err != nil {
			fmt.Println("Error decoding:", err)
			return false, err
		}
	case "deleterecord":
		errorCode, err = k.decodeDeleteRecordResponse(topic, response)
		if err != nil {
			fmt.Println("Error decoding:", err)
			return false, err
		}
	}

	if errorCode == 0 {
		return true, nil
	}

	return false, nil
}

func (k *KafkaClient) processCreateTopicRequest(topic string, partitions int, replicas int) error {
	correlationID := int32(1)

	buf := new(bytes.Buffer)

	// Calculate the remaining size
	clientIDLength := int16(len(k.ClientID))
	topicNameLength := int16(len(topic))

	apiKey := int16(CreateTopics) // CreateTopics API key
	apiVersion := int16(Version0)

	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(k.ClientID)

	// Number of topics (only one in this example)
	binary.Write(buf, binary.BigEndian, int32(1))

	// Topic Name
	binary.Write(buf, binary.BigEndian, topicNameLength)
	buf.WriteString(topic)

	// Number of Partitions
	binary.Write(buf, binary.BigEndian, int32(partitions))

	// Replication Factor
	binary.Write(buf, binary.BigEndian, int16(replicas))

	// Assignments (empty in this example)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Configs (empty in this example)
	binary.Write(buf, binary.BigEndian, int32(0))

	// Timeout
	binary.Write(buf, binary.BigEndian, int32(10000))

	//Write Remaining size at the start
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	for i := 0; i < len(k.Meta.Brokers); i++ {
		isCreated, err := sendRequestToBroker(k, k.Meta.Brokers[i].Address, buf.Bytes(), topic, "createtopic")
		if err != nil {
			return err
		} else {
			if isCreated {
				return nil
			}
		}
	}

	return nil
}

func (k *KafkaClient) decodeCreateTopicResponse(topicName string, response []byte) (int16, error) {
	var errorCode int16

	buf := bytes.NewReader(response)

	// Decode topics
	var numTopics int32
	err := binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(numTopics); i++ {
		// Decode topic name length
		var nameLength int16
		err = binary.Read(buf, binary.BigEndian, &nameLength)
		if err != nil {
			return 0, err
		}

		// Decode topic name
		nameBytes := make([]byte, nameLength)
		err = binary.Read(buf, binary.BigEndian, &nameBytes)
		if err != nil {
			return 0, err
		}
		decodedTopicName := string(nameBytes)

		// Decode error code
		err = binary.Read(buf, binary.BigEndian, &errorCode)
		if err != nil {
			return 0, err
		}
		if errorCode != 0 {
			return errorCode, nil
		}

		// Compare decoded topic name with provided topic name
		if decodedTopicName == topicName {
			return 0, nil
		}
	}

	return 0, fmt.Errorf("topic '%s' not found in response", topicName)
}

/*
Message format: Version 0: Header + Body + Message set
*/
func (k *KafkaClient) Produce(topic string, partition int, key, valueInput string, compressionType string) error {
	k.Meta.mu.Lock()
	t, exists := k.Meta.Topics[topic]
	if !exists {
		k.Meta.mu.Unlock()
		err := fmt.Errorf("Topic %v doesn't exist\n", topic)
		return err
	}
	k.Meta.mu.Unlock()
	if partition >= len(t.Partitions) || partition < 0 {
		err := fmt.Errorf("Topic Partition %v doesn't exist\n", partition)
		return err
	}

	correlationID := k.CorrelationID
	k.CorrelationID++
	clientID := k.ClientID
	clientIDLength := int32(len(clientID))
	topicNameLength := int32(len(topic))
	keyLength := int32(len(key))
	value := []byte(valueInput)
	if compressionType != "none" {
		value, _ = compress([]byte(value), compressionType)
	}
	valueLength := int32(len(value))
	apiKey := int16(Produce) // Produce API key
	apiVersion := int16(Version0)

	buf := k.BufferPool.Get().(*bytes.Buffer)

	defer func() {
		buf.Reset()
		k.BufferPool.Put(buf)
	}()

	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, int16(clientIDLength)) // Casting to int16
	buf.WriteString(clientID)

	// Required Acks
	binary.Write(buf, binary.BigEndian, int16(-1))

	// Timeout
	binary.Write(buf, binary.BigEndian, int32(10000))

	// Number of topics
	binary.Write(buf, binary.BigEndian, int32(1))

	// Topic Name
	binary.Write(buf, binary.BigEndian, int16(topicNameLength)) // Casting to int16
	buf.WriteString(topic)

	// Number of Partitions
	binary.Write(buf, binary.BigEndian, int32(1))

	// Partition
	binary.Write(buf, binary.BigEndian, int32(partition))

	messageSetSize := 0

	// Version: 0: {Total Message Size(4), Set {Offset(8), Current Message Size(4), Attribute size(2), CRC(4), Key Length(4), Key, Value Length(4), Value}}

	// Message Set Size
	messageSetSizeOffset := k.HeaderSize + k.BodySize + topicNameLength

	currMessageSetSize := OffsetSize + CurrentMessageSize + AttributeSize + CRCSize + KeySize + keyLength + ValueSize + valueLength
	messageSize := AttributeSize + CRCSize + KeySize + keyLength + ValueSize + valueLength

	binary.Write(buf, binary.BigEndian, int32(0))
	crcWriteOffset := messageSetSizeOffset + TotalMessageSize + OffsetSize + CurrentMessageSize

	// Offset
	binary.Write(buf, binary.BigEndian, int64(0))

	// Message Size
	binary.Write(buf, binary.BigEndian, int32(messageSize))

	// CRC
	binary.Write(buf, binary.BigEndian, int32(0))

	crcChecksumStart := crcWriteOffset + 4

	// Attribute
	binary.Write(buf, binary.BigEndian, int16(0))

	// Key
	binary.Write(buf, binary.BigEndian, keyLength)
	buf.Write([]byte(key))

	// Value
	binary.Write(buf, binary.BigEndian, valueLength)
	buf.Write([]byte(value))

	crcChecksumEnd := crcChecksumStart + AttributeSize + CRCSize + keyLength + 4 + valueLength
	crc := crc32.ChecksumIEEE(buf.Bytes()[crcChecksumStart:crcChecksumEnd]) // Calculate CRC from data start to CRC field

	// Append the CRC to the buffer
	binary.BigEndian.PutUint32(buf.Bytes()[crcWriteOffset:], crc)
	messageSetSize += int(currMessageSetSize)
	binary.BigEndian.PutUint32(buf.Bytes()[messageSetSizeOffset:], uint32(messageSetSize))
	remainingSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(remainingSize))

	k.Meta.mu.Lock()
	leaderAddress := k.Meta.Leader[topic][partition]
	_, exists = k.Meta.LeaderConnMap[leaderAddress]
	if !exists {
		conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
		if err != nil {
			k.Meta.mu.Unlock()
			fmt.Printf("failed to connect to Kafka broker: %v", err)
			return err
		}
		k.Meta.LeaderConnMap[leaderAddress] = conn
	}
	conn := k.Meta.LeaderConnMap[leaderAddress]

	err := k.writeCommand(conn, buf.Bytes())
	if err != nil {
		k.Meta.mu.Unlock()
		return err
	}

	response, err := k.receiveResponse(conn)
	if err != nil {
		k.Meta.mu.Unlock()
		return fmt.Errorf("failed to receive produce response: %v", err)
	}
	k.Meta.mu.Unlock()

	errorCode, err := k.decodeProduceResponse(topic, partition, response)
	if err != nil || errorCode != 0 {
		fmt.Println("Error Produce Response:", err, errorCode)
		return err
	}
	if k.Verbose {
		fmt.Printf("Message wrote on topic %v, partition %v\n", topic, partition)
	}
	return nil
}

func (k *KafkaClient) ProduceMessageSet(topic string, partition int, messageCount int, keyInput, messageInput string,
	messageSize int, maxBufferSize int, compressionType string) (int, error) {
	var key string
	var value []byte
	var randomKey, randomValue bool

	randomKeyLength := 8
	key = keyInput
	if keyInput == "" {
		key, _ = generateRandomString(randomKeyLength)
		randomKey = true
	}

	value = []byte(messageInput)
	if messageInput == "" {
		valueStr, _ := generateRandomString(messageSize)
		randomValue = true
		value = []byte(valueStr)
	}

	k.Meta.mu.Lock()
	t, exists := k.Meta.Topics[topic]
	if !exists {
		k.Meta.mu.Unlock()
		err := fmt.Errorf("Topic %v doesn't exist\n", topic)
		return -1, err
	}
	k.Meta.mu.Unlock()

	if partition >= len(t.Partitions) || partition < 0 {
		err := fmt.Errorf("Topic Partition %v doesn't exist\n", partition)
		return -1, err
	}

	correlationID := k.CorrelationID
	clientID := k.ClientID
	clientIDLength := int32(len(clientID))
	topicNameLength := int32(len(topic))
	keyLength := int32(len(key))
	newValue := value
	if compressionType != "none" {
		newValue, _ = compress([]byte(value), compressionType)
	}
	value = newValue
	valueLength := int32(len(value))
	apiKey := int16(Produce) // Produce API key
	apiVersion := int16(Version0)
	count := 0
	totalCount := 0

	fmt.Println("Produce in progress...")

	// Version: 0: {Total Message Size(4), Set {Offset(8), Current Message Size(4), Attribute size(2), CRC(4), Key Length(4), Key, Value Length(4), Value}}
	pktHdrSize := k.HeaderSize
	buf := k.BufferPool.Get().(*bytes.Buffer)

	defer func() {
		buf.Reset()
		k.BufferPool.Put(buf)
	}()

	for {
		buf.Reset()
		count = 0

		// Write the header
		binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
		binary.Write(buf, binary.BigEndian, apiKey)
		binary.Write(buf, binary.BigEndian, apiVersion)
		binary.Write(buf, binary.BigEndian, correlationID)

		// Client ID
		binary.Write(buf, binary.BigEndian, int16(clientIDLength)) // Casting to int16
		buf.WriteString(clientID)

		// Required Acks
		binary.Write(buf, binary.BigEndian, int16(-1))

		// Timeout
		binary.Write(buf, binary.BigEndian, int32(10000))

		// Number of topics
		binary.Write(buf, binary.BigEndian, int32(1))

		// Topic Name
		binary.Write(buf, binary.BigEndian, int16(topicNameLength)) // Casting to int16
		buf.WriteString(topic)

		// Number of Partitions
		binary.Write(buf, binary.BigEndian, int32(1))

		// Partition
		binary.Write(buf, binary.BigEndian, int32(partition))

		messageSetSize := 0
		messageSetSizeOffset := k.HeaderSize + k.BodySize + topicNameLength
		currMessageSetSize := OffsetSize + CurrentMessageSize + AttributeSize + CRCSize + KeySize + keyLength + ValueSize + valueLength
		currMessageSize := AttributeSize + CRCSize + KeySize + keyLength + ValueSize + valueLength
		crcWriteOffset := messageSetSizeOffset + TotalMessageSize + OffsetSize + CurrentMessageSize
		nextCRCSize := CurrentMessageSize + CRCSize + AttributeSize + KeySize + keyLength + ValueSize + valueLength + OffsetSize

		binary.Write(buf, binary.BigEndian, int32(0)) // Total Message set size, place holder

		for {
			if buf.Len() >= maxBufferSize || totalCount >= messageCount {
				break
			}

			// Offset
			binary.Write(buf, binary.BigEndian, int64(0))

			// Message Size
			binary.Write(buf, binary.BigEndian, int32(currMessageSize))

			// CRC
			binary.Write(buf, binary.BigEndian, int32(0))

			crcChecksumStart := crcWriteOffset + 4

			// Attribute
			binary.Write(buf, binary.BigEndian, int16(0))

			// Key
			binary.Write(buf, binary.BigEndian, keyLength)
			buf.Write([]byte(key))

			// Value
			binary.Write(buf, binary.BigEndian, valueLength)

			buf.Write(value)

			crcChecksumEnd := crcChecksumStart + AttributeSize + KeySize + keyLength + ValueSize + valueLength
			crc := crc32.ChecksumIEEE(buf.Bytes()[crcChecksumStart:crcChecksumEnd]) // Calculate CRC from data start to CRC field

			// Append the CRC to the buffer
			binary.BigEndian.PutUint32(buf.Bytes()[crcWriteOffset:], crc)
			count += 1
			totalCount += 1
			messageSetSize += int(currMessageSetSize)
			crcWriteOffset += nextCRCSize
			totalPktSize := int(int(pktHdrSize) + int(messageSetSize))
			if totalCount >= messageCount || totalPktSize > maxBufferSize {
				break
			}
			if randomKey {
				key, _ = generateRandomString(randomKeyLength)
			}
			if randomValue {
				valueStr, _ := generateRandomString(messageSize)
				value = []byte(valueStr)
			}
		}
		binary.BigEndian.PutUint32(buf.Bytes()[messageSetSizeOffset:], uint32(messageSetSize))
		remainingSize := int32(buf.Len() - 4)
		binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(remainingSize))

		k.Meta.mu.Lock()
		leaderAddress := k.Meta.Leader[topic][partition]

		_, exists = k.Meta.LeaderConnMap[leaderAddress]
		if !exists {
			conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
			if err != nil {
				k.Meta.mu.Unlock()
				fmt.Printf("failed to connect to Kafka broker: %v", err)
				return -1, err
			}
			k.Meta.LeaderConnMap[leaderAddress] = conn
		}
		conn := k.Meta.LeaderConnMap[leaderAddress]

		err := k.writeCommand(conn, buf.Bytes())
		if err != nil {
			k.Meta.mu.Unlock()
			return -1, err
		}

		response, err := k.receiveResponse(conn)
		if err != nil {
			k.Meta.mu.Unlock()
			fmt.Println(err)
			return -1, fmt.Errorf("failed to receive produce response: %v", err)
		}
		k.Meta.mu.Unlock()

		errorCode, err := k.decodeProduceResponse(topic, partition, response)
		if err != nil || errorCode != 0 {
			fmt.Println("Error Produce Response:", err, errorCode)
			return -1, err
		} else {
			if totalCount == messageCount {
				fmt.Println("Produce completed", totalCount, messageCount)
				break
			}
		}
		correlationID += 1
	}

	return count, nil
}

func (k *KafkaClient) decodeProduceResponse(topicName string, partition int, response []byte) (int16, error) {
	var errorCode int16

	buf := bytes.NewReader(response)

	// Decode topics
	var numTopics int32
	err := binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return 0, err
	}
	for i := 0; i < int(numTopics); i++ {
		// Decode topic name length
		var nameLength int16
		err = binary.Read(buf, binary.BigEndian, &nameLength)
		if err != nil {
			return 0, err
		}

		// Decode topic name
		nameBytes := make([]byte, nameLength)
		err = binary.Read(buf, binary.BigEndian, &nameBytes)
		if err != nil {
			return 0, err
		}
		decodedTopicName := string(nameBytes)

		var numPartition, partition int32
		err = binary.Read(buf, binary.BigEndian, &numPartition)
		if err != nil {
			return 0, err
		}

		err = binary.Read(buf, binary.BigEndian, &partition)
		if err != nil {
			return 0, err
		}

		// Decode error code
		err = binary.Read(buf, binary.BigEndian, &errorCode)
		if err != nil {
			return 0, err
		}
		if errorCode != 0 {
			return errorCode, fmt.Errorf("Produce command failed with error code: %v", errorCode)
		}

		// Compare decoded topic name with provided topic name
		if decodedTopicName == topicName {
			return 0, nil
		}
	}

	return 0, fmt.Errorf("topic '%s' not found in response", topicName)
}

// Fetch fetches messages from the given topic and partition
func (k *KafkaClient) Fetch(topic string, partition int, fetchOffset int64, maxBytes int32, file *os.File, compressionType string) (int, error) {
	k.Meta.mu.Lock()
	t, exists := k.Meta.Topics[topic]
	if !exists {
		k.Meta.mu.Unlock()
		err := fmt.Errorf("Topic %v doesn't exist\n", topic)
		return -1, err
	}
	k.Meta.mu.Unlock()

	if partition >= len(t.Partitions) {
		err := fmt.Errorf("Topic Partition %v doesn't exist\n", partition)
		return -1, err
	}

	// Construct the Fetch request
	reqBuf := new(bytes.Buffer)

	// Header
	apiKey := int16(Fetch) // Fetch API key
	apiVersion := int16(Version0)
	correlationID := int32(1)
	clientID := "client-1"
	clientIDLength := int16(len(clientID))

	// Write header
	binary.Write(reqBuf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(reqBuf, binary.BigEndian, apiKey)
	binary.Write(reqBuf, binary.BigEndian, apiVersion)
	binary.Write(reqBuf, binary.BigEndian, correlationID)
	binary.Write(reqBuf, binary.BigEndian, clientIDLength)
	reqBuf.WriteString(clientID)

	// ReplicaId, MaxWaitTime, MinBytes
	replicaID := int32(-1)
	maxWaitTime := int32(1000)
	minBytes := int32(1)

	// Write request details
	binary.Write(reqBuf, binary.BigEndian, replicaID)
	binary.Write(reqBuf, binary.BigEndian, maxWaitTime)
	binary.Write(reqBuf, binary.BigEndian, minBytes)

	// Number of topics
	binary.Write(reqBuf, binary.BigEndian, int32(1))

	// Topic name
	topicLength := int16(len(topic))
	binary.Write(reqBuf, binary.BigEndian, topicLength)
	reqBuf.WriteString(topic)

	// Number of partitions
	binary.Write(reqBuf, binary.BigEndian, int32(1))

	// Partition, FetchOffset, MaxBytes
	binary.Write(reqBuf, binary.BigEndian, int32(partition))
	binary.Write(reqBuf, binary.BigEndian, fetchOffset)
	binary.Write(reqBuf, binary.BigEndian, maxBytes)

	//Write Remaining size at the start
	requestSize := int32(reqBuf.Len() - 4)
	binary.BigEndian.PutUint32(reqBuf.Bytes()[0:], uint32(requestSize))

	// Connect to Kafka broker
	k.Meta.mu.Lock()
	leaderAddress := k.Meta.Leader[topic][partition]
	_, exists = k.Meta.LeaderConnMap[leaderAddress]
	if !exists {
		conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
		if err != nil {
			k.Meta.mu.Unlock()
			fmt.Printf("failed to connect to Kafka broker: %v", err)
			return -1, err
		}
		k.Meta.LeaderConnMap[leaderAddress] = conn
	}
	conn := k.Meta.LeaderConnMap[leaderAddress]

	// Send Fetch request
	err := k.writeCommand(conn, reqBuf.Bytes())
	if err != nil {
		return -1, err
	}
	// Read the response
	response, err := k.receiveResponse(conn)
	if err != nil {
		fmt.Println("Error Fetch response:", err)
		return -1, err
	}
	k.Meta.mu.Unlock()
	// Parse the Fetch response
	count, err := k.parseFetchResponse(response, file, compressionType)
	return count, err
}

// parseFetchResponse parses the Fetch response
func (k *KafkaClient) parseFetchResponse(response []byte, file *os.File, compressionType string) (int, error) {
	buf := bytes.NewReader(response)
	count := 0
	// Read number of topics
	var numTopics int32
	if err := binary.Read(buf, binary.BigEndian, &numTopics); err != nil {
		return -1, err
	}

	for i := 0; i < int(numTopics); i++ {
		// Read topic name
		var topicLength int16
		if err := binary.Read(buf, binary.BigEndian, &topicLength); err != nil {
			return -1, err
		}
		topicName := make([]byte, topicLength)
		if err := binary.Read(buf, binary.BigEndian, &topicName); err != nil {
			return -1, err
		}

		// Read number of partitions
		var numPartitions int32
		if err := binary.Read(buf, binary.BigEndian, &numPartitions); err != nil {
			return -1, err
		}

		for j := 0; j < int(numPartitions); j++ {
			// Read partition details
			var partition int32
			var errorCode int16
			var highwaterMarkOffset int64
			var messageSetSize int32

			if err := binary.Read(buf, binary.BigEndian, &partition); err != nil {
				return -1, err
			}
			if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
				return -1, err
			}
			if err := binary.Read(buf, binary.BigEndian, &highwaterMarkOffset); err != nil {
				return -1, err
			}
			if err := binary.Read(buf, binary.BigEndian, &messageSetSize); err != nil {
				return -1, err
			}

			// Read MessageSet
			messageSet := make([]byte, messageSetSize)
			if err := binary.Read(buf, binary.BigEndian, &messageSet); err != nil {
				return -1, err
			}

			// Parse MessageSet
			count = k.parseMessageSet(messageSet, messageSetSize, partition, file, compressionType)
		}
	}

	return count, nil
}

// parseMessageSet parses the message set
func (k *KafkaClient) parseMessageSet(messageSet []byte, messageSetSize int32, partition int32, file *os.File, compressionType string) int {
	buf := bytes.NewReader(messageSet)
	count := 0

	minMessageSetSize := (OffsetSize + CurrentMessageSize + CRCSize + AttributeSize + KeySize + ValueSize)
	for buf.Len() > 0 {
		// Read message details
		var offset int64
		var messageSize, crc int32
		var attribute int16
		var keyLen, msgLen int32

		currSize := messageSetSize

		for currSize > 0 {
			if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
				fmt.Printf("Error reading offset: %v\n", err)
				return count
			}
			if offset < 0 {
				return count
			}
			if err := binary.Read(buf, binary.BigEndian, &messageSize); err != nil {
				fmt.Printf("Error reading message size: %v %v\n", err, messageSize)
				return count
			}
			if messageSize == 0 {
				return count
			}
			if err := binary.Read(buf, binary.BigEndian, &crc); err != nil {
				fmt.Printf("Error reading CRC: %v %v\n", err, crc)
				return count
			}
			if err := binary.Read(buf, binary.BigEndian, &attribute); err != nil {
				fmt.Printf("Error reading Attribute: %v\n", err)
				return count
			}
			if err := binary.Read(buf, binary.BigEndian, &keyLen); err != nil {
				fmt.Printf("Error reading Key size: %v\n", err)
				return count
			}
			key := []byte("")
			if keyLen <= 0 {
				keyLen = 0
			} else {
				key = make([]byte, keyLen)
				if err := binary.Read(buf, binary.BigEndian, &key); err != nil {
					return count
				}
			}
			if err := binary.Read(buf, binary.BigEndian, &msgLen); err != nil {
				return count
			}
			msg := make([]byte, msgLen)
			if err := binary.Read(buf, binary.BigEndian, &msg); err != nil {
				return count
			}
			msgStr := string(msg)
			if compressionType != "none" {
				msgStrBytes, _ := decompress(msg, compressionType)
				msgStr = string(msgStrBytes)
			}
			msgStr = strings.ReplaceAll(msgStr, "\n", "")
			if k.Verbose && !k.ProduceToPulsarOption {
				msgStr = fmt.Sprintf("Partition: %d, Offset: %d, Key: %v, Message: %v", partition, offset, string(key), msgStr)
			}
			msgStr += "\n"
			if file != nil {
				file.Write([]byte(msgStr))
			} else if k.ProduceToPulsarOption {
				k.PulsarChannel <- msgStr
			} else {
				fmt.Printf(msgStr)
			}
			count += 1
			//currSize -= (8 + 4 + 4 + 2 + 4 + keyLen + 4 + msgLen)
			currSize -= (OffsetSize + CurrentMessageSize + CRCSize + AttributeSize + KeySize + keyLen + ValueSize + msgLen)
			if int(currSize) <= minMessageSetSize {
				return count
			}
		}

	}
	return count
}

func (k *KafkaClient) processDeleteTopicRequest(topic string) error {
	correlationID := int32(1)

	buf := new(bytes.Buffer)

	// Calculate the remaining size
	clientIDLength := int16(len(k.ClientID))
	topicNameLength := int16(len(topic))

	apiKey := int16(DeleteTopics) // CreateTopics API key
	apiVersion := int16(Version0)

	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(k.ClientID)

	// Number of topics (only one in this example)
	binary.Write(buf, binary.BigEndian, int32(1))

	// Topic Name
	binary.Write(buf, binary.BigEndian, topicNameLength)
	buf.WriteString(topic)

	// Timeout
	binary.Write(buf, binary.BigEndian, int32(10000))

	//Write Remaining size at the start
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	for i := 0; i < len(k.Meta.Brokers); i++ {
		isCreated, err := sendRequestToBroker(k, k.Meta.Brokers[i].Address, buf.Bytes(), topic, "deletetopic")
		if err != nil {
			return err
		} else {
			if isCreated {
				return nil
			}
		}
	}

	return nil
}

func (k *KafkaClient) decodeDeleteTopicResponse(topicName string, response []byte) (int16, error) {
	var errorCode int16

	buf := bytes.NewReader(response)

	// Decode topics
	var numTopics int32
	err := binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(numTopics); i++ {
		// Decode topic name length
		var nameLength int16
		err = binary.Read(buf, binary.BigEndian, &nameLength)
		if err != nil {
			return 0, err
		}

		// Decode topic name
		nameBytes := make([]byte, nameLength)
		err = binary.Read(buf, binary.BigEndian, &nameBytes)
		if err != nil {
			return 0, err
		}
		decodedTopicName := string(nameBytes)

		// Decode error code
		err = binary.Read(buf, binary.BigEndian, &errorCode)
		if err != nil {
			return 0, err
		}
		if errorCode != 0 {
			return errorCode, nil
		}

		// Compare decoded topic name with provided topic name
		if decodedTopicName == topicName {
			return 0, nil
		}
	}

	return 0, fmt.Errorf("topic '%s' not found in response", topicName)
}

func (k *KafkaClient) DeleteTopic(topic string) error {
	var err error

	isRootUser, err := isRoot()
	if err != nil {
		fmt.Println(err)
		return err
	}

	if !isRootUser {
		err = fmt.Errorf("This operation requires root privileges")
		return err
	}

	if err := k.processDeleteTopicRequest(topic); err != nil {
		err := fmt.Errorf("Error encoding:", err)
		return err
	}

	time.Sleep(time.Second)
	k.UpdateMetadata()
	return nil
}

func (k *KafkaClient) processDeleteRecordRequest(topic string, partition int, offset int64) error {
	correlationID := int32(1)
	buf := new(bytes.Buffer)

	// Calculate the remaining size
	clientIDLength := int16(len(k.ClientID))
	topicNameLength := int16(len(topic))

	apiKey := int16(DeleteRecords) // CreateTopics API key
	apiVersion := int16(Version0)

	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(k.ClientID)

	// Number of topics
	binary.Write(buf, binary.BigEndian, int32(1))

	// Topic Name
	binary.Write(buf, binary.BigEndian, topicNameLength)
	buf.WriteString(topic)

	// Number of partitions
	binary.Write(buf, binary.BigEndian, int32(1))
	binary.Write(buf, binary.BigEndian, int32(partition))
	binary.Write(buf, binary.BigEndian, int64(offset))

	// Timeout
	binary.Write(buf, binary.BigEndian, int32(10000))

	//Write Remaining size at the start
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	for i := 0; i < len(k.Meta.Brokers); i++ {
		isCreated, err := sendRequestToBroker(k, k.Meta.Brokers[i].Address, buf.Bytes(), topic, "deleterecord")
		if err != nil {
			return err
		} else {
			if isCreated {
				return nil
			}
		}
	}

	return nil
}

func (k *KafkaClient) decodeDeleteRecordResponse(topicName string, response []byte) (int16, error) {
	var errorCode int16

	buf := bytes.NewReader(response)

	var throttleTime int32
	err := binary.Read(buf, binary.BigEndian, &throttleTime)
	if err != nil {
		return 0, err
	}

	// Decode topics
	var numTopics int32
	err = binary.Read(buf, binary.BigEndian, &numTopics)
	if err != nil {
		return 0, err
	}

	for i := 0; i < int(numTopics); i++ {
		// Decode topic name length
		var nameLength int16
		err = binary.Read(buf, binary.BigEndian, &nameLength)
		if err != nil {
			return 0, err
		}

		// Decode topic name
		nameBytes := make([]byte, nameLength)
		err = binary.Read(buf, binary.BigEndian, &nameBytes)
		if err != nil {
			return 0, err
		}
		decodedTopicName := string(nameBytes)

		var numPartitions int32
		err := binary.Read(buf, binary.BigEndian, &numPartitions)
		if err != nil {
			return 0, err
		}

		for j := 0; j < int(numPartitions); j++ {
			var partition int32
			err := binary.Read(buf, binary.BigEndian, &partition)
			if err != nil {
				return 0, err
			}
			var lowWaterMark int32
			err = binary.Read(buf, binary.BigEndian, &lowWaterMark)
			if err != nil {
				return 0, err
			}
			// Decode error code
			err = binary.Read(buf, binary.BigEndian, &errorCode)
			if err != nil {
				return 0, err
			}

			if errorCode != 0 {
				return errorCode, nil
			}
		}

		// Compare decoded topic name with provided topic name
		if decodedTopicName == topicName {
			return 0, nil
		}
	}

	return 0, fmt.Errorf("topic '%s' not found in response", topicName)
}

func (k *KafkaClient) DeleteRecord(topic string, partition int, offset int64) error {
	var err error

	isRootUser, err := isRoot()
	if err != nil {
		fmt.Println(err)
		return err
	}

	if !isRootUser {
		err = fmt.Errorf("This operation requires root privileges")
		return err
	}

	if err = k.processDeleteRecordRequest(topic, partition, offset); err != nil {
		err = fmt.Errorf("Error encoding:", err)
		return err
	}

	k.UpdateMetadata()
	return nil
}

func (k *KafkaClient) ProduceFromMQTT(topic string, partition int, keyInput, valueInput string) error {
	return k.Produce(topic, partition, keyInput, valueInput, "none")
}

func (k *KafkaClient) ProduceFromPulsar(topic string, partition int, keyInput, valueInput string) error {
	return k.Produce(topic, partition, keyInput, valueInput, "none")
}
