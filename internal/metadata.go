package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"sort"
	"time"
	"runtime"
)

func (k *KafkaClient) encodeAndSendMetadataRequest() error {
	var correlationID int32
	var clientID string

	correlationID = k.CorrelationID
	clientID = k.ClientID

	buf := new(bytes.Buffer)

	// Kafka header
	apiKey := int16(Metadata) // Metadata API key
	apiVersion := int16(Version0)
	clientIDLength := int16(len(clientID))
	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(clientID)

	// Topics
	binary.Write(buf, binary.BigEndian, uint32(0))

	//Write Remaining size at the start
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	err := k.writeCommand(k.Conn, buf.Bytes())
	if err != nil {
		return err
	}

	return nil
}

func (k *KafkaClient) writeCommand(conn net.Conn, pkt []byte) error {
	err := conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if err != nil {
		return fmt.Errorf("failed to set write deadline: %v", err)
	}

	// Write request to connection
	_, err = conn.Write(pkt)
	if err != nil {
		return fmt.Errorf("failed to write request to connection: %v", err)
	}
	return nil
}

func (k *KafkaClient) receiveResponse(conn net.Conn) ([]byte, error) {
	// Get a buffer from the pool for the size
	sizeBuf := k.BufferPool.Get().(*bytes.Buffer)
	defer func() {
		sizeBuf.Reset()
		k.BufferPool.Put(sizeBuf)
	}()
	sizeBuf.Grow(4) // Ensure the buffer has at least 4 bytes capacity

	// Read the first 4 bytes to get the message size
	_, err := io.CopyN(sizeBuf, conn, 4)
	if err != nil {
		return nil, fmt.Errorf("failed to read message size: %v", err)
	}

	// Convert the message size from network byte order to integer
	size := int(binary.BigEndian.Uint32(sizeBuf.Bytes()))

	// Get another buffer from the pool for the message
	msgBuf := k.BufferPool.Get().(*bytes.Buffer)
	defer func() {
		msgBuf.Reset()
		k.BufferPool.Put(msgBuf)
	}()
	msgBuf.Grow(size) // Ensure the buffer has enough capacity

	// Read the remaining bytes of the message
	_, err = io.CopyN(msgBuf, conn, int64(size))
	if err != nil {
		return nil, fmt.Errorf("failed to read message: %v", err)
	}

	// Create a copy of the message bytes (excluding the size)
	// This is necessary because we're returning the buffer to the pool
	response := make([]byte, size-4)
	copy(response, msgBuf.Bytes()[4:])

	return response, nil
}

func (k *KafkaClient) decodeMetadataResponse(response []byte) (MetadataInfo, error) {
	var metadata MetadataInfo

	buf := bytes.NewReader(response)

	// Decode brokers
	var numBrokers int32
	binary.Read(buf, binary.BigEndian, &numBrokers)
	metadata.Brokers = make([]Broker, numBrokers)
	metadata.BrokerAddress = make([]string, numBrokers)
	metadata.Leader = make(map[string]map[int]string)
	metadata.ServerAddress = make(map[int32]string)
	metadata.LeaderConnMap = make(map[string]net.Conn)

	for i := 0; i < int(numBrokers); i++ {
		var broker Broker
		binary.Read(buf, binary.BigEndian, &broker.NodeID)

		var hostLength int16
		binary.Read(buf, binary.BigEndian, &hostLength)
		hostBytes := make([]byte, hostLength)
		binary.Read(buf, binary.BigEndian, &hostBytes)
		broker.Host = string(hostBytes)

		binary.Read(buf, binary.BigEndian, &broker.Port)
		broker.Address = fmt.Sprintf("%v:%v", broker.Host, broker.Port)
		metadata.BrokerAddress = append(metadata.BrokerAddress, broker.Address)
		metadata.Brokers[i] = broker
		metadata.ServerAddress[broker.NodeID] = broker.Address
	}

	// Decode topics
	var numTopics int32
	binary.Read(buf, binary.BigEndian, &numTopics)
	metadata.Topics = make(map[string]Topic)
	maxPartitions := 0
	for i := 0; i < int(numTopics); i++ {
		var topic Topic
		binary.Read(buf, binary.BigEndian, &topic.ErrorCode)

		var nameLength int16
		binary.Read(buf, binary.BigEndian, &nameLength)
		nameBytes := make([]byte, nameLength)
		binary.Read(buf, binary.BigEndian, &nameBytes)
		topic.Name = string(nameBytes)
		metadata.Leader[topic.Name] = make(map[int]string)

		var numPartitions int32
		binary.Read(buf, binary.BigEndian, &numPartitions)
		topic.Partitions = make([]Partition, numPartitions)
		if maxPartitions  < int(numPartitions) {
			maxPartitions = int(numPartitions)
		}
		for j := 0; j < int(numPartitions); j++ {
			var partition Partition
			binary.Read(buf, binary.BigEndian, &partition.ErrorCode)
			binary.Read(buf, binary.BigEndian, &partition.PartitionID)
			binary.Read(buf, binary.BigEndian, &partition.LeaderID)
			metadata.Leader[topic.Name][int(partition.PartitionID)] = metadata.ServerAddress[partition.LeaderID]

			var numReplicaNodes int32
			binary.Read(buf, binary.BigEndian, &numReplicaNodes)
			partition.ReplicaNodes = make([]int32, numReplicaNodes)
			for k := 0; k < int(numReplicaNodes); k++ {
				var replicaNode int32
				binary.Read(buf, binary.BigEndian, &replicaNode)
				partition.ReplicaNodes[k] = replicaNode
			}

			var numInSyncNodes int32
			binary.Read(buf, binary.BigEndian, &numInSyncNodes)
			partition.InSyncNodes = make([]int32, numInSyncNodes)
			for k := 0; k < int(numInSyncNodes); k++ {
				var isrNode int32
				binary.Read(buf, binary.BigEndian, &isrNode)
				partition.InSyncNodes[k] = isrNode
			}
			topic.Partitions[j] = partition
		}
		sort.Slice(topic.Partitions, func(i, j int) bool { return topic.Partitions[i].PartitionID < topic.Partitions[j].PartitionID })
		metadata.Topics[topic.Name] = topic
	}

	runtime.GOMAXPROCS(maxPartitions)

	return metadata, nil
}

func (k *KafkaClient) UpdateMetadata() error {
	err := k.encodeAndSendMetadataRequest()
	if err != nil {
		fmt.Println("Error sending metadata request:", err)
		return err
	}

	response, err := k.receiveResponse(k.Conn)
	if err != nil {
		fmt.Println("Error getting metadata response:", err)
		return err
	}

	m, err := k.decodeMetadataResponse(response)
	if err != nil {
		fmt.Println("Error processing metadata response:", err)
		return err
	}
	k.Meta = &m
	return nil
}
