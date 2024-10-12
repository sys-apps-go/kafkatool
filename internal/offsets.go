package internal

import (
	"bytes"
	"fmt"
	"net"
	"time"
	"encoding/binary"
)

func (k *KafkaClient) encodeListOffsetsRequest(correlationID int32, clientID string, replicaID int32, topic string, partition int, timestamp int64, maxNumOffsets int32) ([]byte, error) {
	buf := new(bytes.Buffer)

	// Calculate the remaining size
	clientIDLength := int32(len(clientID))
	topicNameLength := int32(len(topic))

	apiKey := int16(ListOffsets) // ListOffsets API key
	apiVersion := int16(Version0)

	// Write the header
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)

	// Client ID
	binary.Write(buf, binary.BigEndian, int16(clientIDLength))
	buf.WriteString(clientID)

	// Replica ID
	binary.Write(buf, binary.BigEndian, replicaID)

	// Number of topics
	binary.Write(buf, binary.BigEndian, int32(1))

	// Topic Name
	binary.Write(buf, binary.BigEndian, int16(topicNameLength))
	buf.WriteString(topic)

	// Number of Partitions
	binary.Write(buf, binary.BigEndian, int32(1))

	// Partition Index
	binary.Write(buf, binary.BigEndian, int32(partition))

	// Timestamp
	binary.Write(buf, binary.BigEndian, timestamp)

	// Max Number of Offsets
	binary.Write(buf, binary.BigEndian, maxNumOffsets)

	//Write Remaining size at the start
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	return buf.Bytes(), nil
}

func (k *KafkaClient) decodeListOffsetsResponse(response []byte) (int64, error) {
	buf := bytes.NewReader(response)
	var numTopics int32
	if err := binary.Read(buf, binary.BigEndian, &numTopics); err != nil {
		return int64(-1), err
	}

	for i := 0; i < int(numTopics); i++ {
		var topicNameLength int16
		if err := binary.Read(buf, binary.BigEndian, &topicNameLength); err != nil {
			return int64(-1), err
		}

		topicNameBytes := make([]byte, topicNameLength)
		if err := binary.Read(buf, binary.BigEndian, &topicNameBytes); err != nil {
			return int64(-1), err
		}
		topicName := string(topicNameBytes)

		var numPartitions int32
		if err := binary.Read(buf, binary.BigEndian, &numPartitions); err != nil {
			return int64(-1), err
		}

		var partitionIndex int32
		if err := binary.Read(buf, binary.BigEndian, &partitionIndex); err != nil {
			return int64(-1), err
		}

		var errorCode int16
		if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
			return int64(-1), err
		}

		if errorCode != 0 {
			return int64(-1), fmt.Errorf("error code %d for topic %s, partition %d", errorCode, topicName, partitionIndex)
		}

		var numOffsets int32
		if err := binary.Read(buf, binary.BigEndian, &numOffsets); err != nil {
			return int64(-1), err
		}
		var offset int64
		if err := binary.Read(buf, binary.BigEndian, &offset); err != nil {
			return int64(-1), err
		}

		return offset, nil
	}

	return int64(-1), nil
}

func (k *KafkaClient) ListOffsets(topic string, partition int) (int64, int64, error) {
	clientID := "client-1"
	replicaID := int32(-1)
	maxNumOffsets := int32(1)

	// Get start offset
	startTimestamp := int64(-2) // -2 for earliest
	startReq, err := k.encodeListOffsetsRequest(1, clientID, replicaID, topic, partition, startTimestamp, maxNumOffsets)
	if err != nil {
		return 0, 0, err
	}

	leaderAddress := k.Meta.Leader[topic][partition]
	conn, err := net.DialTimeout("tcp", leaderAddress, 5*time.Second)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to connect to Kafka broker: %v", err)
	}
	defer conn.Close()
	err = k.writeCommand(conn, startReq)
	if err != nil {
		return 0, 0, err
	}

	startResponse, err := k.receiveResponse(conn)
	if err != nil {
		return 0, 0, err
	}

	startOffset, err := k.decodeListOffsetsResponse(startResponse)
	if err != nil {
		return 0, 0, err
	}

	// Get end offset
	endTimestamp := int64(-1) // -1 for latest
	endReq, err := k.encodeListOffsetsRequest(2, clientID, replicaID, topic, partition, endTimestamp, maxNumOffsets)
	if err != nil {
		return 0, 0, err
	}

	err = k.writeCommand(conn, endReq)
	if err != nil {
		return 0, 0, err
	}

	endResponse, err := k.receiveResponse(conn)
	if err != nil {
		return 0, 0, err
	}

	endOffset, err := k.decodeListOffsetsResponse(endResponse)
	if err != nil {
		return 0, 0, err
	}

	return startOffset, endOffset, nil
}
