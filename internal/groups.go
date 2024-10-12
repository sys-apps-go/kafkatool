package internal

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"net"
	"os"
	"github.com/IBM/sarama"
)

func (k *KafkaClient) subscribeGroup(topic, groupID string) error {
	memberID := ""
	protocolType := "consumer"
	protocolName := "roundrobin"
	clientID := k.ClientID

	coordinatorServer, err := findCoordinator(k.Conn, groupID, clientID)
	if err != nil {
		fmt.Println("Error:", err)
		return err
	}

	connCoord, err := net.Dial("tcp", coordinatorServer)
	if err != nil {
		fmt.Printf("failed to connect to Kafka broker: %v %v", coordinatorServer, err)
		return err
	}
	defer connCoord.Close()

	// Construct the JoinGroup request
	buf := new(bytes.Buffer)

	// Request header
	correlationID := int32(1)
	apiKey := int16(JoinGroup)    // JoinGroup API key
	apiVersion := int16(Version3) // API version
	clientIDLength := int16(len(clientID))
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(clientID)

	// JoinGroup request body
	groupIDLength := int16(len(groupID))
	binary.Write(buf, binary.BigEndian, groupIDLength)
	buf.WriteString(groupID)
	sessionTimeout := int32(10000)   // Session timeout in milliseconds
	rebalanceTimeout := int32(30000) // Rebalance timeout in milliseconds
	binary.Write(buf, binary.BigEndian, sessionTimeout)
	binary.Write(buf, binary.BigEndian, rebalanceTimeout)
	memberIDLength := int16(len(memberID))
	binary.Write(buf, binary.BigEndian, memberIDLength)
	buf.WriteString(memberID)
	protocolTypeLength := int16(len(protocolType))
	binary.Write(buf, binary.BigEndian, protocolTypeLength)
	buf.WriteString(protocolType)

	// Protocols
	numProtocols := int32(1)
	binary.Write(buf, binary.BigEndian, numProtocols)
	protocolNameLength := int16(len(protocolName))
	binary.Write(buf, binary.BigEndian, protocolNameLength)
	buf.WriteString(protocolName)

	// Protocol metadata
	metadata := new(bytes.Buffer)
	binary.Write(metadata, binary.BigEndian, int16(0))
	numTopics := 1
	binary.Write(metadata, binary.BigEndian, numTopics)
	topicLength := int16(len(topic))
	binary.Write(metadata, binary.BigEndian, topicLength)
	metadata.WriteString(topic)
	binary.Write(metadata, binary.BigEndian, int32(-1)) // User data

	metadataBytes := metadata.Bytes()
	metadataLength := int32(len(metadataBytes))
	binary.Write(buf, binary.BigEndian, metadataLength)
	buf.Write(metadataBytes)

	// Update the size of the request
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	// Send the JoinGroup request
	_, err = connCoord.Write(buf.Bytes())
	if err != nil {
		fmt.Printf("failed to send JoinGroup request: %v", err)
		return err
	}

	// Read the response
	responseSizeBytes := make([]byte, 4)
	_, err = connCoord.Read(responseSizeBytes)
	if err != nil {
		fmt.Printf("failed to read response size: %v", err)
		return err
	}
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)
	responseBytes := make([]byte, responseSize)
	_, err = connCoord.Read(responseBytes)
	if err != nil {
		fmt.Printf("failed to read response: %v", err)
		return err
	}

	// Handle the JoinGroup response
	response := bytes.NewBuffer(responseBytes)
	var correlationIDResp int32
	var throttleTime int32
	var errorCode int16
	var generationID int32
	var groupProtocolLength int16
	var leaderIDLength int16
	var memberIDRespLength int16
	var membersCount int32

	binary.Read(response, binary.BigEndian, &correlationIDResp)
	binary.Read(response, binary.BigEndian, &throttleTime)
	binary.Read(response, binary.BigEndian, &errorCode)
	if errorCode != 0 {
		err := fmt.Errorf("Error code processing Join Group Response: ", errorCode)
		return err
	}
	binary.Read(response, binary.BigEndian, &generationID)
	binary.Read(response, binary.BigEndian, &groupProtocolLength)
	groupProtocol := make([]byte, groupProtocolLength)
	response.Read(groupProtocol)
	binary.Read(response, binary.BigEndian, &leaderIDLength)
	leaderID := make([]byte, leaderIDLength)
	response.Read(leaderID)
	binary.Read(response, binary.BigEndian, &memberIDRespLength)
	memberIDResp := make([]byte, memberIDRespLength)
	response.Read(memberIDResp)
	binary.Read(response, binary.BigEndian, &membersCount)

	fmt.Printf("JoinGroup response: CorrelationID: %d, ErrorCode: %d, GenerationID: %d, GroupProtocol: %s, LeaderID: %s, MemberID: %s, MembersCount: %d\n",
		correlationIDResp, errorCode, generationID, groupProtocol, leaderID, memberIDResp, membersCount)

	syncGroup(connCoord, topic, groupID, clientID, string(memberIDResp), generationID)

	partitionCount := len(k.Meta.Topics[topic].Partitions)
	offMap, _ := getOffsets(connCoord, groupID, clientID, topic, partitionCount)
	fmt.Println(offMap)

	offsets := make(map[string]map[int32]int64)
	partitions := make(map[int32]int64)
	partitionIndex := 0
	var firstOffset, lastOffset int64
	for partition, _ := range k.Meta.Topics[topic].Partitions {
		firstOffset, lastOffset, err = k.ListOffsets(topic, partition)
		if err != nil {
			return fmt.Errorf("Error listing offsets for topic %v partition %v: %v", topic, partition, err)
		}
		fmt.Printf("Topic: %v, Partition: %v, FirstOffset: %v, LastOffset: %v\n",
			topic, partition, firstOffset, lastOffset)
		if firstOffset >= 0 && lastOffset >= 0 {
			partitions[int32(partition)] = lastOffset
			offsets[topic] = partitions
			partitionIndex = partition
			break
		}
	}

	err = commitOffsets(connCoord, groupID, string(memberIDResp), offsets)
	offMap, _ = getOffsets(connCoord, groupID, clientID, topic, partitionCount)
	// Read topic partitions that have group committed by this process
	count := int64(lastOffset - firstOffset)
	currCount := int64(0)
	fetchOffset := firstOffset
	for currCount < count {
		currCount, err := k.Fetch(topic, partitionIndex, fetchOffset, maxBufferSize, nil, "none")
		if err != nil {
			fmt.Printf("Fetch failed: %v\n", err)
			os.Exit(1)
		}
		if currCount < 0 {
			break
		}
		count += int64(currCount)
		fetchOffset += int64(currCount)
	}
	return nil
}

func findCoordinator(conn net.Conn, groupID, clientID string) (string, error) {
	// Prepare the request
	request := createFindCoordinatorRequest(groupID, clientID)

	// Send the request
	_, err := conn.Write(request)
	if err != nil {
		return "", fmt.Errorf("failed to write to Kafka broker: %v", err)
	}

	// Read the response
	responseSizeBytes := make([]byte, 4)
	_, err = conn.Read(responseSizeBytes)
	if err != nil {
		fmt.Printf("failed to read response size: %v", err)
		return "", err
	}
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)

	response := make([]byte, responseSize)
	_, err = conn.Read(response)
	if err != nil {
		fmt.Printf("failed to read response: %v", err)
		return "", err
	}

	// Handle the response
	server, err := handleFindCoordinatorResponse(response)
	if err != nil {
		return "", fmt.Errorf("failed to handle find coordinator response: %v", err)
	}

	return server, nil
}

func createFindCoordinatorRequest(groupID, clientID string) []byte {
	buf := new(bytes.Buffer)

	// Request size
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size

	// API key (Find Coordinator) and API version
	binary.Write(buf, binary.BigEndian, int16(10))
	binary.Write(buf, binary.BigEndian, int16(0))

	// Correlation ID
	binary.Write(buf, binary.BigEndian, int32(0))

	// Client ID
	binary.Write(buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)

	// Group ID
	binary.Write(buf, binary.BigEndian, int16(len(groupID)))
	buf.WriteString(groupID)
	binary.Write(buf, binary.BigEndian, int8(0))
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	return buf.Bytes()
}

func handleFindCoordinatorResponse(response []byte) (string, error) {
	buf := bytes.NewReader(response)
	var seqNum int32
	binary.Read(buf, binary.BigEndian, &seqNum)

	// Read throttle time
	var errorCode int16
	binary.Read(buf, binary.BigEndian, &errorCode)

	// Read node ID
	var nodeID int32
	binary.Read(buf, binary.BigEndian, &nodeID)

	// Read host
	var hostLength int16
	binary.Read(buf, binary.BigEndian, &hostLength)
	host := make([]byte, hostLength)
	buf.Read(host)

	// Read port
	var port int32
	binary.Read(buf, binary.BigEndian, &port)

	fmt.Printf("Coordinator found: NodeID=%d, Host=%s, Port=%d\n", nodeID, host, port)

	return fmt.Sprintf("%v:%v", string(host), port), nil
}

func syncGroup(connCoord net.Conn, topic, groupID, clientID, memberID string, generationID int32) error {
	// Construct the SyncGroup request
	buf := new(bytes.Buffer)

	// Request header
	correlationID := int32(1)
	apiKey := int16(SyncGroup)    // SyncGroup API key
	apiVersion := int16(Version2) // API version
	clientIDLength := int16(len(clientID))
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(clientID)

	// SyncGroup request body
	groupIDLength := int16(len(groupID))
	binary.Write(buf, binary.BigEndian, groupIDLength)
	buf.WriteString(groupID)
	binary.Write(buf, binary.BigEndian, generationID)
	memberIDLength := int16(len(memberID))
	binary.Write(buf, binary.BigEndian, memberIDLength)
	buf.WriteString(memberID)

	// Group assignments
	assignment := createGroupAssignment(topic, memberID, 5)
	binary.Write(buf, binary.BigEndian, int32(1))
	buf.Write(assignment)

	// Update the size of the request
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	// Send the SyncGroup request
	_, err := connCoord.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send SyncGroup request: %v", err)
	}

	// Read the response
	responseSizeBytes := make([]byte, 4)
	_, err = connCoord.Read(responseSizeBytes)
	if err != nil {
		return fmt.Errorf("failed to read response size: %v", err)
	}
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)
	responseBytes := make([]byte, responseSize)
	_, err = connCoord.Read(responseBytes)
	if err != nil {
		return fmt.Errorf("failed to read response: %v", err)
	}

	// Handle the SyncGroup response
	response := bytes.NewBuffer(responseBytes)
	var correlationIDResp int32
	var throttleTime int32
	var errorCode int16
	var assignmentLengthResp int32

	binary.Read(response, binary.BigEndian, &correlationIDResp)
	binary.Read(response, binary.BigEndian, &throttleTime)
	binary.Read(response, binary.BigEndian, &errorCode)
	binary.Read(response, binary.BigEndian, &assignmentLengthResp)
	assignmentResp := make([]byte, assignmentLengthResp)
	response.Read(assignmentResp)

	fmt.Printf("SyncGroup response: CorrelationID: %d, ThrottleTime: %d ms, ErrorCode: %d, Assignment: %v\n",
		correlationIDResp, throttleTime, errorCode, assignmentResp)

	return nil
}

func createGroupAssignment(topic, memberID string, partitions int) []byte {
	buf := new(bytes.Buffer)

	// Member assignment (example)
	memberIDLength := int16(len(memberID))
	binary.Write(buf, binary.BigEndian, memberIDLength)
	buf.WriteString(memberID)

	// Assignment metadata
	metadata := new(bytes.Buffer)
	binary.Write(metadata, binary.BigEndian, int16(0)) // Version
	numTopics := int32(1)
	binary.Write(metadata, binary.BigEndian, numTopics)
	topicLength := int16(len(topic))
	binary.Write(metadata, binary.BigEndian, topicLength)
	metadata.WriteString(topic)
	numPartitions := int32(partitions)
	binary.Write(metadata, binary.BigEndian, numPartitions)
	for i := 0; i < partitions; i++ {
		partition := int32(i)
		binary.Write(metadata, binary.BigEndian, int32(partition))
	}
	binary.Write(metadata, binary.BigEndian, int32(-1)) // User data

	metadataBytes := metadata.Bytes()
	metadataLength := int32(len(metadataBytes))
	binary.Write(buf, binary.BigEndian, metadataLength)
	buf.Write(metadataBytes)

	return buf.Bytes()
}

func getOffsets(connCoord net.Conn, groupID, clientID string, topic string, numPartitions int) (map[string]map[int32]int64, error) {
	// Construct the OffsetFetch request
	buf := new(bytes.Buffer)

	// Request header
	correlationID := int32(3)
	apiKey := int16(OffsetFetch)  // OffsetFetch API key
	apiVersion := int16(Version0) // API version
	clientIDLength := int16(len(clientID))
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(clientID)

	// OffsetFetch request body
	groupIDLength := int16(len(groupID))
	binary.Write(buf, binary.BigEndian, groupIDLength)
	buf.WriteString(groupID)

	numTopics := int32(1)
	binary.Write(buf, binary.BigEndian, numTopics)
	topicLength := int16(len(topic))
	binary.Write(buf, binary.BigEndian, topicLength)
	buf.WriteString(topic)
	binary.Write(buf, binary.BigEndian, int32(numPartitions))

	for i := 0; i < numPartitions; i++ {
		binary.Write(buf, binary.BigEndian, int32(i))
	}

	// Update the size of the request
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))
	// Send the OffsetFetch request
	_, err := connCoord.Write(buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("failed to send OffsetFetch request: %v", err)
	}

	// Read the response
	responseSizeBytes := make([]byte, 4)
	_, err = connCoord.Read(responseSizeBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to read response size: %v", err)
	}
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)
	response := make([]byte, responseSize)
	_, err = connCoord.Read(response)
	if err != nil {
		return nil, fmt.Errorf("failed to read response: %v", err)
	}
	// Parse the OffsetFetch response
	offsetMap, err := parseOffsetFetchResponse(response)
	return offsetMap, err
}

func parseOffsetFetchResponse(response []byte) (map[string]map[int32]int64, error) {
	buf := bytes.NewReader(response)

	var correlationID int32
	binary.Read(buf, binary.BigEndian, &correlationID)

	offsets := make(map[string]map[int32]int64)

	var numTopics int32
	binary.Read(buf, binary.BigEndian, &numTopics)
	for i := int32(0); i < numTopics; i++ {
		var topicLength int16
		binary.Read(buf, binary.BigEndian, &topicLength)
		topic := make([]byte, topicLength)
		buf.Read(topic)

		var numPartitions int32
		binary.Read(buf, binary.BigEndian, &numPartitions)
		partitions := make(map[int32]int64)
		for j := int32(0); j < numPartitions; j++ {
			var partition int32
			binary.Read(buf, binary.BigEndian, &partition)

			var offset int64
			binary.Read(buf, binary.BigEndian, &offset)

			var metadataLength int16
			binary.Read(buf, binary.BigEndian, &metadataLength)
			metadata := make([]byte, metadataLength)
			buf.Read(metadata)

			var partitionErrorCode int16
			binary.Read(buf, binary.BigEndian, &partitionErrorCode)

			partitions[partition] = offset
		}

		offsets[string(topic)] = partitions
	}

	return offsets, nil
}

func commitOffsets(conn net.Conn, groupID, memberID string, offsets map[string]map[int32]int64) error {
	// Construct the OffsetCommit request
	buf := new(bytes.Buffer)

	// Request header
	correlationID := int32(4)
	apiKey := int16(OffsetCommit) // OffsetCommit API key
	apiVersion := int16(Version0) // API version
	clientID := "sarama"
	clientIDLength := int16(len(clientID))
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size
	binary.Write(buf, binary.BigEndian, apiKey)
	binary.Write(buf, binary.BigEndian, apiVersion)
	binary.Write(buf, binary.BigEndian, correlationID)
	binary.Write(buf, binary.BigEndian, clientIDLength)
	buf.WriteString(clientID)

	// OffsetCommit request body
	groupIDLength := int16(len(groupID))
	binary.Write(buf, binary.BigEndian, groupIDLength)
	buf.WriteString(groupID)

	generationID := int32(0) // Set generation ID appropriately
	binary.Write(buf, binary.BigEndian, generationID)

	memberIDLength := int16(len(memberID))
	binary.Write(buf, binary.BigEndian, memberIDLength)
	buf.WriteString(memberID)

	retentionTime := int64(-1)
	binary.Write(buf, binary.BigEndian, retentionTime)

	numTopics := int32(len(offsets))

	binary.Write(buf, binary.BigEndian, numTopics)
	for topic, partitions := range offsets {
		topicLength := int16(len(topic))
		binary.Write(buf, binary.BigEndian, topicLength)
		buf.WriteString(topic)

		numPartitions := int32(len(partitions))
		binary.Write(buf, binary.BigEndian, numPartitions)
		for partition, offset := range partitions {
			binary.Write(buf, binary.BigEndian, int32(partition))
			binary.Write(buf, binary.BigEndian, offset)
			metadata := ""
			metadataLength := int16(len(metadata))
			binary.Write(buf, binary.BigEndian, metadataLength)
			buf.WriteString(metadata)
		}
	}

	// Update the size of the request
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	// Send the OffsetCommit request
	_, err := conn.Write(buf.Bytes())
	if err != nil {
		return fmt.Errorf("failed to send OffsetCommit request: %v", err)
	}

	// Read the OffsetCommit response
	response := make([]byte, 4096)
	_, err = conn.Read(response)
	if err != nil {
		return fmt.Errorf("failed to read OffsetCommit response: %v", err)
	}

	// Parse the OffsetCommit response
	return parseOffsetCommitResponse(response)
}

func parseOffsetCommitResponse(response []byte) error {
	buf := bytes.NewReader(response)
	var responseSize int32
	binary.Read(buf, binary.BigEndian, &responseSize)

	var correlationID int32
	binary.Read(buf, binary.BigEndian, &correlationID)

	// Read topics array length
	var numTopics int32
	if err := binary.Read(buf, binary.BigEndian, &numTopics); err != nil {
		return err
	}

	// Iterate over topics
	for i := int32(0); i < numTopics; i++ {
		// Read topic name length
		var topicNameLength int16
		if err := binary.Read(buf, binary.BigEndian, &topicNameLength); err != nil {
			return err
		}

		// Read topic name
		topicName := make([]byte, topicNameLength)
		if _, err := buf.Read(topicName); err != nil {
			return err
		}

		// Read partitions array length
		var numPartitions int32
		if err := binary.Read(buf, binary.BigEndian, &numPartitions); err != nil {
			return err
		}

		// Iterate over partitions
		for j := int32(0); j < numPartitions; j++ {

			// Read partition index
			var partitionIndex int32
			if err := binary.Read(buf, binary.BigEndian, &partitionIndex); err != nil {
				return err
			}

			// Read error code
			var errorCode int16
			if err := binary.Read(buf, binary.BigEndian, &errorCode); err != nil {
				return err
			}
			if errorCode != 0 {
				err := fmt.Errorf("Error code in Commit Offset: %v\n", errorCode)
				return err
			}

		}

	}

	return nil
}

func (k *KafkaClient) ListGroupsProto() error {
	conn := k.Conn
	// Prepare the request
	request := createListGroupsRequest(k.ClientID)

	// Send the request
	_, err := conn.Write(request)
	if err != nil {
		return fmt.Errorf("failed to write to Kafka broker: %v", err)
	}

	// Read the response
	responseSizeBytes := make([]byte, 4)
	_, err = conn.Read(responseSizeBytes)
	if err != nil {
		fmt.Printf("failed to read response size: %v", err)
		return err
	}
	responseSize := binary.BigEndian.Uint32(responseSizeBytes)

	response := make([]byte, responseSize)
	_, err = conn.Read(response)
	if err != nil {
		fmt.Printf("failed to read response: %v", err)
		return err
	}

	// Handle the response
	handleListGroupsResponse(response)

	return nil
}

func createListGroupsRequest(clientID string) []byte {
	buf := new(bytes.Buffer)

	// Request size
	binary.Write(buf, binary.BigEndian, int32(0)) // Placeholder for request size

	// API key (Find Coordinator) and API version
	binary.Write(buf, binary.BigEndian, int16(16))
	binary.Write(buf, binary.BigEndian, int16(0))

	// Correlation ID
	binary.Write(buf, binary.BigEndian, int32(0))

	// Client ID
	binary.Write(buf, binary.BigEndian, int16(len(clientID)))
	buf.WriteString(clientID)

	// Group ID
	requestSize := int32(buf.Len() - 4)
	binary.BigEndian.PutUint32(buf.Bytes()[0:], uint32(requestSize))

	return buf.Bytes()
}

func handleListGroupsResponse(response []byte) {
	buf := bytes.NewReader(response)

	var seqNum int32
	binary.Read(buf, binary.BigEndian, &seqNum)

	// Read Error code
	var errorCode int16
	binary.Read(buf, binary.BigEndian, &errorCode)

	// Read Group count
	var groupCount int32
	binary.Read(buf, binary.BigEndian, &groupCount)

	for i := 0; i < int(groupCount); i++ {
		var groupIDLength int16
		binary.Read(buf, binary.BigEndian, &groupIDLength)
		groupID := make([]byte, groupIDLength)
		buf.Read(groupID)
		var protocolTypeLength int16
		binary.Read(buf, binary.BigEndian, &protocolTypeLength)
		protocolType := make([]byte, protocolTypeLength)
		buf.Read(protocolType)
		fmt.Printf("Group ID: %v, Protocol Type: %v\n", string(groupID), string(protocolType))
	}
}

func (k *KafkaClient) ListGroups() error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_1_0_0

	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		fmt.Printf("Error creating client: %v", err)
		return err
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdminFromClient(client)
	if err != nil {
		fmt.Printf("Error creating cluster admin: %v", err)
		return err
	}
	defer admin.Close()

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		fmt.Printf("Error listing consumer groups: %v", err)
		return err
	}

	fmt.Println("Consumer Groups:")
	for group := range groups {
		fmt.Println(group)

		groupDetails, err := admin.DescribeConsumerGroups([]string{group})
		if err != nil {
			fmt.Printf("Error describing group: %v", err)
			return err
		}

		for _, detail := range groupDetails {
			fmt.Printf("  Group ID: %s\n", detail.GroupId)
			fmt.Printf("  State: %s\n", detail.State)
			fmt.Printf("  Protocol Type: %s\n", detail.ProtocolType)
			fmt.Printf("  Protocol: %s\n", detail.Protocol)

			fmt.Println("  Members:")
			for _, member := range detail.Members {
				fmt.Printf("    Client ID: %s\n", member.ClientId)
				fmt.Printf("    Client Host: %s\n", member.ClientHost)
			}
			fmt.Println()
		}
	}
	return nil
}
