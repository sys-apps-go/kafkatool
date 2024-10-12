package internal

import (
	"net"
	"sync"
	"bytes"
)

const (
	Version0 int16 = 0
	Version1 int16 = 1
	Version2 int16 = 2
	Version3 int16 = 3
)

const (
	Produce         int16 = 0
	Fetch           int16 = 1
	ListOffsets     int16 = 2
	Metadata        int16 = 3
	OffsetCommit    int16 = 8
	OffsetFetch     int16 = 9
	FindCoordinator int16 = 10
	JoinGroup       int16 = 11
	LeaveGroup      int16 = 13
	SyncGroup       int16 = 14
	DescribeGroups  int16 = 15
	ListGroups      int16 = 16
	ApiVersions     int16 = 18
	CreateTopics    int16 = 19
	DeleteTopics    int16 = 20
	DeleteRecords   int16 = 21
)

const (
	RequestSizeBytes    = 4
	ApiKeyBytes         = 2
	ApiVersionBytes     = 2
	CorrelationIDBytes  = 4
	ClientIDLenBytes    = 2
	RequiredAcksBytes   = 2
	TimeoutBytes        = 4
	TopicCountBytes     = 4
	TopicNameLenBytes   = 2
	PartitionCountBytes = 4
	PartitionBytes      = 4
	TotalMessageSize    = 4
	OffsetSize          = 8
	CurrentMessageSize  = 4
	CRCSize             = 4
	AttributeSize       = 2
	KeySize             = 4
	ValueSize           = 4
)

const (
	maxBufferSize = 1 * 1024 * 1024
	kafkaOutFile  = "/tmp/kafka"
)

type msgData struct {
	topic string
	msg   string
}

type Broker struct {
	NodeID  int32
	Host    string
	Port    int32
	Address string
}

type Partition struct {
	ErrorCode    int16
	PartitionID  int32
	LeaderID     int32
	ReplicaNodes []int32
	InSyncNodes  []int32
}

type Topic struct {
	ErrorCode  int16
	Name       string
	Partitions []Partition
}

type MetadataInfo struct {
	BrokerAddress []string
	Brokers       []Broker
	Topics        map[string]Topic
	Leader        map[string]map[int]string
	LeaderConnMap map[string]net.Conn
	mu            sync.Mutex
	ServerAddress map[int32]string
}

type CreateTopicRequest struct {
	TopicName         string
	NumPartitions     int32
	ReplicationFactor int16
	TimeoutMs         int32
}

type CreateTopicResponse struct {
	ErrorCode int16
}

type KafkaClient struct {
	CorrelationID         int32
	ClientID              string
	MainServer            string
	Meta                  *MetadataInfo
	Conn                  net.Conn
	BufferPool            *sync.Pool
	Wg                    sync.WaitGroup
	Verbose               bool
	ProduceToPulsarOption bool
	PulsarChannel         chan string
	CompressionType       string
	MaxBufferSize         int
	MessageChannel        chan string
	HeaderSize            int32
	BodySize              int32
	MessageSetSize        int32
	TotalSize             int32
}

type TrieNode struct {
	children map[rune]*TrieNode
	isEnd    bool
	command  string
}

type Trie struct {
	root *TrieNode
}

func NewKafkaClient() *KafkaClient {
	k := &KafkaClient{
		ClientID:       "client-1",
		CorrelationID:  1,
		PulsarChannel:  make(chan string, 1024),
		MaxBufferSize:  maxBufferSize,
		MessageChannel: make(chan string, 1024),
		BufferPool: &sync.Pool{
			New: func() interface{} {
				return new(bytes.Buffer)
			},
		},
	}
	k.HeaderSize = RequestSizeBytes + ApiKeyBytes + ApiVersionBytes + CorrelationIDBytes + ClientIDLenBytes + int32(len(k.ClientID))
	// For one Topic and one Partition
	k.BodySize = RequiredAcksBytes + TimeoutBytes + TopicCountBytes + TopicNameLenBytes + PartitionCountBytes + PartitionBytes
	return k
}
