package internal

import (
	"fmt"
	"sort"
	"net"
	"strings"
	"github.com/IBM/sarama"
)

func (k *KafkaClient) PrintConfigAPI() error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// Create a new client
	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		return fmt.Errorf("Error creating client: %v", err)
	}
	defer client.Close()

	// Get broker information
	brokers := client.Brokers()
	fmt.Println("Kafka Configuration:")
	fmt.Printf("Brokers: %d, Config:\n", len(brokers))
	
	// Sort brokers by ID
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID() < brokers[j].ID()
	})

	for _, broker := range brokers {
		addr := broker.Addr()
		host, port, _ := net.SplitHostPort(addr)
		fmt.Printf("\tHost ID: %d, Host Name: %s, Port: %s\n", broker.ID(), host, port)
	}
	fmt.Println()

	// Get topic information
	topics, err := client.Topics()
	if err != nil {
		return fmt.Errorf("Error getting topics: %v", err)
	}

	fmt.Printf("Topics: %d\n", len(topics))
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			return fmt.Errorf("Error getting partitions for topic %s: %v:", topic, err)
		}

		numReplicas := 0
		numISR := 0
		if len(partitions) > 0 {
			replicas, _ := client.Replicas(topic, partitions[0])
			isr, _ := client.InSyncReplicas(topic, partitions[0])
			numReplicas = len(replicas)
			numISR = len(isr)
		}

		fmt.Printf("\tTopic Name: %s, Number of Partitions: %d, Number of Replicas: %d, Number of InSync Replicas: %d, Config:\n",
			topic, len(partitions), numReplicas, numISR)

		for _, partition := range partitions {
			leader, err := client.Leader(topic, partition)
			if err != nil {
				continue
			}

			replicas, err := client.Replicas(topic, partition)
			if err != nil {
				continue
			}

			isr, err := client.InSyncReplicas(topic, partition)
			if err != nil {
				continue
			}

			fmt.Printf("\t\tPartition ID: %2d   Leader: %-2d   Replica ID: %-10s InSyncReplica ID: %s\n",
				partition, leader.ID(), formatIDs(replicas), formatIDs(isr))
		}
		fmt.Println()
	}
	return nil
}

func formatIDs(ids []int32) string {
	strIDs := make([]string, len(ids))
	for i, id := range ids {
		strIDs[i] = fmt.Sprintf("%d", id)
	}
	return strings.Join(strIDs, " ")
}

func (k *KafkaClient) PrintTopicsAPI() error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// Create a new client
	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		return fmt.Errorf("Error creating client: %v", err)
	}
	defer client.Close()

	// Get broker information
	brokers := client.Brokers()
	
	// Sort brokers by ID
	sort.Slice(brokers, func(i, j int) bool {
		return brokers[i].ID() < brokers[j].ID()
	})

	// Get topic information
	topics, err := client.Topics()
	if err != nil {
		return fmt.Errorf("Error getting topics: %v", err)
	}

	fmt.Printf("Kafka Topics: %d\n", len(topics))
	for _, topic := range topics {
		partitions, err := client.Partitions(topic)
		if err != nil {
			return fmt.Errorf("Error getting partitions for topic %s: %v:", topic, err)
		}

		numReplicas := 0
		numISR := 0
		if len(partitions) > 0 {
			replicas, _ := client.Replicas(topic, partitions[0])
			isr, _ := client.InSyncReplicas(topic, partitions[0])
			numReplicas = len(replicas)
			numISR = len(isr)
		}

		fmt.Printf("\tTopic Name: %s, Number of Partitions: %d, Number of Replicas: %d, Number of InSync Replicas: %d, Config:\n",
			topic, len(partitions), numReplicas, numISR)

		for _, partition := range partitions {
			leader, err := client.Leader(topic, partition)
			if err != nil {
				continue
			}

			replicas, err := client.Replicas(topic, partition)
			if err != nil {
				continue
			}

			isr, err := client.InSyncReplicas(topic, partition)
			if err != nil {
				continue
			}

			fmt.Printf("\t\tPartition ID: %2d   Leader: %-2d   Replica ID: %-10s InSyncReplica ID: %s\n",
				partition, leader.ID(), formatIDs(replicas), formatIDs(isr))
		}
		fmt.Println()
	}
	return nil
}

func (k *KafkaClient) PrintOffsetsAPI(topicInput string) error {
	config := sarama.NewConfig()
	config.Version = sarama.V2_8_0_0

	// Create a new client
	client, err := sarama.NewClient(k.Meta.BrokerAddress, config)
	if err != nil {
		return fmt.Errorf("Error creating client: %v", err)
	}
	defer client.Close()

	fmt.Println("Kafka Topic Offsets:")
	topics, err := client.Topics()
	if err != nil {
		return fmt.Errorf("Error creating client: %v", err)
	}

	for _, topic := range topics {
		if topicInput != "" && topic != topicInput {
			continue
		}
		partitions, err := client.Partitions(topic)
		if err != nil {
			continue
		}

		for _, partition := range partitions {
			oldestOffset, err := client.GetOffset(topic, partition, sarama.OffsetOldest)
			if err != nil {
				continue
			}

			newestOffset, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				continue
			}

			fmt.Printf("Topic: %s, Partition: %2d, FirstOffset: %d, LastOffset: %d\n",
				topic, partition, oldestOffset, newestOffset)
		}
		fmt.Printf("\n")
	}
	return nil
}
