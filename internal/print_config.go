package internal

import (
	"fmt"
)
func (k *KafkaClient) PrintTopics(printBrokers, printTopics bool) {
	k.PrintConfig(printBrokers, printTopics)
}

func (k *KafkaClient) PrintOffsets(topicName string) error {
	if len(k.Meta.Topics) == 0 {
		return fmt.Errorf("No Topic exists")
	}

	printOffsetsForTopic := func(topic string, t Topic) error {
		for partition, _ := range t.Partitions {
			firstOffset, lastOffset, err := k.ListOffsets(topic, partition)
			if err != nil {
				return fmt.Errorf("Error listing offsets for topic %v partition %v: %v", topic, partition, err)
			}
			fmt.Printf("Topic: %v, Partition: %2v, FirstOffset: %v, LastOffset: %v\n",
				topic, partition, firstOffset, lastOffset)
		}
		return nil
	}

	if topicName != "" {
		t, exists := k.Meta.Topics[topicName]
		if !exists {
			return fmt.Errorf("Topic %v doesn't exist", topicName)
		}
		fmt.Printf("Kafka Topic Offsets:\n")
		return printOffsetsForTopic(topicName, t)
	}

	fmt.Printf("\nKafka Topic Offsets:\n")

	for topic, t := range k.Meta.Topics {
		err := printOffsetsForTopic(topic, t)
		if err != nil {
			return err
		}
		fmt.Printf("\n")
	}
	return nil
}

func (k *KafkaClient) PrintConfig(printBrokers, printTopics bool) {
	k.printMetadata(printBrokers, printTopics)
}

func (k *KafkaClient) printMetadata(printBrokers, printTopics bool) {
	m := k.Meta
	numBrokers := len(m.Brokers)
	if printBrokers {
		fmt.Println("\nKafka Configuration:")
		fmt.Printf("Brokers: %v, Config: \n", numBrokers)
	}
	for i := 0; i < numBrokers; i++ {
		if printBrokers {
			fmt.Printf("	Host ID: %v, Host Name: %v, Port: %v\n", m.Brokers[i].NodeID, m.Brokers[i].Host, m.Brokers[i].Port)
		}
	}
	if printBrokers {
		fmt.Println("")
	}

	numTopics := len(m.Topics)
	if printTopics {
		fmt.Printf("Topics: %v\n", numTopics)
	}
	for topic, t := range m.Topics {
		numPartitions := len(t.Partitions)
		for i := 0; i < numPartitions; i++ {
			partition := t.Partitions[i]
			numReplicaNodes := len(partition.ReplicaNodes)
			numInSyncNodes := len(partition.InSyncNodes)
			if i == 0 && printTopics {
				fmt.Printf("	Topic Name: %v, Number of Partitions: %v, Number of Replicas: %v, Number of InSync Replicas: %v, Config:\n",
					topic, numPartitions, numReplicaNodes, numInSyncNodes)
			}

			if printTopics {
				fmt.Printf("		Partition ID: %2v   Leader: %v   Replica ID: ", partition.PartitionID, partition.LeaderID)
			}
			for j := 0; j < numReplicaNodes; j++ {
				replicaNodeID := partition.ReplicaNodes[j]
				if printTopics {
					fmt.Printf("%v ", replicaNodeID)
				}
			}
			if printTopics {
				fmt.Printf(" InSyncReplica ID: ")
			}
			for j := 0; j < numInSyncNodes; j++ {
				inSyncNodeID := partition.InSyncNodes[j]
				if printTopics {
					fmt.Printf("%v ", inSyncNodeID)
				}
			}
			if printTopics {
				fmt.Printf("\n")
			}
		}
		if printTopics {
			fmt.Println("")
		}
	}

}
