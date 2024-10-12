package internal

import (
	"fmt"
	"github.com/IBM/sarama"
)

// DeleteTopic deletes a specific Kafka topic
func DeleteTopicAPI(brokers []string, topicName string) error {
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

	config := sarama.NewConfig()
	// Configure any additional settings if needed

	clusterAdmin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating cluster admin: %v", err)
	}
	defer clusterAdmin.Close()

	err = clusterAdmin.DeleteTopic(topicName)
	if err != nil {
		return fmt.Errorf("error deleting topic %s: %v", topicName, err)
	}

	fmt.Printf("Topic %s deleted successfully\n", topicName)
	return nil
}

func DeleteRecordsUpToOffsetAPI(brokers []string, topicName string, partitionOffsets map[int32]int64) error {
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

	config := sarama.NewConfig()
	// Configure any additional settings if needed

	admin, err := sarama.NewClusterAdmin(brokers, config)
	if err != nil {
		return fmt.Errorf("error creating cluster admin: %v", err)
	}
	defer admin.Close()

	// The API now expects a string (topic) and a map of partition offsets
	err = admin.DeleteRecords(topicName, partitionOffsets)
	if err != nil {
		return fmt.Errorf("error deleting records: %v", err)
	}

	fmt.Printf("Records deleted successfully up to specified offsets in topic %s\n", topicName)
	return nil
}
