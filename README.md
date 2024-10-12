NAME:
   kafkatool - A Kafka client tool

USAGE:
   kafkatool [global options] command [command options]

COMMANDS:
   createtopic        Create a new topic
   produce            Produce messages to a topic partition
   produceconsole     Produce messages from the console
   producestream      Produce messages as a stream from a file
   producetopic       Produce a message to a topic
   producefrompulsar  Produce messages from pulsar to kafka
   producefrommqtt    Produce messages from MQTT to kafka
   producetopulsar    Produce messages from kafka to pulsar
   fetch              Fetch messages from a topic partition
   fetchloop          Fetch messages in a loop
   fetchtopic         Fetch all messages from a topic from all partitions
   subscribegroup     Subscribe to a group to listen for given topic
   listoffsets        List offsets of all topics or a given topic
   listconfig         List configuration of the client
   listtopics         List all topics
   listgroups         List all consumer groups
   help, h            Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --broker value, -b value  Main broker address
   --help, -h                show help


1. listconfig:

Kafka Configuration:
Brokers: 5, Config: 
	Host ID: 0, Host Name: linuxmint, Port: 9092
	Host ID: 4, Host Name: linuxmint, Port: 9096
	Host ID: 1, Host Name: linuxmint, Port: 9093
	Host ID: 2, Host Name: linuxmint, Port: 9094
	Host ID: 3, Host Name: linuxmint, Port: 9095

Topics: 1
	Topic Name: topic1, Number of Partitions: 5, Number of Replicas: 3, Number of InSync Replicas: 3, Config:
		Partition ID:  0   Leader: 1  Replica ID: 1 2 3  InSyncReplica ID: 1 2 3 
		Partition ID:  1   Leader: 2  Replica ID: 2 3 0  InSyncReplica ID: 2 3 0 
		Partition ID:  2   Leader: 3  Replica ID: 3 0 4  InSyncReplica ID: 3 0 4 
		Partition ID:  3   Leader: 0  Replica ID: 0 4 1  InSyncReplica ID: 0 4 1 
		Partition ID:  4   Leader: 4  Replica ID: 4 1 2  InSyncReplica ID: 4 1 2 

2. listoffsets:

Kafka Topic Offsets:
Topic: topic1, Partition:  0, FirstOffset: 0, LastOffset: 100
Topic: topic1, Partition:  1, FirstOffset: 0, LastOffset: 50
Topic: topic1, Partition:  2, FirstOffset: 0, LastOffset: 200
Topic: topic1, Partition:  3, FirstOffset: 0, LastOffset: 100
Topic: topic1, Partition:  4, FirstOffset: 0, LastOffset: 200
