package main

import (
	"bufio"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"github.com/sys-apps-go/kafkatool/internal"
	"github.com/urfave/cli/v2"
	"io/ioutil"
	"log"
	"net"
	"os"
	"strings"
	"time"
)

func main() {
	client := internal.NewKafkaClient()
	app := &cli.App{
		Name:  "kafkatool",
		Usage: "A Kafka client tool",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "broker",
				Aliases: []string{"b"},
				Usage:   "Main broker address",
			},
			&cli.BoolFlag{
				Name:  "tls",
				Usage: "Use TLS connection",
			},
			&cli.StringFlag{
				Name:  "ca-cert",
				Usage: "Path to CA certificate file (required for TLS)",
			},
		},
		Before: func(c *cli.Context) error {
			brokerAddress := c.String("broker")
			if brokerAddress == "" {
				brokerAddress = "localhost:9092"
			}
			client.MainServer = brokerAddress

			var conn net.Conn
			var err error

			if c.Bool("tls") {
				// TLS connection
				caCertPath := c.String("ca-cert")
				if caCertPath == "" {
					return fmt.Errorf("CA certificate path is required for TLS connection")
				}

				conn, err = connectTLS(brokerAddress, caCertPath)
			} else {
				// Normal TCP connection
				conn, err = net.DialTimeout("tcp", brokerAddress, 5*time.Second)
			}

			if err != nil {
				fmt.Printf("failed to connect to Kafka broker: %v\n", err)
				return err
			}

			client.Conn = conn
			client.UpdateMetadata()

			return nil
		},
		Commands: []*cli.Command{
			{
				Name:  "createtopic",
				Usage: "Create a new topic",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Required: true, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitions", Aliases: []string{"p"}, Value: 1, Usage: "Number of Partitions"},
					&cli.IntFlag{Name: "replicas", Aliases: []string{"r"}, Value: 1, Usage: "Number of Replicas"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					partitions := c.Int("partitions")
					replicas := c.Int("replicas")

					fmt.Printf("Creating topic: %s with %d partitions and %d replicas...\n", topic, partitions, replicas)
					err := client.CreateTopic(topic, partitions, replicas)
					if err != nil {
						fmt.Printf("Error in creating topic: %v, error: %v\n", topic, err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "produce",
				Usage: "Produce messages to a topic partition",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.IntFlag{Name: "messagesize", Aliases: []string{"s"}, Value: 128, Usage: "Message size"},
					&cli.IntFlag{Name: "messagecount", Aliases: []string{"n"}, Value: 100, Usage: "Number of messages"},
					&cli.StringFlag{Name: "message", Aliases: []string{"m"}, Value: "1234567890", Usage: "Message string"},
					&cli.StringFlag{Name: "key", Aliases: []string{"k"}, Value: "key", Usage: "Key"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					var err error
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					messageSize := c.Int("messagesize")
					numMessages := c.Int("messagecount")
					message := c.String("message")
					key := c.String("key")
					api := c.Bool("api")
					verbose := c.Bool("verbose")
					compressionType := c.String("compression")
					client.CompressionType = compressionType
					client.Verbose = verbose

					_, exists := client.Meta.Topics[topic]
					if !exists {
						client.CreateTopic(topic, 1, 1)
					}

					if api {
						err = client.ProduceToTopicPartition(topic, partitionIndex, numMessages, key, message, messageSize,
							client.MaxBufferSize, compressionType)
					} else {
						_, err = client.ProduceMessageSet(topic, partitionIndex, numMessages, key,
							message, messageSize, client.MaxBufferSize, compressionType)
					}
					if err != nil {
						fmt.Println("Error Producing data to kafka:", err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "produceconsole",
				Usage: "Produce messages from the console",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.StringFlag{Name: "key", Aliases: []string{"k"}, Value: "key", Usage: "Key"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					key := c.String("key")
					compressionType := c.String("compression")
					client.CompressionType = compressionType

					_, exists := client.Meta.Topics[topic]
					if !exists {
						client.CreateTopic(topic, 1, 1)
					}

					scanner := bufio.NewScanner(os.Stdin)
					for scanner.Scan() {
						line := scanner.Text()
						err := client.Produce(topic, partitionIndex, key, line, compressionType)
						if err != nil {
							fmt.Println("Error Producing data to kafka:", err)
							os.Exit(1)
						}
					}
					return nil
				},
			},
			{
				Name:  "producestream",
				Usage: "Produce messages as a stream from a file",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.StringFlag{Name: "inputfile", Aliases: []string{"f"}, Usage: "Input file"},
					&cli.StringFlag{Name: "key", Aliases: []string{"k"}, Value: "key", Usage: "Key"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					inputFile := c.String("inputfile")
					key := c.String("key")
					compressionType := c.String("compression")
					api := c.Bool("api")
					verbose := c.Bool("verbose")
					client.CompressionType = compressionType

					_, exists := client.Meta.Topics[topic]
					if !exists {
						client.CreateTopic(topic, 1, 1)
					}

					partitions := len(client.Meta.Topics[topic].Partitions)
					if inputFile == "" {
						fmt.Println("Input file not given")
						os.Exit(1)
					}
					client.Verbose = verbose

					if api {
						go client.StreamDataFromFileToKafka(topic, partitions, key, client.MaxBufferSize, compressionType)
					}
					err := client.StreamFileToKafka(inputFile, topic, key, compressionType, partitions, api)
					if err != nil {
						fmt.Println("Error streaming file", err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "producetopic",
				Usage: "Produce a message to a topic",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.StringFlag{Name: "message", Aliases: []string{"m"}, Value: "1234567890", Usage: "Message string"},
					&cli.IntFlag{Name: "messagecount", Aliases: []string{"n"}, Value: 100, Usage: "Number of messages"},
					&cli.StringFlag{Name: "key", Aliases: []string{"k"}, Value: "key", Usage: "Key"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
					&cli.IntFlag{Name: "messagesize", Aliases: []string{"s"}, Value: 128, Usage: "Message size"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					numMessages := c.Int("messagecount")
					messageSize := c.Int("messagesize")
					message := c.String("message")
					key := c.String("key")
					compressionType := c.String("compression")
					verbose := c.Bool("verbose")
					client.CompressionType = compressionType

					t, exists := client.Meta.Topics[topic]
					if !exists {
						client.CreateTopic(topic, 1, 1)
						t = client.Meta.Topics[topic]
					}

					client.Verbose = verbose
					partitions := len(t.Partitions)
					client.Wg.Add(partitions)
					for partitionIndex := 0; partitionIndex < partitions; partitionIndex++ {
						go func() {
							defer client.Wg.Done()
							_, err := client.ProduceMessageSet(topic, partitionIndex, numMessages, key,
								message, messageSize, client.MaxBufferSize, compressionType)
							if err != nil {
								fmt.Println("Produce batch failed", err)
							}
						}()
					}
					client.Wg.Wait()
					return nil
				},
			},
			{
				Name:  "producefrompulsar",
				Usage: "Produce messages from pulsar to kafka",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Kafka topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.StringFlag{Name: "pulsartopic", Aliases: []string{"pt"}, Usage: "Pulsar topic name"},
					&cli.StringFlag{Name: "pulsarbroker", Aliases: []string{"pb"}, Value: "pulsar://localhost:6650", Usage: "Pulsar broker name"},
					&cli.StringFlag{Name: "key", Aliases: []string{"k"}, Value: "key", Usage: "Key"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					pulsarTopic := c.String("pulsartopic")
					pulsarBroker := c.String("pulsarbroker")
					key := c.String("key")
					verbose := c.Bool("verbose")

					client.Verbose = verbose
					_, exists := client.Meta.Topics[topic]
					if !exists {
						client.CreateTopic(topic, 1, 1)
					}

					err := client.PublishMessagesFromPulsar(pulsarBroker, pulsarTopic, topic, partitionIndex, key)
					if err != nil {
						fmt.Println("Error PulsarToKafka:", err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "producefrommqtt",
				Usage: "Produce messages from MQTT to kafka",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topicfilter", Aliases: []string{"tf"}, Usage: "MQTT topic filter name"},
					&cli.StringFlag{Name: "mqttbroker", Aliases: []string{"mb"}, Value: "localhost:1883", Usage: "MQTT broker address"},
				},
				Action: func(c *cli.Context) error {
					topicFilter := c.String("topicfilter")
					mqttBroker := c.String("mqttbroker")
					err := client.MQTTTopicsToKafka(topicFilter, mqttBroker)
					if err != nil {
						fmt.Printf("Fetch failed: %v\n", err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "producetopulsar",
				Usage: "Produce messages from kafka to pulsar",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Kafka topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.StringFlag{Name: "pulsartopic", Aliases: []string{"pt"}, Usage: "Pulsar topic name"},
					&cli.StringFlag{Name: "pulsarbroker", Aliases: []string{"pb"}, Value: "pulsar://localhost:6650", Usage: "Pulsar broker name"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					pulsarTopic := c.String("pulsartopic")
					pulsarBroker := c.String("pulsarbroker")
					verbose := c.Bool("verbose")

					_, exists := client.Meta.Topics[topic]
					if !exists {
						fmt.Printf("Topic %v doesn't exist\n", topic)
						os.Exit(1)
					}

					client.Verbose = verbose
					client.ProduceToPulsarOption = true

					var file *os.File

					pulsarURL := pulsarBroker

					go client.PublishMessagesToPulsar(pulsarURL, pulsarTopic)

					maxBytes := int32(1048576) // 1 MB

					firstOffset, lastOffset, err := client.ListOffsets(topic, partitionIndex)
					if err != nil {
						fmt.Println("Error List Offsets:", err)
						os.Exit(1)
					}
					fetchOffset := firstOffset

					count := int64(0)
					compressionType := "none"
					for count < lastOffset {
						currCount, err := client.Fetch(topic, partitionIndex, fetchOffset, maxBytes, file, compressionType)
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
					close(client.PulsarChannel)
					return nil
				},
			},
			{
				Name:  "fetch",
				Usage: "Fetch messages from a topic partition",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.StringFlag{Name: "outputfile", Aliases: []string{"f"}, Value: "", Usage: "Output file name"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
				},
				Action: func(c *cli.Context) error {
					var file *os.File
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					outFileName := c.String("outputfile")
					verbose := c.Bool("verbose")
					compressionType := c.String("compression")
					api := c.Bool("api")
					client.CompressionType = compressionType

					if !api {
						maxBytes := int32(1048576) // 1 MB

						firstOffset, lastOffset, err := client.ListOffsets(topic, partitionIndex)
						if err != nil {
							fmt.Println("Error List Offsets:", err)
							os.Exit(1)
						}
						fetchOffset := firstOffset

						count := int64(0)

						if outFileName != "" {
							file, err = os.Create(outFileName)
							if err != nil {
								fmt.Printf("failed to create file: %v", err)
								os.Exit(1)
							}
						}

						client.Verbose = verbose
						for count < lastOffset {
							currCount, err := client.Fetch(topic, partitionIndex, fetchOffset, maxBytes, file, compressionType)
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
					} else {
						err := client.FetchFromPartition(topic, int32(partitionIndex), compressionType)
						if err != nil {
							fmt.Printf("Fetch failed: %v\n", err)
							os.Exit(1)
						}
					}
					return nil
				},
			},
			{
				Name:  "fetchloop",
				Usage: "Fetch messages in a loop",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.StringFlag{Name: "outputfile", Aliases: []string{"f"}, Value: "", Usage: "Output file name"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
				},
				Action: func(c *cli.Context) error {
					var file *os.File
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					outFileName := c.String("outputfile")
					verbose := c.Bool("verbose")
					compressionType := c.String("compression")
					client.CompressionType = compressionType
					maxBytes := int32(1048576) // 1 MB

					firstOffset, lastOffset, err := client.ListOffsets(topic, partitionIndex)
					if err != nil {
						fmt.Println("Error List Offsets:", err)
						return err
					}
					fetchOffset := firstOffset
					client.Verbose = verbose

					if outFileName != "" {
						file, err = os.Create(outFileName)
						if err != nil {
							fmt.Printf("failed to create file: %v", err)
							return err
						}
						defer file.Close()
					}

					for {
						// Fetch the latest last offset to check for new messages
						_, lastOffset, err = client.ListOffsets(topic, partitionIndex)
						if err != nil {
							fmt.Println("Error List Offsets:", err)
							time.Sleep(5 * time.Second)
							continue
						}

						// While there are new messages, fetch them
						for fetchOffset < lastOffset {
							currCount, err := client.Fetch(topic, partitionIndex, fetchOffset, maxBytes, file, compressionType)
							if err != nil {
								fmt.Printf("Fetch failed: %v\n", err)
								time.Sleep(5 * time.Second) // Wait before retrying in case of failure
								continue
							}
							if currCount < 0 {
								break
							}
							fetchOffset += int64(currCount)
						}

						// Sleep for a short duration before checking for new messages
						time.Sleep(1 * time.Second)
					}
					return nil
				},
			},
			{
				Name:  "fetchtopic",
				Usage: "Fetch all messages from a topic from all partitions",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.StringFlag{Name: "outputfile", Aliases: []string{"f"}, Value: "/tmp/outfile", Usage: "Output file name"},
					&cli.StringFlag{Name: "compression", Aliases: []string{"c"}, Value: "none", Usage: "Compression type: gzip/lz4/snappy/zstd"},
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
					&cli.BoolFlag{Name: "verbose", Aliases: []string{"v"}, Usage: "Verbose mode"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					outFileName := c.String("outputfile")
					compressionType := c.String("compression")
					api := c.Bool("api")
					verbose := c.Bool("verbose")
					client.CompressionType = compressionType
					maxBytes := int32(1048576) // 1 MB
					t, exists := client.Meta.Topics[topic]
					if !exists {
						fmt.Printf("Topic %v doesn't exist\n", topic)
						os.Exit(1)
					}
					client.Verbose = verbose

					if !api {
						partitions := len(t.Partitions)
						client.Wg.Add(partitions)
						for partition := 0; partition < partitions; partition++ {
							fileName := fmt.Sprintf("%v.%v.%v", outFileName, topic, partition)
							fmt.Printf("Writing to file %v\n", fileName)
							file, err := os.Create(fileName)
							if err != nil {
								fmt.Printf("failed to create file: %v", err)
								os.Exit(1)
							}
							defer file.Close()
							go func(f *os.File) {
								defer client.Wg.Done()
								firstOffset, lastOffset, err := client.ListOffsets(topic, partition)
								if err != nil {
									fmt.Println("Error List Offsets:", err)
									os.Exit(1)
								}
								fetchOffset := firstOffset

								count := int64(0)

								for count < lastOffset {
									currCount, err := client.Fetch(topic, partition, fetchOffset, maxBytes, file, compressionType)
									if err != nil {
										fmt.Printf("Fetch failed: %v\n", err)
										os.Exit(1)
									}
									count += int64(currCount)
									fetchOffset += int64(currCount)
								}
							}(file)
						}
						client.Wg.Wait()
					} else {
						client.FetchFromAllPartitions(topic, compressionType)
					}
					return nil
				},
			},
			{
				Name:  "subscribegroup",
				Usage: "Subscribe to a group to listen for given topic",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.StringFlag{Name: "group", Aliases: []string{"g"}, Usage: "Group name"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					group := c.String("group")
					client.SubscribeGroup(topic, group)
					return nil
				},
			},
			{
				Name:  "listoffsets",
				Usage: "List offsets of all topics or a given topic",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Usage: "Topic name"},
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					api := c.Bool("api")
					if api {
						client.PrintOffsetsAPI(topic)
					} else {
						client.PrintOffsets(topic)
					}
					return nil
				},
			},
			{
				Name:  "listconfig",
				Usage: "List configuration of the client",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
				},
				Action: func(c *cli.Context) error {
					api := c.Bool("api")
					if api {
						client.PrintConfigAPI()
					} else {
						client.PrintConfig(true, true)
					}
					return nil
				},
			},
			{
				Name:  "listtopics",
				Usage: "List all topics",
				Flags: []cli.Flag{
					&cli.BoolFlag{Name: "api", Aliases: []string{"a"}, Value: false, Usage: "Use APIs"},
				},
				Action: func(c *cli.Context) error {
					api := c.Bool("api")
					if api {
						client.PrintTopicsAPI()
					} else {
						client.PrintTopics(false, true)
					}
					return nil
				},
			},
			{
				Name:  "listgroups",
				Usage: "List all consumer groups",
				Action: func(c *cli.Context) error {
					client.ListGroups()
					return nil
				},
			},
			{
				Name:  "deletetopic",
				Usage: "Delete a topic",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Required: true, Usage: "Topic name"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")

					fmt.Printf("Deleting topic %s ...\n", topic)
					err := client.DeleteTopic(topic)
					if err != nil {
						fmt.Printf("Error in deleting topic: %v, error: %v\n", topic, err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Name:  "deleterecord",
				Usage: "Delete record",
				Flags: []cli.Flag{
					&cli.StringFlag{Name: "topic", Aliases: []string{"t"}, Required: true, Usage: "Topic name"},
					&cli.IntFlag{Name: "partitionindex", Aliases: []string{"i"}, Usage: "Partition index"},
					&cli.Int64Flag{Name: "offset", Aliases: []string{"o"}, Usage: "offset to delete"},
				},
				Action: func(c *cli.Context) error {
					topic := c.String("topic")
					partitionIndex := c.Int("partitionindex")
					offset := c.Int64("offset")

					fmt.Printf("Deleting offset for topic %v, partition %v ...\n", topic, partitionIndex)
					err := client.DeleteRecord(topic, partitionIndex, offset)
					if err != nil {
						fmt.Printf("Error in deleting record: topic: %v, partition: %v, error: %v\n", topic, partitionIndex, err)
						os.Exit(1)
					}
					return nil
				},
			},
			{
				Action: func(c *cli.Context) error {
					cli.ShowAppHelp(c)
					return nil
				},
			},
		},
	}

	trie := internal.NewTrie()
	correctCommands := make(map[string]bool)

	for _, cmd := range app.Commands {
		trie.Insert(cmd.Name)
		correctCommands[cmd.Name] = true
	}

	// Check if there are any arguments
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		cmdArg := os.Args[1]

		// Check if it's an exact match first
		if correctCommands[cmdArg] {
			// It's a correct command, proceed with normal execution
			err := app.Run(os.Args)
			if err != nil {
				log.Fatal(err)
			}
			return
		}

		// If not an exact match, search for partial matches
		matches := trie.SearchPrefix(cmdArg)

		if len(matches) > 0 {
			fmt.Printf("Did you mean:\n")
			for _, match := range matches {
				fmt.Printf("  %s\n", match)
			}
			os.Exit(0)
		} else {
			fmt.Printf("Unknown command: %s\n", cmdArg)
			fmt.Println("Available commands:")
			for cmd := range correctCommands {
				fmt.Printf("  %s\n", cmd)
			}
			os.Exit(1)
		}
	}

	// If no arguments or the first argument is a flag, run the app normally
	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}

func connectTLS(brokerAddress, caCertPath string) (net.Conn, error) {
	caCert, err := ioutil.ReadFile(caCertPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read CA certificate: %v", err)
	}

	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)

	tlsConfig := &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
	}

	conn, err := tls.Dial("tcp", brokerAddress, tlsConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to establish TLS connection: %v", err)
	}

	return conn, nil
}
