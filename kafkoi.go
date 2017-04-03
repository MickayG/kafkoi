package kafkoi

import (
	"flag"
	"os"
	"strings"

	// Standard kafka client
	"github.com/Shopify/sarama"

	// Adds clustering abilities such as offset tracking
	cluster "github.com/bsm/sarama-cluster"

	log "github.com/Sirupsen/logrus"
	"os/signal"
	"encoding/hex"
)

type PartitionerType int
const (
	// From Sarama (kafka library) docs:
	// If the message's key is nil then a random partition is chosen. Otherwise the FNV-1a hash of the encoded bytes of the message key is used, modulus the number of partitions. This ensures that messages with the same key always end up on the same partition.
	HashPartitioner PartitionerType = iota

	// From Sarama (kafka library) docs:
	// Randomly chooses a random partition each time
	RandomPartitioner PartitionerType = iota

	//From Sarama (kafka library) docs:
	// Walk through the available partitions one at a time
	RoundRobinPartitioner PartitionerType = iota
)

// The configuration for the Kafka input and output
type ServiceConfig struct {
	BrokerArray     []string
	InputTopic      string
	OutputTopic     string
	ConsumerGroup   string
	BatchOffsetSize int
	Partitioner     PartitionerType
}

// Represents a Kafka message to be read or sent
type Message struct {
	Key []byte
	Value []byte
}


// Method to be implemented to do the transform.
// Returns the message and a boolean which indicates whether the message should be written or not.
// Ie. if bool = true, message will be written, if false, it will not be written to the output topic
type Transform func(Message) (Message, bool)

// Returns a new ServiceConfig with default values set
func NewServiceConfig() ServiceConfig {
	var config ServiceConfig
	config.ConsumerGroup = createUUID()
	config.BatchOffsetSize = 1000
	config.Partitioner = RoundRobinPartitioner
	return config
}

// Parse command line arguements, returning a ServiceConfig for the run method.
// Arguments:
//    broker-list: Comma delimited list of brokers
//    input-topic: Input topic, where data is read from
//    output-topic: Output topic, where data is written to, after being transformed
//    (Optional) consumer-group: Name of the consumer group. If not set, the group will be a random UUID
//    (Optional) Size of offset batches, written to Kafka. Larger sizes increases the chance of re-reading data in the event
//      of failure, however the larger the size, the more efficient offset storage is.
func ParseArgs() ServiceConfig {

	broker_list := flag.String("broker-list", "", "Comma delimited list of brokers. E.g 'broker1:9092,broker2:9092'")
	input_topic := flag.String("input-topic", "", "Name of the input topic")
	output_topic := flag.String("output-topic", "", "Name of the output topic")
	consumer_group := flag.String("consumer-group", createUUID(), "Optional name of the consumer-group. If not set then a UUID will be generated")
	offset_batch_size := flag.Int("offset-batch-size", 1000, "Optional size off consumer offsets to batch write. If not set, defaults to 1000")

	flag.Parse()

	checkFlagSet("broker-list", broker_list)
	checkFlagSet("input-topic", input_topic)
	checkFlagSet("output-topic", output_topic)
	broker_array := strings.Split(*broker_list, ",")

	var config ServiceConfig
	config.BrokerArray = broker_array
	config.InputTopic = *input_topic
	config.OutputTopic = *output_topic
	config.ConsumerGroup = *consumer_group
	config.BatchOffsetSize = *offset_batch_size

	return config
}

// Start the microservice, reading and writing to Kafka with regards to the ServiceConfig.
// The transform argument should be implemented to transform the data between reading & writing.
func Run(transform Transform, config ServiceConfig){
	consumer := create_consumer(config)
	producer := create_producer(config)

	// When the application finishes, remember to close the consumer and producer
	defer consumer.Close()
	defer (*producer).Close()

	// trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	RunTransform(transform, consumer, producer, config.OutputTopic, config.BatchOffsetSize, signals)
}

// Run the transform with the instantiated consumer and producer
func RunTransform(transform Transform, consumer *cluster.Consumer, producer *sarama.AsyncProducer, outputTopic string, batchOffsetSize int, signals chan os.Signal) {
	// This will be used to store any offsets
	offsets := cluster.NewOffsetStash()

	messages := consumer.Messages()
	for {
		select {
		case msg, more := <- messages:
			if more {
				// We don't want to encode the key & value unless we're actually going to log something
				if log.GetLevel() <= log.DebugLevel {
					log.WithFields(log.Fields{"key": hex.EncodeToString(msg.Key), "value": hex.EncodeToString(msg.Value)}).Debug("Recv")
				}

				var message Message
				message.Key = msg.Key
				message.Value = msg.Value

				output, write := transform(message)
				if write {
					(*producer).Input() <- &sarama.ProducerMessage{
						Topic: outputTopic,
						Key:   sarama.ByteEncoder(output.Key),
						Value: sarama.ByteEncoder(output.Value)}

					if log.GetLevel() <= log.DebugLevel {
						log.WithFields(log.Fields{"key": hex.EncodeToString(output.Key), "value": hex.EncodeToString(output.Value)}).Debug("Send")
					}
				}

				// TODO Can do at least once? Is this guarenteed with the close signal.. probably not
				offsets.MarkOffset(msg, "")

				offsetsSize := len(offsets.Offsets())
				if offsetsSize > batchOffsetSize {
					// When we reach the desired offset size, launch a thread to write the offsets
					go StoreOffsets(offsets, consumer)
				}
			}
		case err, more := <-consumer.Errors():
			if more {
				log.Error("Error: %s\n", err.Error())
			}
		case ntf, more := <-consumer.Notifications():
			if more {
				log.Info("Notification: %+v\n", ntf)
			}
		case <-signals:
			// Ensure we store all the offsets on application close
			if len(offsets.Offsets()) > 0{
				StoreOffsets(offsets, consumer)
			}
			return
		}
	}
}
func StoreOffsets(offsets *cluster.OffsetStash, consumer *cluster.Consumer) {
		offsetsSize := len(offsets.Offsets())
		consumer.MarkOffsets(offsets)
		log.WithField("offsetSize", offsetsSize).Debug("Stored offsets")
}


// Create a consumer with the ServiceConfig.
// If the consumer fails to load, application will panic
func create_consumer(config ServiceConfig) *cluster.Consumer {
	consumerConfig := cluster.NewConfig()
	consumerConfig.Consumer.Return.Errors = true
	consumerConfig.Group.Return.Notifications = true

	consumer, err := cluster.NewConsumer(config.BrokerArray, config.ConsumerGroup, []string{config.InputTopic}, consumerConfig)

	if err != nil {
		panic(err)
	}

	log.WithFields(log.Fields{
		"brokers": config.BrokerArray,
		"consumer-group": config.ConsumerGroup,
		"input-topic": config.InputTopic}).Info("Created consumer")

	return consumer
}

// Create a producer with the ServiceConfig.
// If the producer fails to load, application will panic
func create_producer(config ServiceConfig) *sarama.AsyncProducer {
	producerConfig := sarama.NewConfig()
	producerConfig.Producer.Return.Successes = false

	switch config.Partitioner {
	case RandomPartitioner:
		producerConfig.Producer.Partitioner = sarama.NewRandomPartitioner
		log.WithField("Partitioner", "random").Info("Set producer partitioner")
	case RoundRobinPartitioner:
		producerConfig.Producer.Partitioner = sarama.NewRoundRobinPartitioner
		log.WithField("Partitioner", "roundrobin").Info("Set producer partitioner")
	case HashPartitioner:
		producerConfig.Producer.Partitioner = sarama.NewHashPartitioner
		log.WithField("Partitioner", "hash").Info("Set producer partitioner")
	default:
		log.WithField("Partitioner", config.Partitioner).Error("Unknown partitioner type")
	}

	producer, err := sarama.NewAsyncProducer(config.BrokerArray, producerConfig)
	if err != nil {
		log.Panic(err)
	}

	log.WithFields(log.Fields{
		"brokers": config.BrokerArray,
	}).Info("Created producer")

	return &producer
}





































