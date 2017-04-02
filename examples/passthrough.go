package examples

import "github.com/MickayG/kafkoi"


func main() {
	//Parse the command line arguments
	serviceConfig := kafkoi.ParseArgs()

	// Here's the logic that will happen between the Kafka topics.
	// In this example it just passes the message on. Resulting in Kafka messages being read from one topic
	// and onto another
	passthrough := func(message kafkoi.Message) (kafkoi.Message, bool) {
		return message, true
	}

	// Run KafKoi with the passthrough method and the config passed via command line arguments
	kafkoi.Run(passthrough, serviceConfig)
}
