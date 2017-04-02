package kafkoi

import "testing"

func Passthrough(message Message) (Message, bool) {
	return message, true
}

func ManualTestLaunch(t *testing.T) {
	var config ServiceConfig
	config.OutputTopic = "testoutput"
	config.InputTopic = "testinput"
	config.BrokerArray = []string{"192.168.0.2:9092"}
	config.ConsumerGroup = createUUID()

	Run(Passthrough, config)
}

