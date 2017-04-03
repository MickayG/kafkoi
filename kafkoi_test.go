package kafkoi

import (
	"testing"
	log "github.com/Sirupsen/logrus"
)

func Passthrough(message Message) (Message, bool) {
	return message, true
}

func TestManual(t *testing.T) {
	log.SetLevel(log.DebugLevel)
	config := NewServiceConfig()
	config.OutputTopic = "multipleoutput"
	config.InputTopic = "multipleinput"
	config.BrokerArray = []string{"192.168.0.2:9092"}
	config.ConsumerGroup = createUUID()

	Run(Passthrough, config)
}

