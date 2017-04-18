[![GoDoc](https://godoc.org/github.com/MickayG/kafkoi?status.png)](https://godoc.org/github.com/MickayG/kafkoi)


KafKoi
======

A very simple, **work-in-progress**, lightweight ETL framework which reads to and from Kafka.

### Using this library

I don't recommend it. This is my first foray into Go.

If you've reviewed the code, find no faults and think it's super awesome, import it.

```go
import "github.com/MickayG/kafkoi"
```

### Example

```go
	//Parse the command line arguments (see documentation on method for what they are)
	serviceConfig := kafkoi.ParseArgs()

	// Here's the logic that will happen between the Kafka topics.
	// In this example it just forwards the message on. Resulting in Kafka messages being read from one topic
	// and written onto another
	passthrough := func(message kafkoi.Message) (kafkoi.Message, bool) {
		return message, true
	}

	// Run KafKoi with the passthrough method and the config passed via command line arguments
	kafkoi.Run(passthrough, serviceConfig)
```
