package kafkaconsumer

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func CreateConsumer(broker int, group int, topics []string, message chan kafka.Event, error chan kafka.Event) (*kafka.Consumer, error) {

	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM)

	c, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers": broker,
		// Avoid connecting to IPv6 brokers:
		// This is needed for the ErrAllBrokersDown show-case below
		// when using localhost brokers on OSX, since the OSX resolver
		// will return the IPv6 addresses first.
		// You typically don't need to specify this configuration property.
		"broker.address.family": "v4",
		"group.id":              group,
		"session.timeout.ms":    6000,
		"auto.offset.reset":     "earliest"})

	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	fmt.Printf("Created Consumer %v\n", c)

	fmt.Printf("listening to the following topics %v", topics)

	err = c.SubscribeTopics(topics, nil)
	return c, err

	// for run {
	// 	select {
	// 	case sig := <-sigchan:
	// 		fmt.Printf("Caught signal %v: terminating\n", sig)
	// 		run = false
	// 	default:
	// 		ev := c.Poll(100)
	// 		if ev == nil {
	// 			continue
	// 		}

	// 		switch e := ev.(type) {
	// 		case *kafka.Message:
	// 			// fmt.Printf("%% Message on %s:\n%s\n",
	// 			// 	e.TopicPartition, string(e.Value))
	// 			// if e.Headers != nil {
	// 			// 	fmt.Printf("%% Headers: %v\n", e.Headers)
	// 			// }
	// 			message <- ev
	// 		case kafka.Error:
	// 			// Errors should generally be considered
	// 			// informational, the client will try to
	// 			// automatically recover.
	// 			// But in this example we choose to terminate
	// 			// the application if all brokers are down.
	// 			fmt.Fprintf(os.Stderr, "%% Error: %v: %v\n", e.Code(), e)
	// 			if e.Code() == kafka.ErrAllBrokersDown {
	// 				run = false
	// 			}
	// 		default:
	// 			fmt.Printf("Ignored %v\n", e)
	// 		}
	// 	}
	// }

	// fmt.Printf("Closing consumer\n")
	// c.Close()
}
