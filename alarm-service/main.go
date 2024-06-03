package main

import (
	"context"
	"github.com/IBM/sarama"
	"log"
	"strings"
)

// ConsumerGroupHandler represents the Sarama consumer group handler for Alarm
type ConsumerGroupHandler struct{}

func (ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		msg := string(message.Value)

		// Parse the message to extract the State value
		state := extractState(msg)
		if state == "off" {
			log.Printf("Alarm: A device is off! Details: %s", msg)
		}

		session.MarkMessage(message, "")
	}
	return nil
}

func extractState(msg string) string {
	// Example message: "State ID: 68, Device ID: 3, State: On"
	parts := strings.Split(msg, ",")
	for _, part := range parts {
		if strings.Contains(part, "State:") {
			state := strings.TrimSpace(strings.Split(part, ":")[1])
			return state
		}
	}
	return ""
}

func initKafka(consumerGroupID string) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, consumerGroupID, config)
	if err != nil {
		log.Fatalf("failed to start Kafka consumer group: %v", err)
	}

	ctx := context.Background()
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{"device_states"}, ConsumerGroupHandler{}); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
		}
	}()

	return consumerGroup
}

func main() {
	// Initialize Kafka consumer for Alarm
	initKafka("alarm-consumer-group")

	// Prevent the main function from exiting
	select {}
}
