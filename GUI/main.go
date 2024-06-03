package main

import (
	"context"
	"log"
	"sync"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/IBM/sarama"
)

var (
	messages []string
	mu       sync.Mutex
)

// ConsumerGroupHandler represents the Sarama consumer group handler for GUI
type ConsumerGroupHandler struct {
	messageList *widget.List
}

func (h *ConsumerGroupHandler) Setup(_ sarama.ConsumerGroupSession) error   { return nil }
func (h *ConsumerGroupHandler) Cleanup(_ sarama.ConsumerGroupSession) error { return nil }
func (h *ConsumerGroupHandler) ConsumeClaim(session sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	for message := range claim.Messages() {
		mu.Lock()
		messages = append(messages, string(message.Value))
		mu.Unlock()
		h.messageList.Refresh()
		session.MarkMessage(message, "")
	}
	return nil
}

func initKafka(consumerGroupID string, messageList *widget.List) sarama.ConsumerGroup {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumerGroup, err := sarama.NewConsumerGroup([]string{"localhost:9092"}, consumerGroupID, config)
	if err != nil {
		log.Fatalf("failed to start Kafka consumer group: %v", err)
	}

	ctx := context.Background()
	handler := &ConsumerGroupHandler{messageList: messageList}
	go func() {
		for {
			if err := consumerGroup.Consume(ctx, []string{"device_states"}, handler); err != nil {
				log.Printf("Error from consumer: %v", err)
			}
		}
	}()

	return consumerGroup
}

func main() {
	a := app.New()
	w := a.NewWindow("Kafka Messages")
	w.Resize(fyne.NewSize(400, 300))

	messageList := widget.NewList(
		func() int {
			mu.Lock()
			defer mu.Unlock()
			return len(messages)
		},
		func() fyne.CanvasObject {
			return widget.NewLabel("")
		},
		func(i widget.ListItemID, o fyne.CanvasObject) {
			mu.Lock()
			defer mu.Unlock()
			o.(*widget.Label).SetText(messages[i])
		},
	)

	// Initialize Kafka consumer for GUI
	initKafka("gui-consumer-group", messageList)

	scrollContainer := container.NewVScroll(messageList)
	scrollContainer.SetMinSize(fyne.NewSize(400, 250))

	w.SetContent(container.NewVBox(
		widget.NewLabel("Kafka Messages from 'device_states' topic"),
		scrollContainer,
	))

	w.ShowAndRun()
}
