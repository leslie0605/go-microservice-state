package main

import (
	"fyne.io/fyne/v2"
	"log"
	"sync"

	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/widget"
	"github.com/IBM/sarama"
)

var (
	messages []string
	mu       sync.Mutex
)

func initKafka() sarama.Consumer {
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	consumer, err := sarama.NewConsumer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start Kafka consumer: %v", err)
	}

	return consumer
}

func consumeMessages(consumer sarama.Consumer, messageList *widget.List) {
	partitionConsumer, err := consumer.ConsumePartition("device_states", 0, sarama.OffsetNewest)
	if err != nil {
		log.Fatalf("failed to start partition consumer: %v", err)
	}

	go func() {
		for message := range partitionConsumer.Messages() {
			mu.Lock()
			messages = append(messages, string(message.Value))
			mu.Unlock()
			messageList.Refresh()
		}
	}()

	go func() {
		for err := range partitionConsumer.Errors() {
			log.Printf("error consuming messages: %v", err)
		}
	}()
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

	consumer := initKafka()
	consumeMessages(consumer, messageList)

	scrollContainer := container.NewVScroll(messageList)
	scrollContainer.SetMinSize(fyne.NewSize(400, 250))

	w.SetContent(container.NewVBox(
		widget.NewLabel("Kafka Messages from 'device_states' topic"),
		scrollContainer,
	))

	w.ShowAndRun()
}
