package main

import (
	"fmt"
	"log"
	"net/http"

	"github.com/IBM/sarama"
	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// State represents the state of a device.
type State struct {
	StateID  int    `gorm:"primaryKey"`
	DeviceID int    `gorm:"index"` // Relationship with Device
	State    string `gorm:"type:varchar(255)"`
}

var (
	db            *gorm.DB
	kafkaProducer sarama.SyncProducer
)

func initDB() {
	dsn := "host=localhost user=myuser password=mypassword dbname=alarm_service port=5432"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	db.AutoMigrate(&State{})
}

func initKafka() {
	config := sarama.NewConfig()
	// Require 5 acks for the producer to consider a message as successfully sent
	config.Producer.RequiredAcks = sarama.WaitForAll
	// Retry up to 5 times to send a message
	config.Producer.Retry.Max = 5
	// Ensure that the producer returns the result of the messages sent successfully
	config.Producer.Return.Successes = true

	var err error
	kafkaProducer, err = sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to start Kafka producer: %v", err)
	}
}

func updateState(c *gin.Context) {
	var state State
	if err := c.ShouldBindJSON(&state); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Create a new state entry in the database
	if err := db.Create(&state).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Send message to Kafka
	message := fmt.Sprintf("State ID: %d, Device ID: %d, State: %s", state.StateID, state.DeviceID, state.State)
	kafkaMessage := &sarama.ProducerMessage{
		Topic: "device_states",
		Value: sarama.StringEncoder(message),
	}
	_, _, err := kafkaProducer.SendMessage(kafkaMessage)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "State updated successfully"})
}

func main() {
	initDB()
	initKafka()

	r := gin.Default()
	r.POST("/updateState", updateState)

	if err := r.Run(":8081"); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
