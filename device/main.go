package main

import (
	"bytes"
	"encoding/json"
	"log"
	"net/http"

	"github.com/gin-gonic/gin"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Device represents a device with a state.
type Device struct {
	DeviceID int    `gorm:"primaryKey"`
	State    string `gorm:"type:varchar(255)"`
}

var db *gorm.DB

func initDB() {
	dsn := "host=localhost user=myuser password=mypassword dbname=alarm_service port=5432"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	db.AutoMigrate(&Device{})
}

// Ack represents the acknowledgment message
type Ack struct {
	DeviceID int    `json:"device_id"`
	Status   string `json:"status"` // "ack" or "nak"
}

func updateDeviceState(c *gin.Context) {
	var device Device
	if err := c.ShouldBindJSON(&device); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if err := db.Save(&device).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}

	// Notify State Service
	stateServiceURL := "http://localhost:8081/updateState"
	stateData := map[string]interface{}{
		"DeviceID": device.DeviceID,
		"State":    device.State,
	}
	jsonData, _ := json.Marshal(stateData)
	resp, err := http.Post(stateServiceURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil || resp.StatusCode != http.StatusOK {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to notify state service"})
		return
	}

	c.JSON(http.StatusOK, gin.H{"message": "Device state updated successfully"})

	// send ACK to command service
	sendAck(device.DeviceID, "ack")
}

func sendAck(deviceID int, status string) {
	commandServiceURL := "http://localhost:8083/ack"
	ackData := Ack{
		DeviceID: deviceID,
		Status:   status,
	}
	jsonData, _ := json.Marshal(ackData)
	_, err := http.Post(commandServiceURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Failed to send ACK to command service: %v", err)
	}
}

func main() {
	initDB()

	r := gin.Default()
	r.POST("/updateDeviceState", updateDeviceState)

	if err := r.Run(":8080"); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
