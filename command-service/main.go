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

// Command represents a command to change device state
type Command struct {
	DeviceID int `json:"DeviceID"` // Ensure the JSON tag matches the JSON field name
	Cmd      int `json:"Cmd"`      // Ensure the JSON tag matches the JSON field name
}

type Ack struct {
	DeviceID int    `json:"device_id"`
	Status   string `json:"status"` // "ack" or "nak"
}

var db *gorm.DB

func initDB() {
	dsn := "host=localhost user=myuser password=mypassword dbname=alarm_service port=5432"
	var err error
	db, err = gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		log.Fatalf("failed to connect database: %v", err)
	}

	db.AutoMigrate(&Command{})
}

func handleCommand(c *gin.Context) {
	var cmd Command
	if err := c.ShouldBindJSON(&cmd); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	// Determine the new state based on cmd
	newState := "off"
	if cmd.Cmd == 1 {
		newState = "on"
	}

	// Prepare the data to send to the device service
	deviceData := map[string]interface{}{
		"DeviceID": cmd.DeviceID,
		"State":    newState,
	}
	jsonData, _ := json.Marshal(deviceData)

	// Log data being sent to device service
	log.Printf("Sending data to device service: %s", jsonData)

	// Call the device service to update the state
	deviceServiceURL := "http://localhost:8080/updateDeviceState"
	resp, err := http.Post(deviceServiceURL, "application/json", bytes.NewBuffer(jsonData))
	if err != nil {
		log.Printf("Error calling device service: %v", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update device state"})
		return
	}

	if resp.StatusCode != http.StatusOK {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "Failed to update device state"})
		return
	}

	log.Printf("Command sent to device service: DeviceID=%d, NewState=%s", cmd.DeviceID, newState)
	c.JSON(http.StatusOK, gin.H{"message": "Command sent, waiting for device to update"})
}

// handleAck handles ACK from the device
func handleAck(c *gin.Context) {
	var ack Ack
	if err := c.ShouldBindJSON(&ack); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}

	if ack.Status != "ack" && ack.Status != "nak" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "Invalid status"})
		return
	}

	// Log the ACK/NAK message
	log.Printf("Received %s from device %d", ack.Status, ack.DeviceID)
	c.JSON(http.StatusOK, gin.H{"message": "ACK/NAK received"})
}

func main() {
	initDB()

	r := gin.Default()
	r.POST("/command", handleCommand)
	r.POST("/ack", handleAck) // Endpoint for device to send ACK

	if err := r.Run(":8083"); err != nil {
		log.Fatalf("failed to run server: %v", err)
	}
}
