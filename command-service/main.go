package main

import (
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

// Command represents a command to change device state
type Command struct {
	DeviceID int `json:"device_id"`
	Cmd      int `json:"cmd"` // 0: turn off, 1: turn on
}

type Ack struct {
	DeviceID int    `json:"device_id"`
	Status   string `json:"status"` // "ack" or "nak"
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

	// Update the device state in the database
	var device Device
	if err := db.First(&device, cmd.DeviceID).Error; err != nil {
		if err == gorm.ErrRecordNotFound {
			// Create new device if not found
			device = Device{DeviceID: cmd.DeviceID, State: newState}
			if err := db.Create(&device).Error; err != nil {
				c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
				return
			}
		} else {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	} else {
		// Update the existing device state
		if err := db.Model(&device).Update("state", newState).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
	}

	// Here, we assume that the device will change state and send an ACK to /ack endpoint
	c.JSON(http.StatusOK, gin.H{"message": "Command received, waiting for device to update"})
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
