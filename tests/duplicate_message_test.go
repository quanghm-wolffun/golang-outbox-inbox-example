package tests

import (
	"encoding/json"
	"fmt"
	"log"
	"outbox/customer"
	"outbox/database"
	"outbox/queue"
	"outbox/shared"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/streadway/amqp"
	"github.com/stretchr/testify/assert"
	"gorm.io/datatypes"
)

func TestDuplicateMessages(t *testing.T) {
	// Load environment variables
	if err := godotenv.Load("../.local.env"); err != nil {
		t.Fatal("Error loading .env file:", err)
	}

	// Connect to database
	db, err := database.NewConnection()
	if err != nil {
		t.Fatal("Error connecting to database:", err)
	}

	// Clean up tables before test
	log.Println("Cleaning up tables before test...")
	db.Exec("DELETE FROM inbox_messages")
	db.Exec("DELETE FROM out_box_messages")

	// Connect to RabbitMQ
	conn, err := queue.CreateConnection()
	if err != nil {
		t.Fatal("Error connecting to RabbitMQ:", err)
	}
	defer conn.Close()

	ch, err := queue.CreateChannel(conn)
	if err != nil {
		t.Fatal("Error creating RabbitMQ channel:", err)
	}
	defer ch.Close()

	exchangeName := "outbox_events"
	queueName := "test_worker_queue"

	_, err = ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		t.Fatal("Error declaring queue:", err)
	}

	// Bind queue to exchange
	err = ch.QueueBind(
		queueName,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		t.Fatal("Error binding queue:", err)
	}

	// Purge the queue before testing
	_, err = ch.QueuePurge(queueName, false)
	if err != nil {
		t.Fatal("Error purging queue:", err)
	}
	log.Println("Queue purged successfully")

	// Create a test customer
	customerId := uuid.NewString()
	customerData := customer.Customer{
		ID:        customerId,
		Email:     "test@example.com",
		Name:      "Test User",
		CreatedAt: time.Now(),
	}

	// Convert customer to JSON
	customerJSON, err := json.Marshal(customerData)
	if err != nil {
		t.Fatal("Error marshaling customer:", err)
	}

	// Create a test message
	testMessage := shared.OutBoxMessage{
		ID:          uuid.NewString(),
		EventName:   "CustomerCreated",
		Payload:     datatypes.JSON(customerJSON),
		IsProcessed: false,
	}

	// Serialize the message
	messageJSON, err := json.Marshal(testMessage)
	if err != nil {
		t.Fatal("Error marshaling message:", err)
	}

	// Send the same message 3 times
	for i := 0; i < 3; i++ {
		err = ch.Publish(
			exchangeName,
			"",
			false,
			false,
			amqp.Publishing{
				ContentType: "application/json",
				Body:        messageJSON,
			},
		)
		if err != nil {
			t.Fatal("Error publishing message:", err)
		}
		log.Printf("Sent duplicate message #%d: %s", i+1, string(messageJSON))
		time.Sleep(100 * time.Millisecond)
	}

	// Wait for messages to be processed
	log.Println("Waiting for messages to be processed...")
	time.Sleep(15 * time.Second)

	// Check the inbox table to verify duplicates were handled correctly
	var inboxMessages []shared.InboxMessage
	if err := db.Where("event_name = ?", "CustomerCreated").Find(&inboxMessages).Error; err != nil {
		t.Fatal("Error querying inbox messages:", err)
	}

	log.Printf("Found %d inbox messages", len(inboxMessages))
	for i, msg := range inboxMessages {
		log.Printf("  Message %d: ID=%s, Processed=%t", i+1, msg.ID, msg.IsProcessed)
	}

	// Count processed messages
	var processedCount int64
	if err := db.Model(&shared.InboxMessage{}).
		Where("event_name = ? AND is_processed = ?", "CustomerCreated", true).
		Count(&processedCount).Error; err != nil {
		t.Fatal("Error counting processed messages:", err)
	}

	// Assert that exactly one message was processed (idempotent processing)
	assert.Equal(t, int64(1), processedCount, "Exactly one message should have been marked as processed")

	// Clean up after test
	log.Println("Cleaning up tables after test...")
	db.Exec("DELETE FROM inbox_messages")
	db.Exec("DELETE FROM out_box_messages")

	fmt.Println("Test completed successfully. Duplicate messages were properly handled.")
}
