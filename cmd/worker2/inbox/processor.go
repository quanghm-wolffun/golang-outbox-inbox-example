package inbox

import (
	"errors"
	"log"
	"outbox/shared"
	"time"

	"gorm.io/datatypes"
	"gorm.io/gorm"
)

const (
	_consumerName = "worker2"
)

// MessageHandler interface defines how to handle different types of messages
type MessageHandler interface {
	HandleMessage(eventName string, payload datatypes.JSON) error
}

// Processor processes incoming messages from the inbox
type Processor struct {
	DB      *gorm.DB
	Handler MessageHandler
}

// SaveMessage saves a message to the inbox with idempotency checks
func (p *Processor) SaveMessage(eventName string, payload datatypes.JSON) error {
	// Generate a deterministic message ID based on event content
	contentHash := shared.GenerateContentHash(eventName, _consumerName, payload)

	// First check if the message already exists
	var existing shared.InboxMessage
	result := p.DB.Where("id = ?", contentHash).First(&existing)

	// If found, it's a duplicate
	if result.Error == nil {
		log.Printf("Duplicate message detected with ID: %s", contentHash)
		return nil
	}

	// If error is not "record not found", it's a database error
	if !errors.Is(result.Error, gorm.ErrRecordNotFound) {
		return result.Error
	}

	// The Message doesn't exist, create it
	inboxMessage := shared.InboxMessage{
		ID:              contentHash,
		EventName:       eventName,
		Payload:         payload,
		IsProcessed:     false,
		ProcessingCount: 0,
	}

	return p.DB.Create(&inboxMessage).Error
}

// ProcessMessages processes pending messages in the inbox
func (p *Processor) ProcessMessages() {
	var messages []shared.InboxMessage

	// Find unprocessed messages with retry limit
	err := p.DB.Where("is_processed = ? AND processing_count < 3", false).
		Order("first_attempt_at ASC").
		Limit(10).
		Find(&messages).Error

	if err != nil {
		log.Println("Error fetching inbox messages:", err)
		return
	}

	if len(messages) == 0 {
		return
	}

	for _, msg := range messages {
		now := time.Now()
		updateFields := map[string]interface{}{
			"processing_count": msg.ProcessingCount + 1,
			"last_attempt_at":  now,
		}

		if msg.FirstAttemptAt == nil {
			updateFields["first_attempt_at"] = now
		}

		// Update a message to increment processing count
		if err := p.DB.Model(&shared.InboxMessage{}).
			Where("id = ?", msg.ID).
			Updates(updateFields).Error; err != nil {
			log.Println("Error updating inbox message:", err)
			continue
		}

		// Process the message
		if err := p.Handler.HandleMessage(msg.EventName, msg.Payload); err != nil {
			log.Printf("Failed to process inbox message %s: %v\n", msg.ID, err)
			continue
		}

		// Mark as processed
		now = time.Now()
		if err := p.DB.Model(&shared.InboxMessage{}).
			Where("id = ?", msg.ID).
			Updates(map[string]interface{}{
				"is_processed": true,
				"processed_at": now,
			}).Error; err != nil {
			log.Println("Error marking inbox message as processed:", err)
		} else {
			log.Printf("Successfully processed inbox message: %s\n", msg.ID)
		}
	}
}
