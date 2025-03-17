package shared

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"gorm.io/datatypes"
)

type InboxMessage struct {
	ID              string         `gorm:"primaryKey" json:"id"`
	EventName       string         `gorm:"event_name" json:"event_name"`
	Payload         datatypes.JSON `gorm:"payload" json:"payload"`
	IsProcessed     bool           `gorm:"is_processed" json:"is_processed"`
	ProcessingCount int            `gorm:"processing_count" json:"processing_count"`
	FirstAttemptAt  *time.Time     `gorm:"first_attempt_at" json:"first_attempt_at"`
	LastAttemptAt   *time.Time     `gorm:"last_attempt_at" json:"last_attempt_at"`
	ProcessedAt     *time.Time     `gorm:"processed_at" json:"processed_at"`
}

// GenerateContentHash creates a deterministic hash from event content
// This helps with idempotency by giving identical messages the same ID
func GenerateContentHash(eventName, consumerName string, payload datatypes.JSON) string {
	// Extract key identifiers from payload based on an event type
	var identifier string

	switch eventName {
	case "CustomerCreated":
		var customer map[string]interface{}
		if err := json.Unmarshal(payload, &customer); err == nil {
			if id, ok := customer["id"].(string); ok {
				fmt.Println("Customer id:", id)
				identifier = id
			}
		}
	// Add cases for other event types
	default:
		identifier = string(payload)
	}

	// Create hash from event name, consumer name, and content identifier
	hasher := sha256.New()
	hasher.Write([]byte(eventName + consumerName + identifier))
	return hex.EncodeToString(hasher.Sum(nil))
}
