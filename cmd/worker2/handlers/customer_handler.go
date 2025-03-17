package handlers

import (
	"encoding/json"
	"log"
	"outbox/customer"

	"gorm.io/datatypes"
)

type CustomerHandler struct{}

func (h *CustomerHandler) HandleMessage(eventName string, payload datatypes.JSON) error {
	switch eventName {
	case "CustomerCreated":
		return h.handleCustomerCreated(payload)
	// Add other customer event types as needed
	default:
		log.Printf("Unknown customer event: %s\n", eventName)
		return nil
	}
}

func (h *CustomerHandler) handleCustomerCreated(payload datatypes.JSON) error {
	var customer customer.Customer
	if err := json.Unmarshal(payload, &customer); err != nil {
		return err
	}

	// Do something with the customer data
	log.Printf("Processing CustomerCreated event: Customer ID=%s, Name=%s, Email=%s\n",
		customer.ID, customer.Name, customer.Email)

	// Here you would implement your actual business logic,
	// For example, send welcome email, notify other services, etc.

	return nil
}
