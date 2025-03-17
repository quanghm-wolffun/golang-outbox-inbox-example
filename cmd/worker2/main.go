package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"os/signal"
	"outbox/cmd/worker/handlers"
	"outbox/cmd/worker2/inbox"
	"outbox/database"
	"outbox/queue"
	"outbox/shared"
	"syscall"

	"github.com/google/uuid"
	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
	"gorm.io/datatypes"
)

type OutboxEvent struct {
	ID        string         `json:"id"`
	EventName string         `json:"event_name"`
	Payload   datatypes.JSON `json:"payload"`
}

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("loading env file: ", err)
	}

	// Connect to database
	db, err := database.NewConnection()
	if err != nil {
		log.Fatal("error connecting to db: ", err)
	}

	// Auto-migrate the inbox table
	if err := db.AutoMigrate(&shared.InboxMessage{}); err != nil {
		log.Fatal("migrate error - ", err)
	}

	// Initialize handlers
	customerHandler := &handlers.CustomerHandler{}

	// Create inbox processor
	inboxProcessor := inbox.Processor{
		DB:      db,
		Handler: customerHandler,
	}

	// Create RabbitMQ connection
	conn, err := queue.CreateConnection()
	if err != nil {
		log.Fatal(err)
	}
	defer closeConnection(conn)

	ch, err := queue.CreateChannel(conn)
	if err != nil {
		log.Fatal(err)
	}
	defer closeConnection(ch)

	// Declare the fanout exchange to match relay service
	exchangeName := "outbox_events"
	err = ch.ExchangeDeclare(
		exchangeName, // name
		"fanout",     // type
		true,         // durable
		false,        // auto-deleted
		false,        // internal
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare exchange: %v", err)
	}

	// Create a unique queue for this worker
	queueName := "worker2_queue_" + uuid.NewString()[0:8]
	q, err := ch.QueueDeclare(
		queueName, // unique name for this worker
		false,     // durable
		true,      // delete when unused
		false,     // exclusive
		false,     // no-wait
		nil,       // arguments
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	// Bind the queue to the exchange
	err = ch.QueueBind(
		q.Name,       // queue name
		"",           // routing key - empty for fanout
		exchangeName, // exchange
		false,        // no-wait
		nil,          // arguments
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}

	log.Printf("Created queue %s and bound to exchange %s", q.Name, exchangeName)

	// Start a cron job to process inbox messages
	c := cron.New()
	_, err = c.AddFunc("@every 10s", inboxProcessor.ProcessMessages)
	if err != nil {
		log.Fatal("register inbox processor error", err)
	}
	c.Start()
	defer c.Stop()

	// Consume messages from RabbitMQ and save to inbox
	messages, err := ch.Consume(
		q.Name,
		"",    // consumer
		true,  // auto-ack
		false, // exclusive
		false, // no-local
		false, // no-wait
		nil,   // args
	)
	if err != nil {
		log.Fatal("Failed to register a consumer", err)
	}

	// Run in background
	go func() {
		log.Printf("Worker2 consuming from queue [%s] bound to exchange [%s]\n", q.Name, exchangeName)
		for m := range messages {
			var evt OutboxEvent
			if err := json.Unmarshal(m.Body, &evt); err != nil {
				log.Println("Handle message error: ", string(m.Body))
				log.Println("ERR:", err)
				continue
			}

			// Save message to inbox
			if err := inboxProcessor.SaveMessage(evt.EventName, evt.Payload); err != nil {
				log.Printf("Failed to save message to inbox: %v\n", err)
				continue
			}

			log.Printf("Worker2 received [%s] - Payload: '%s' and saved to inbox", evt.EventName, evt.Payload)
		}
	}()

	// Wait for terminated signal
	kill := make(chan os.Signal, 1)
	signal.Notify(kill, syscall.SIGINT, syscall.SIGTERM)
	<-kill
}

func closeConnection(c io.Closer) {
	err := c.Close()
	if err != nil {
		log.Println(err)
	}
}
