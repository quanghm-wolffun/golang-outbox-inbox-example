package main

import (
	"encoding/json"
	"io"
	"log"
	"os"
	"os/signal"
	"outbox/cmd/worker/handlers"
	"outbox/cmd/worker/inbox"
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

	db, err := database.NewConnection()
	if err != nil {
		log.Fatal("error connecting to db: ", err)
	}

	if err := db.AutoMigrate(&shared.InboxMessage{}); err != nil {
		log.Fatal("migrate error - ", err)
	}

	customerHandler := &handlers.CustomerHandler{}
	inboxProcessor := inbox.Processor{
		DB:      db,
		Handler: customerHandler,
	}

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

	exchangeName := "outbox_events"

	queueName := "worker2_queue_" + uuid.NewString()[0:8]
	q, err := ch.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to declare queue: %v", err)
	}

	err = ch.QueueBind(
		q.Name,
		"",
		exchangeName,
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %v", err)
	}

	log.Printf("Worker 2 created queue %s and bound to exchange %s", q.Name, exchangeName)

	c := cron.New()
	_, err = c.AddFunc("@every 10s", inboxProcessor.ProcessMessages)
	if err != nil {
		log.Fatal("register inbox processor error", err)
	}
	c.Start()
	defer c.Stop()

	messages, err := ch.Consume(
		q.Name,
		"",
		false,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		log.Fatal("Failed to register a consumer", err)
	}

	go func() {
		for m := range messages {
			var evt OutboxEvent
			if err := json.Unmarshal(m.Body, &evt); err != nil {
				log.Println("Handle message error: ", string(m.Body))
				log.Println("ERR:", err)
				m.Nack(false, true) // Không xóa message, requeue lại
				continue
			}

			if err := inboxProcessor.SaveMessage(evt.EventName, evt.Payload); err != nil {
				log.Printf("Failed to save message to inbox: %v\n", err)
				m.Nack(false, true) // Không xóa message, requeue lại
				continue
			}

			log.Printf("Worker 2 received [%s] - Payload: '%s' and saved to inbox", evt.EventName, evt.Payload)
			m.Ack(false)
		}
	}()

	kill := make(chan os.Signal, 1)
	signal.Notify(kill, syscall.SIGINT, syscall.SIGTERM)
	<-kill
}

func closeConnection(c io.Closer) {
	if err := c.Close(); err != nil {
		log.Println("Error closing connection:", err)
	}
}
