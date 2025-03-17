package main

import (
	"io"
	"log"
	"os"
	"os/signal"
	"outbox/database"
	"outbox/queue"
	"outbox/shared"
	"syscall"

	"github.com/joho/godotenv"
	"github.com/robfig/cron/v3"
)

func main() {
	if err := godotenv.Load(); err != nil {
		log.Println("loading env file: ", err)
	}

	db, err := database.NewConnection()
	if err != nil {
		log.Fatal("error connecting to db")
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

	// Declare a fanout exchange
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

	// The queue declaration is still needed for compatibility
	// Worker services will bind their own queues to this exchange
	q, err := ch.QueueDeclare(
		"outbox_fanout", // name (can be empty for exclusive queues)
		true,            // durable
		false,           // delete when unused
		false,           // exclusive
		false,           // no-wait
		nil,             // arguments
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

	jobProcessor := shared.OutboxProcessor{
		DB:           db,
		Channel:      ch,
		Queue:        q,
		Exchange:     exchangeName,
		ExchangeType: "fanout",
	}

	c := cron.New()
	_, err = c.AddFunc("@every 10s", jobProcessor.HandleOutboxMessage)
	if err != nil {
		log.Fatal("register handler error", err)
	}
	log.Printf("Start processing outbox messages with fanout exchange: %s", exchangeName)
	c.Start()
	defer c.Stop()

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
