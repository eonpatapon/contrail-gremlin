package main

import (
	"github.com/streadway/amqp"
)

func setupRabbit(rabbitURI string, rabbitVHost string, rabbitQueue string) (*amqp.Connection, *amqp.Channel, <-chan amqp.Delivery) {
	log.Notice("Connecting to RabbitMQ...")

	conn, err := amqp.DialConfig(rabbitURI, amqp.Config{Vhost: rabbitVHost})
	if err != nil {
		log.Fatalf("Failed to connect: %s", err)
	}

	ch, err := conn.Channel()
	if err != nil {
		log.Fatalf("Failed to open channel: %s", err)
	}

	q, err := ch.QueueDeclare(
		rabbitQueue, // name
		false,       // durable
		false,       // delete when unused
		true,        // exclusive
		false,       // no-wait
		amqp.Table{"x-expires": int32(180000)}, // arguments
	)
	if err != nil {
		log.Fatalf("Failed to create queue: %s", err)
	}

	err = ch.QueueBind(
		q.Name,      // queue name
		"",          // routing key
		VncExchange, // exchange
		false,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to bind queue: %s", err)
	}

	msgs, err := ch.Consume(
		q.Name, // queue
		"",     // consumer
		false,  // auto-ack
		false,  // exclusive
		false,  // no-local
		false,  // no-wait
		nil,    // args
	)
	if err != nil {
		log.Fatalf("Failed to register consumer: %s", err)
	}

	log.Notice("Connected.")

	return conn, ch, msgs
}

func teardownRabbit(conn *amqp.Connection, ch *amqp.Channel, rabbitQueue string) error {
	err := ch.QueueUnbind(rabbitQueue, "", VncExchange, amqp.Table{})
	if err != nil {
		return err
	}
	_, err = ch.QueueDelete(rabbitQueue, false, false, true)
	if err != nil {
		return err
	}
	return conn.Close()
}
