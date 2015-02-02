package main

import (
	"flag"

	"github.com/ilgooz/samqp"
	"github.com/streadway/amqp"
)

var (
	url = flag.String("rabbit", "guest:guest@192.168.1.7:5672", "AMQP URL")
)

func main() {
	flag.Parse()

	conn := samqp.Dial(*url)
	ch := conn.Channel()
	q := ch.QueueDeclare("my-queue", true, false, false, false, nil)
	ch.Publish("my-publish", q.Name, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        []byte("test"),
	})

	select {}
}
