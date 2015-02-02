package samqp

import (
	"fmt"
	"sync"
	"time"

	"github.com/streadway/amqp"
	"github.com/tj/go-debug"
)

var log = debug.Debug("SAMQP")

type Connection struct {
	url string

	amqpConn *amqp.Connection
	aviable  bool

	sendChannel chan chan *amqp.Channel
	requestPoll []chan *amqp.Channel

	m sync.Mutex
}

func Dial(url string) *Connection {
	c := &Connection{
		url:         "amqp://" + url,
		aviable:     false,
		sendChannel: make(chan chan *amqp.Channel),
		requestPoll: make([]chan *amqp.Channel, 0),
	}
	go c.connectLoop()
	go c.serveChannel()
	return c
}

func (c *Connection) Die() {
	c.amqpConn.Close()
}

func (c *Connection) connectLoop() {
	for {
		amqpConn, err := amqp.Dial(c.url)
		if err != nil {
			log("connot connect")
			time.Sleep(time.Millisecond * 200)
			continue
		}

		c.m.Lock()
		c.amqpConn = amqpConn
		c.aviable = true
		c.m.Unlock()
		log("connected")

		go c.freeReqPoll()

		onClose := amqpConn.NotifyClose(make(chan *amqp.Error))
		<-onClose
		c.m.Lock()
		c.aviable = false
		c.m.Unlock()
		log("connection lost")
	}
}

func (c *Connection) serveChannel() {
	for request := range c.sendChannel {
		c.m.Lock()

		if c.aviable {
			amqpChannel, err := c.amqpConn.Channel()
			if err == nil {
				c.m.Unlock()
				request <- amqpChannel
				continue
			} else {
				fmt.Println(err)
			}
		}

		c.requestPoll = append(c.requestPoll, request)

		c.m.Unlock()
	}
}

func (c *Connection) freeReqPoll() {
	c.m.Lock()
	poll := c.requestPoll
	c.requestPoll = append(c.requestPoll[:0])
	c.m.Unlock()

	for _, request := range poll {
		c.sendChannel <- request
	}
}

type Channel struct {
	amqpChannel *amqp.Channel
	aviable     bool

	channel chan chan *amqp.Channel
	poll    []chan *amqp.Channel

	request chan chan *amqp.Channel
	do      []func() error

	m sync.Mutex
}

func (c *Connection) Channel() *Channel {
	ch := &Channel{
		request: c.sendChannel,
		aviable: false,
		channel: make(chan chan *amqp.Channel),
		poll:    make([]chan *amqp.Channel, 0),
		do:      make([]func() error, 0),
	}
	go ch.channelLoop()
	go ch.serveChannel()
	return ch
}

func (ch *Channel) channelLoop() {
	for {
		receive := make(chan *amqp.Channel)
		ch.request <- receive

		amqpChannel := <-receive

		ch.m.Lock()
		ch.amqpChannel = amqpChannel
		ch.aviable = true
		ch.m.Unlock()

		log("got new channel")

		go ch.freePoll()

		onCLose := ch.amqpChannel.NotifyClose(make(chan *amqp.Error))
		<-onCLose
		log("a channel dropped")

		ch.m.Lock()
		ch.aviable = false
		ch.m.Unlock()
	}
}

func (ch *Channel) freePoll() {
	ch.m.Lock()
	poll := ch.poll
	ch.poll = append(ch.poll[:0])
	ch.m.Unlock()

	for _, request := range poll {
		ch.channel <- request
	}
}

func (ch *Channel) serveChannel() {
	for request := range ch.channel {
		ch.m.Lock()
		if ch.aviable {
			ch.m.Unlock()
			request <- ch.amqpChannel
		} else {
			ch.poll = append(ch.poll, request)
			ch.m.Unlock()
		}
	}
}

func (ch *Channel) getChannel() *amqp.Channel {
	receive := make(chan *amqp.Channel)
	ch.channel <- receive
	return <-receive
}

func (ch *Channel) QueueDeclare(name string, durable, autoDelete, exclusive, noWait bool, args amqp.Table) amqp.Queue {
	channel := ch.getChannel()
	q, err := channel.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	if err != nil {
		return ch.QueueDeclare(name, durable, autoDelete, exclusive, noWait, args)
	}
	return q
}

func (ch *Channel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) {
	do := func() error {
		channel := ch.getChannel()
		return channel.Publish(exchange, key, mandatory, immediate, msg)
	}

	go func() {
		err := do()
		if err != nil {
			fmt.Println(err)
			ch.Publish(exchange, key, mandatory, immediate, msg)
		}
	}()
}
