package rbmqpublisher

import (
	"context"
	"errors"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

func failOnError(err error, msg string) error {
	logrus.Error(err, msg)
	return err
}

type RabbitMQ struct {
	RabbitmqCh   *amqp.Channel
	RabbitmqConn *amqp.Connection
	Config       *RabbitmqSetting
}
type RabbitmqSetting struct {
	Addr               string
	User               string
	UserPw             string
	AmqpPort           string
	RabbitmqManagePort string
	Exchange           string
	RoutingKey         string
	LogMode            string
}

func (m *RabbitMQ) Init() error {
	var err error

	// check config
	if m.Config == nil {
		return errors.New("configuration is nil")
	}

	if m.Config.Addr == "" ||
		m.Config.User == "" ||
		m.Config.UserPw == "" ||
		m.Config.AmqpPort == "" ||
		m.Config.RabbitmqManagePort == "" ||
		m.Config.Exchange == "" ||
		m.Config.RoutingKey == "" ||
		m.Config.LogMode == "" {
		return errors.New("configuration fields cannot be empty")
	}

	rabbitmqAddr := fmt.Sprintf("amqp://%s:%s@%s:%s", m.Config.User, m.Config.UserPw, m.Config.Addr, m.Config.AmqpPort)
	m.RabbitmqConn, err = amqp.Dial(rabbitmqAddr)
	if err != nil {
		return failOnError(err, "failed to Connect to RabbitMQ")
	}

	m.RabbitmqCh, err = m.RabbitmqConn.Channel()
	if err != nil {
		return failOnError(err, "Failed to Open channel")
	}
	err = m.RabbitmqCh.ExchangeDeclare(
		m.Config.Exchange, //exchange name
		"direct",          // exchange type
		true,              // durable
		false,             //auto-deleted
		false,             // internal
		false,             // no-wait
		nil,               //arguments
	)
	if err != nil {
		return failOnError(err, "Failed to declare an exchange")
	}
	logrus.Trace("Rabbitmq ready")
	return nil
}

func (m *RabbitMQ) Publish(body []byte) error {

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()

	if m.RabbitmqCh == nil {
		return errors.New("empty channel")
	}
	err := m.RabbitmqCh.PublishWithContext(ctx,
		m.Config.Exchange,   // exchange
		m.Config.RoutingKey, // routing key
		false,               // mandatory
		false,               // immediate
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Timestamp:    time.Now(),
			Body:         body,
		})
	if err != nil {
		return failOnError(err, "Failed to publish a message")
	}
	return nil
}
