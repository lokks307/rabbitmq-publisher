package rbmqpublisher

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"

	"github.com/labstack/echo/v4"
	"github.com/lokks307/djson/v2"
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sirupsen/logrus"
)

func failOnError(err error, msg string) {
	log.Printf("%s: %s", msg, err)
}

var RabbitmqCh *amqp.Channel
var RabbitmqConn *amqp.Connection

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

var rabbitmqConfig *RabbitmqSetting

func Init(rabbitSetting *RabbitmqSetting) error {
	var err error
	rabbitmqAddr := fmt.Sprintf("amqp://%s:%s@%s:%s", rabbitSetting.User, rabbitSetting.UserPw, rabbitSetting.Addr, rabbitSetting.AmqpPort)
	logrus.Trace("rabbitmq: " + rabbitmqAddr)
	RabbitmqConn, err = amqp.Dial(rabbitmqAddr)
	if err != nil {
		failOnError(err, "failed to Connect to RabbitMQ")
		return err
	}

	RabbitmqCh, err = RabbitmqConn.Channel()
	if err != nil {
		failOnError(err, "Failed to Open channel")
		return err
	}
	err = RabbitmqCh.ExchangeDeclare(
		rabbitSetting.Exchange, //exchange name
		"direct",               // exchange type
		true,                   // durable
		false,                  //auto-deleted
		false,                  // internal
		false,                  // no-wait
		nil,                    //arguments
	)
	if err != nil {
		failOnError(err, "Failed to declare an exchange")
		return err
	}
	logrus.Trace("Rabbitmq ready")
	return nil
}

func RabbitmqOperationLogger(rabbitSetting *RabbitmqSetting) echo.MiddlewareFunc {
	return func(next echo.HandlerFunc) echo.HandlerFunc {
		return func(c echo.Context) (err error) {
			req := c.Request()
			res := c.Response()
			start := time.Now()

			err = next(c)
			if req.RequestURI == "/heartbeat" || req.RequestURI == "/live" || req.RequestURI == "/ready" {
				return
			}

			if rabbitSetting.LogMode == "onlyError" {
				if res.Status < 400 {
					return
				}
			}
			if req.Method == http.MethodGet || req.Method == http.MethodPost || req.Method == http.MethodPut || req.Method == http.MethodDelete {
				stop := time.Now()
				logrus.Trace("api called and will publish message: " + req.RequestURI)
				reqId := c.Get("request_id")
				reqBodyBytes, err := io.ReadAll(c.Request().Body)

				c.Request().Body = io.NopCloser(bytes.NewBuffer(reqBodyBytes))

				val := djson.New().Put(djson.Object{
					"request_id":   reqId,
					"client_ip":    req.RemoteAddr,
					"latency":      stop.Sub(start).Milliseconds(),
					"method":       req.Method,
					"uri":          req.RequestURI,
					"status":       res.Status,
					"referrer":     req.Host,
					"user_agent":   req.UserAgent(),
					"request_body": string(reqBodyBytes),
				})

				if err != nil {
					val.Put("err_log", err.Error())
				}

				userUID := c.Param("user_uid")
				if userUID == "" {
					userUID = "default"
				}

				go func(userUID string, val *djson.JSON) {
					ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
					defer cancel()

					err = RabbitmqCh.PublishWithContext(ctx,
						rabbitSetting.Exchange,   // exchange
						rabbitSetting.RoutingKey, // routing key
						false,                    // mandatory
						false,                    // immediate
						amqp.Publishing{
							DeliveryMode: amqp.Persistent,
							ContentType:  "text/plain",
							Timestamp:    time.Now(),
							Body:         []byte(val.ToString()),
						})
					if err != nil {
						failOnError(err, "Failed to publish a message")
					}
				}(userUID, val)
			}

			return
		}
	}
}
