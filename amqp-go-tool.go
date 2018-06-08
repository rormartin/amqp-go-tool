// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.

// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU General Public License for more details.

// You should have received a copy of the GNU General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
	"github.com/streadway/amqp"
	"github.com/urfave/cli"
	"log"
	"os"
	"strconv"
)

type commandInfo struct {
	user            string
	password        string
	host            string
	port            int
	queue           string
	durable         bool
	autoACK         bool
	prefetch        int
	count           int
	file            string
	formatPrefix    string
	formatSeparator string
	formatPostfix   string
}

const toolName = "amqp-go-tool"

func failOnError(err error, msg string) {
	if err != nil {
		log.Fatalf("%s: %s", msg, err)
	}
}

func (c *commandInfo) commandExport() {
	conn, err := amqp.Dial("amqp://" + c.user + ":" + c.password + "@" + c.host + ":" + strconv.Itoa(c.port) + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(c.queue, c.durable, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, toolName, false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	err = ch.Qos(c.prefetch, 0, false) // prefetch count

	var f *os.File
	// manage file
	if c.file != "" {
		f, err = os.Create(c.file)
		failOnError(err, "Failed to create output file")
	} else {
		f = os.Stdout
	}

	f.WriteString(c.formatPrefix)
	defer func() {
		f.Seek(-1, 1)
		_, err = f.WriteString(c.formatPostfix)
		failOnError(err, "Error writing in file")
		err = f.Close()
		failOnError(err, "Error closing file")
	}()

	counter := 0
	for msg := range msgs {
		_, err = f.Write(msg.Body)
		failOnError(err, "Error writing message content in file")
		_, err = f.WriteString(c.formatSeparator)
		failOnError(err, "Error writing in file")
		if c.autoACK {
			msg.Ack(false)
		}
		counter++
		if (c.count != 0) && (counter > c.count-1) {
			break
		}
	}
}

func (c *commandInfo) commandMoveToQueue(queueDst string) {
	conn, err := amqp.Dial("amqp://" + c.user + ":" + c.password + "@" + c.host + ":" + strconv.Itoa(c.port) + "/")
	failOnError(err, "Failed to connect to RabbitMQ")
	defer conn.Close()

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")
	defer ch.Close()

	q, err := ch.QueueDeclare(c.queue, c.durable, false, false, false, nil)
	failOnError(err, "Failed to declare a queue")

	msgs, err := ch.Consume(q.Name, toolName, false, false, false, false, nil)
	failOnError(err, "Failed to register a consumer")

	err = ch.Qos(c.prefetch, 0, false) // prefetch count

	chDst, err := conn.Channel()
	failOnError(err, "Failed to open a destiny channel")

	_, err = chDst.QueueDeclare(queueDst, c.durable, false, false, false, nil)
	failOnError(err, "Failed to declare destiny queue")

	var f *os.File
	// manage file
	if c.file != "" {
		f, err = os.Create(c.file)
		failOnError(err, "Failed to create output file")
	} else {
		f = os.Stdout
	}

	f.WriteString(c.formatPrefix)
	defer func() {
		f.Seek(-1, 1)
		_, err = f.WriteString(c.formatPostfix)
		failOnError(err, "Error writing in file")
		err = f.Close()
		failOnError(err, "Error closing file")
	}()

	counter := 0
	for msg := range msgs {
		amqpMsg := amqp.Publishing{
			Headers:         msg.Headers,
			ContentType:     msg.ContentType,
			ContentEncoding: msg.ContentEncoding,
			DeliveryMode:    msg.DeliveryMode,
			Priority:        msg.Priority,
			CorrelationId:   msg.CorrelationId,
			ReplyTo:         msg.ReplyTo,
			Expiration:      msg.Expiration,
			MessageId:       msg.MessageId,
			Timestamp:       msg.Timestamp,
			Type:            msg.Type,
			UserId:          msg.UserId,
			AppId:           msg.AppId,
			Body:            msg.Body,
		}
		err = chDst.Publish("", queueDst, false, false, amqpMsg)
		failOnError(err, "Error on message publishing")
		_, err = f.Write(msg.Body)
		failOnError(err, "Error writing message content in file")
		_, err = f.WriteString(c.formatSeparator)
		failOnError(err, "Error writing in file")
		if c.autoACK {
			msg.Ack(false)
		}
		counter++
		if (c.count != 0) && (counter > c.count-1) {
			break
		}
	}
}

func main() {
	app := cli.NewApp()
	app.Name = toolName
	app.Usage = "move and export messages from a and to a RabbitMQ"
	app.Version = "0.0.1"

	app.Commands = []cli.Command{
		{
			Name:  "export",
			Usage: "Export the content of a queue",
			Action: func(c *cli.Context) error {
				globalCtx := c.Parent() // get global flags
				ci := commandInfo{
					user:            globalCtx.String("user"),
					password:        globalCtx.String("password"),
					host:            globalCtx.String("host"),
					port:            globalCtx.Int("port"),
					queue:           globalCtx.String("queue"),
					durable:         globalCtx.Bool("durable"),
					autoACK:         globalCtx.Bool("auto-ack"),
					prefetch:        globalCtx.Int("prefetch"),
					count:           globalCtx.Int("count"),
					file:            globalCtx.String("file"),
					formatPrefix:    globalCtx.String("formatPrefix"),
					formatSeparator: globalCtx.String("formatSeparator"),
					formatPostfix:   globalCtx.String("formatPostfix"),
				}
				if ci.queue == "" {
					return cli.NewExitError("Queue not defined", 1)
				}
				ci.commandExport()
				return nil
			},
		},
		{
			Name:  "move",
			Usage: "Move messages from one queue to another queue",
			Flags: []cli.Flag{
				cli.StringFlag{Name: "destiny"},
			},
			Action: func(c *cli.Context) error {
				globalCtx := c.Parent() // get global flags
				ci := commandInfo{
					user:            globalCtx.String("user"),
					password:        globalCtx.String("password"),
					host:            globalCtx.String("host"),
					port:            globalCtx.Int("port"),
					queue:           globalCtx.String("queue"),
					durable:         globalCtx.Bool("durable"),
					autoACK:         globalCtx.Bool("auto-ack"),
					prefetch:        globalCtx.Int("prefetch"),
					count:           globalCtx.Int("count"),
					file:            globalCtx.String("file"),
					formatPrefix:    globalCtx.String("formatPrefix"),
					formatSeparator: globalCtx.String("formatSeparator"),
					formatPostfix:   globalCtx.String("formatPostfix"),
				}

				if ci.queue == "" {
					return cli.NewExitError("Queue not defined", 1)
				}

				if c.String("destiny") == "" {
					return cli.NewExitError("Destiny queue not defined", 1)
				}

				ci.commandMoveToQueue(c.String("destiny"))
				return nil
			},
		},
		// {
		// 	Name:  "import",
		// 	Usage: "Import the content in a queue",
		// 	Action: func(c *cli.Context) error {
		// 		fmt.Printf("import command")
		// 		return nil
		// 	},
		// },
	}

	app.Flags = []cli.Flag{
		cli.StringFlag{Name: "host", Value: "localhost"},
		cli.IntFlag{Name: "port", Value: 5672},
		cli.StringFlag{Name: "user", Value: "guest"},
		cli.StringFlag{Name: "password", Value: "guest"},
		cli.StringFlag{Name: "queue"},
		cli.BoolTFlag{Name: "durable"},
		cli.BoolFlag{Name: "auto-ack", Usage: "Acknowledge messages (move out of the queue)"},
		cli.IntFlag{Name: "count", Usage: "0 keeps waiting for new messages", Value: 0},
		cli.IntFlag{Name: "prefetch", Value: 1},
		cli.StringFlag{Name: "file, f"},
		cli.StringFlag{Name: "formatPrefix", Value: ""},
		cli.StringFlag{Name: "formatSeparator", Value: "\n"},
		cli.StringFlag{Name: "formatPostfix", Value: ""},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Fatal(err)
	}
}
