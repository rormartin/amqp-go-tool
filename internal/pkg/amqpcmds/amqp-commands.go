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

package amqpcmds

import (
	"fmt"
	"github.com/streadway/amqp"
	"os"
	"strconv"
)

type CommandInfo struct {
	User            string
	Password        string
	Host            string
	Port            int
	Durable         bool
	AutoACK         bool
	Prefetch        int
	Count           int
	File            string
	FormatPrefix    string
	FormatSeparator string
	FormatPostfix   string
}

const toolName = "amqp-go-tool"

func (c *CommandInfo) CommandExport(queue string) error {
	conn, err := amqp.Dial("amqp://" + c.User + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port) + "/")
	if err != nil {
		return fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(queue, c.Durable, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, toolName, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to register a consumer: %v", err)
	}

	err = ch.Qos(c.Prefetch, 0, false) // prefetch count
	if err != nil {
		fmt.Errorf("Error defining prefetch: %v", err)
	}

	var f *os.File
	// manage file
	if c.File != "" {
		f, err = os.Create(c.File)
		if err != nil {
			return fmt.Errorf("Failed to create output file: %v", err)
		}
	} else {
		f = os.Stdout
	}

	f.WriteString(c.FormatPrefix)
	defer func() error {
		f.Seek(-1, 1)
		_, err = f.WriteString(c.FormatPostfix)
		if err != nil {
			return fmt.Errorf("Error writing in file: %v", err)
		}
		err = f.Close()
		if err != nil {
			return fmt.Errorf("Error closing file: %v", err)
		}
		return nil
	}()

	counter := 0
	for msg := range msgs {
		_, err = f.Write(msg.Body)
		if err != nil {
			return fmt.Errorf("Error writing message content in file: %v", err)
		}
		_, err = f.WriteString(c.FormatSeparator)
		if err != nil {
			return fmt.Errorf("Error writing in file: %v", err)
		}
		if c.AutoACK {
			msg.Ack(false)
		}
		counter++
		if (c.Count != 0) && (counter > c.Count-1) {
			break
		}
	}
	return nil
}

func (c *CommandInfo) CommandMoveToQueue(srcQueue, dstQueue string) error {
	conn, err := amqp.Dial("amqp://" + c.User + ":" + c.Password + "@" + c.Host + ":" + strconv.Itoa(c.Port) + "/")
	if err != nil {
		fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		fmt.Errorf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	q, err := ch.QueueDeclare(srcQueue, c.Durable, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Failed to declare a queue: %v", err)
	}

	msgs, err := ch.Consume(q.Name, toolName, false, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Failed to register a consumer: %v", err)
	}

	err = ch.Qos(c.Prefetch, 0, false) // prefetch count
	if err != nil {
		fmt.Errorf("Error defining prefetch: %v", err)
	}

	chDst, err := conn.Channel()
	if err != nil {
		fmt.Errorf("Failed to open a destiny channel: %v", err)
	}

	_, err = chDst.QueueDeclare(dstQueue, c.Durable, false, false, false, nil)
	if err != nil {
		fmt.Errorf("Failed to declare destiny queue: %v", err)
	}

	var f *os.File
	// manage file
	if c.File != "" {
		f, err = os.Create(c.File)
		if err != nil {
			fmt.Errorf("Failed to create output file: %v", err)
		}
	} else {
		f = os.Stdout
	}

	f.WriteString(c.FormatPrefix)
	defer func() error {
		f.Seek(-1, 1)
		_, err = f.WriteString(c.FormatPostfix)
		if err != nil {
			fmt.Errorf("Error writing in file: %v", err)
		}
		err = f.Close()
		if err != nil {
			fmt.Errorf("Error closing file: %v", err)
		}
		return nil
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
		err = chDst.Publish("", dstQueue, false, false, amqpMsg)
		if err != nil {
			fmt.Errorf("Error on message publishing: %v", err)
		}
		_, err = f.Write(msg.Body)
		if err != nil {
			fmt.Errorf("Error writing message content in file: %v", err)
		}
		_, err = f.WriteString(c.FormatSeparator)
		if err != nil {
			fmt.Errorf("Error writing in file: %v", err)
		}
		if c.AutoACK {
			msg.Ack(false)
		}
		counter++
		if (c.Count != 0) && (counter > c.Count-1) {
			break
		}
	}
	return nil
}