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

// CommandInfo defines a basic structure to execute amqp commands
type CommandInfo struct {
	user            string
	password        string
	host            string
	port            int
	autoACK         bool
	prefetch        int
	count           int
	file            string
	formatPrefix    string
	formatSeparator string
	formatPostfix   string
	dialer          func(string) (amqpConnection, error)
}

const toolName = "amqp-go-tool"

// AmqpCommand general interface for the command execution
type AmqpCommand interface {
	CommandExport(queue string) error
	CommandCopyMoveToQueue(srcQueue, dstQueue string) error
}

// NewCommandInfo creates a new instance that can execute the Amqp
// commands, with the default amqp dialer wrapper
func NewCommandInfo(user, password, host string,
	port int,
	autoACK bool,
	prefetch, count int,
	file, formatPrefix, formatSeparator, formatPostfix string) AmqpCommand {

	d := func(url string) (amqpConnection, error) {
		conn, err := amqp.Dial(url)
		if err != nil {
			return nil, err
		}
		return &wrapperConn{conn: conn}, nil
	}

	ci := CommandInfo{
		user:            user,
		password:        password,
		host:            host,
		port:            port,
		autoACK:         autoACK,
		prefetch:        prefetch,
		count:           count,
		file:            file,
		formatPrefix:    formatPrefix,
		formatSeparator: formatSeparator,
		formatPostfix:   formatPostfix,
		dialer:          d,
	}
	return &ci
}

// CommandExport exports the content of a queue using the queue
// configuration and predefined format.
func (c *CommandInfo) CommandExport(queue string) error {
	conn, err := c.dialer("amqp://" + c.user + ":" + c.password + "@" + c.host + ":" + strconv.Itoa(c.port) + "/")
	if err != nil {
		return fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(queue, toolName, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to register a consumer: %v", err)
	}

	err = ch.Qos(c.prefetch, 0, false) // prefetch count
	if err != nil {
		return fmt.Errorf("Error defining prefetch: %v", err)
	}

	var f *os.File
	// manage file
	if c.file != "" {
		f, err = os.Create(c.file)
		if err != nil {
			return fmt.Errorf("Failed to create output file: %v", err)
		}
	} else {
		f = os.Stdout
	}

	f.WriteString(c.formatPrefix)
	defer func() error {
		_, err = f.WriteString(c.formatPostfix)
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
		if !(counter+1 > c.count-1) || c.count == 0 {
			_, err = f.WriteString(c.formatSeparator)
		}
		if err != nil {
			return fmt.Errorf("Error writing in file: %v", err)
		}
		if c.autoACK {
			msg.Ack(false)
		}
		counter++
		if (c.count != 0) && (counter > c.count-1) {
			break
		}
	}
	return nil
}

// CommandCopyMoveToQueue copy or moves messages from one queue to another
// one. The copy is a exact one: it propagate the meta-information of
// the message, not just the content.
func (c *CommandInfo) CommandCopyMoveToQueue(srcQueue, dstQueue string) error {
	conn, err := c.dialer("amqp://" + c.user + ":" + c.password + "@" + c.host + ":" + strconv.Itoa(c.port) + "/")
	if err != nil {
		return fmt.Errorf("Failed to connect to RabbitMQ: %v", err)
	}
	defer conn.Close()

	ch, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a channel: %v", err)
	}
	defer ch.Close()

	msgs, err := ch.Consume(srcQueue, toolName, false, false, false, false, nil)
	if err != nil {
		return fmt.Errorf("Failed to register a consumer: %v", err)
	}

	err = ch.Qos(c.prefetch, 0, false) // prefetch count
	if err != nil {
		return fmt.Errorf("Error defining prefetch: %v", err)
	}

	chDst, err := conn.Channel()
	if err != nil {
		return fmt.Errorf("Failed to open a destiny channel: %v", err)
	}

	var f *os.File
	// manage file
	if c.file != "" {
		f, err = os.Create(c.file)
		if err != nil {
			return fmt.Errorf("Failed to create output file: %v", err)
		}
	} else {
		f = os.Stdout
	}

	f.WriteString(c.formatPrefix)
	defer func() error {
		_, err = f.WriteString(c.formatPostfix)
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
			return fmt.Errorf("Error on message publishing: %v", err)
		}
		_, err = f.Write(msg.Body)
		if err != nil {
			return fmt.Errorf("Error writing message content in file: %v", err)
		}
		if !(counter+1 > c.count-1) || c.count == 0 {
			_, err = f.WriteString(c.formatSeparator)
		}
		if err != nil {
			return fmt.Errorf("Error writing in file: %v", err)
		}
		if c.autoACK {
			msg.Ack(false)
		}
		counter++
		if (c.count != 0) && (counter > c.count-1) {
			break
		}
	}
	return nil
}
