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
	"github.com/streadway/amqp"
)

// amqpConnection interface to help to mock and test the amqp library
type amqpConnection interface {
	Close() error
	Channel() (amqpChannel, error)
}

// amqpChannel interface to help to mock and test the amqp library
type amqpChannel interface {
	Close() error
	Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error)
	Qos(prefetchCount, prefetchSize int, global bool) error
	Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error
}

// --------------------------------------------------------------------------------

// wrapper implementation for help to mock and test the amqp library
type wrapperConn struct {
	conn *amqp.Connection
}

func (c *wrapperConn) Close() error {
	return c.conn.Close()
}

func (c *wrapperConn) Channel() (amqpChannel, error) {
	ch, err := c.conn.Channel()
	if err != nil {
		return nil, err
	}
	return &wrapperChannel{channel: ch}, nil
}

type wrapperChannel struct {
	channel *amqp.Channel
}

func (c *wrapperChannel) Close() error {
	return c.channel.Close()
}

func (c *wrapperChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	return c.channel.Consume(queue, consumer, autoAck, exclusive, noLocal, noWait, args)
}

func (c *wrapperChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	return c.channel.Qos(prefetchCount, prefetchSize, global)
}

func (c *wrapperChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	return c.channel.Publish(exchange, key, mandatory, immediate, msg)
}
