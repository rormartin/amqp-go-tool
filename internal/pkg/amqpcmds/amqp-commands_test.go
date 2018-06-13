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
	"github.com/stretchr/testify/assert"
	"io/ioutil"
	"log"
	"os"
	"testing"
	"time"
)

// -----------------------------------------------------------------------------
// -- TEST DATA ----------------------------------------------------------------
// -----------------------------------------------------------------------------
var testL5 = [][]byte{[]byte("1"), []byte("2"), []byte("3"), []byte("4"), []byte("5")}

// -----------------------------------------------------------------------------
// -- MOCK for amqp ------------------------------------------------------------
// -----------------------------------------------------------------------------
type testACK struct {
	ackCount    *int
	ackError    bool
	nackError   bool
	rejectError bool
}

func (t *testACK) Ack(tag uint64, multiple bool) error {
	if t.ackError {
		return fmt.Errorf("Test error")
	}
	*t.ackCount++
	return nil
}

func (t *testACK) Nack(tag uint64, multiple bool, requeue bool) error {
	if t.nackError {
		return fmt.Errorf("Test error")
	}
	return nil
}

func (t *testACK) Reject(tag uint64, requeue bool) error {
	if t.rejectError {
		return fmt.Errorf("Test error")
	}
	return nil
}

type testConnection struct {
	errorChannel        bool
	errorClose          bool
	errorChannelConsume bool
	errorChannelQos     bool
	errorChannelClose   bool
	errorChannelPublish bool
	ackCount            int
	dataResult          []string
}

func (c *testConnection) Close() error {
	if c.errorClose {
		return fmt.Errorf("Test error")
	}
	return nil
}

func (c *testConnection) Channel() (amqpChannel, error) {
	if c.errorChannel {
		return nil, fmt.Errorf("Test error")
	}
	return &testChannel{c.errorChannelClose, c.errorChannelConsume, c.errorChannelQos, c.errorChannelPublish, testL5, &c.ackCount, &c.dataResult}, nil

}

type testChannel struct {
	errorClose   bool
	errorConsume bool
	errorQos     bool
	errorPublish bool
	data         [][]byte
	ackCount     *int
	dataResult   *[]string
}

func (c *testChannel) Close() error {
	if c.errorClose {
		return fmt.Errorf("Test error")
	}
	return nil
}

func (c *testChannel) Consume(queue, consumer string, autoAck, exclusive, noLocal, noWait bool, args amqp.Table) (<-chan amqp.Delivery, error) {
	if c.errorConsume {
		return nil, fmt.Errorf("Test error")
	}
	cad := make(chan amqp.Delivery)
	go func(ch chan amqp.Delivery) {
		for _, v := range c.data {
			del := amqp.Delivery{Acknowledger: &testACK{ackCount: c.ackCount}, Body: v}
			ch <- del
		}
	}(cad)
	return cad, nil
}

func (c *testChannel) Qos(prefetchCount, prefetchSize int, global bool) error {
	if c.errorQos {
		return fmt.Errorf("Test error")
	}
	return nil
}

func (c *testChannel) Publish(exchange, key string, mandatory, immediate bool, msg amqp.Publishing) error {
	if c.errorPublish {
		return fmt.Errorf("Test error")
	}
	*c.dataResult = append(*c.dataResult, string(msg.Body))
	return nil
}

// -----------------------------------------------------------------------------
// -- TEST START ---------------------------------------------------------------
// -----------------------------------------------------------------------------
func TestNewCommandInfo(t *testing.T) {

	amcmd := NewCommandInfo("user", "password", "host", 1000, true, 10, 20, "file", "prefix", "sep", "post")

	assert.Error(t, amcmd.CommandExport("test"))

}

func TestCommandExport(t *testing.T) {

	t.Run("Error dialing", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return nil, fmt.Errorf("Test error")
		}}
		assert.Error(t, ci.CommandExport("test"))
	})

	t.Run("Error in channel creation", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannel: true}, nil
		}}
		assert.Error(t, ci.CommandExport("test"))
	})

	t.Run("Error in consumer registration", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannelConsume: true}, nil
		}}
		assert.Error(t, ci.CommandExport("test"))
	})

	t.Run("Error defining prefetch", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannelQos: true}, nil
		}}
		assert.Error(t, ci.CommandExport("test"))
	})

	t.Run("Get one element (no autoACK)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 1, formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}
		ci.CommandExport("test")
		result := "(1)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)
	})

	t.Run("Get one element (autoACK)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName,
			count:           1,
			formatPrefix:    "(",
			formatPostfix:   ")",
			formatSeparator: "-",
			autoACK:         true}
		ci.CommandExport("test")
		result := "(1)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 1, tconn.ackCount)
	})

	t.Run("Test get multiple elements (no autoACK)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := &testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return tconn, nil
		}, file: tmpfileName, count: 3, formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}
		ci.CommandExport("test")
		result := "(1-2-3)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)

	})

	t.Run("Get multiple elements (autoACK)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 3, autoACK: true,
			formatPrefix:    "(",
			formatPostfix:   ")",
			formatSeparator: "-"}
		ci.CommandExport("test")
		result := "(1-2-3)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 3, tconn.ackCount)

	})

	t.Run("Get all elements (no autoACK, and no channel close)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 0, autoACK: false,
			formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}

		go ci.CommandExport("test")
		time.Sleep(500 * time.Millisecond) // FIXME: allows routine to fill the file

		result := "(1-2-3-4-5-"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)

	})

	t.Run("Get all elements (autoACK, and no channel close)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 0, autoACK: true,
			formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}

		go ci.CommandExport("test")
		time.Sleep(500 * time.Millisecond) // FIXME: allows routine to fill the file

		result := "(1-2-3-4-5-"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 5, tconn.ackCount)

	})

}

func TestCommandCopyMove(t *testing.T) {

	t.Run("Error dialing", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return nil, fmt.Errorf("Test error")
		}}
		assert.Error(t, ci.CommandCopyMoveToQueue("test1", "test2"))
	})

	t.Run("Error in channel creation", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannel: true}, nil
		}}
		assert.Error(t, ci.CommandCopyMoveToQueue("test1", "test2"))
	})

	t.Run("Error in consumer registration", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannelConsume: true}, nil
		}}
		assert.Error(t, ci.CommandCopyMoveToQueue("test1", "test2"))
	})

	t.Run("Error defining prefetch", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannelQos: true}, nil
		}}
		assert.Error(t, ci.CommandCopyMoveToQueue("test1", "test2"))
	})

	t.Run("Error in publish", func(t *testing.T) {
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &testConnection{errorChannelPublish: true}, nil
		}, count: 1}
		assert.Error(t, ci.CommandCopyMoveToQueue("test1", "test2"))
	})

	t.Run("Copy one element", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 1, formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}
		ci.CommandCopyMoveToQueue("test1", "test2")
		result := "(1)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 1)
		assert.Contains(t, tconn.dataResult, "1")
	})

	t.Run("Move one element", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName,
			count:           1,
			formatPrefix:    "(",
			formatPostfix:   ")",
			formatSeparator: "-",
			autoACK:         true}
		ci.CommandCopyMoveToQueue("test1", "test2")
		result := "(1)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 1, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 1)
		assert.Contains(t, tconn.dataResult, "1")
	})

	t.Run("Copy multiple elements", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := &testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return tconn, nil
		}, file: tmpfileName, count: 3, formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}
		ci.CommandCopyMoveToQueue("test1", "test2")
		result := "(1-2-3)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 3)
		assert.ElementsMatch(t, []string{"1", "2", "3"}, tconn.dataResult)

	})

	t.Run("Move multiple elements", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 3, autoACK: true,
			formatPrefix:    "(",
			formatPostfix:   ")",
			formatSeparator: "-"}
		ci.CommandCopyMoveToQueue("test1", "test2")
		result := "(1-2-3)"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 3, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 3)
		assert.ElementsMatch(t, []string{"1", "2", "3"}, tconn.dataResult)

	})

	t.Run("Copy all elements (no channel close)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 0, autoACK: false,
			formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}

		go ci.CommandCopyMoveToQueue("test1", "test2")
		time.Sleep(500 * time.Millisecond) // FIXME: allows routine to fill the file

		result := "(1-2-3-4-5-"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 0, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 5)
		assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5"}, tconn.dataResult)

	})

	t.Run("Test get all elements (autoACK, and no channel close)", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test")
		if err != nil {
			log.Fatal(err)
		}
		tmpfileName := tmpfile.Name()
		if err := tmpfile.Close(); err != nil {
			log.Fatal(err)
		}
		defer os.Remove(tmpfile.Name()) // clean up

		tconn := testConnection{}
		ci := CommandInfo{dialer: func(url string) (amqpConnection, error) {
			return &tconn, nil
		}, file: tmpfileName, count: 0, autoACK: true,
			formatPrefix: "(", formatPostfix: ")", formatSeparator: "-"}

		go ci.CommandCopyMoveToQueue("test1", "test2")
		time.Sleep(500 * time.Millisecond) // FIXME: allows routine to fill the file

		result := "(1-2-3-4-5-"

		assert.FileExists(t, tmpfileName)
		content, err := ioutil.ReadFile(tmpfileName)
		assert.Equal(t, result, string(content))
		assert.Equal(t, 5, tconn.ackCount)
		assert.Len(t, tconn.dataResult, 5)
		assert.ElementsMatch(t, []string{"1", "2", "3", "4", "5"}, tconn.dataResult)
	})

}
