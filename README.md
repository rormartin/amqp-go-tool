# amqp-go-tool
[![Coverage Status](https://coveralls.io/repos/github/rormartin/amqp-go-tool/badge.svg)](https://coveralls.io/github/rormartin/amqp-go-tool)
[![Go Report Card](https://goreportcard.com/badge/github.com/rormartin/amqp-go-tool)](https://goreportcard.com/report/github.com/rormartin/amqp-go-tool)

Command line tool to manage data in RabbitMQ queues and exchanges as a client.

## Install the utility tool

```
go get github.com/rormartin/amqp-go-tool

go install github.com/rormartin/amqp-go-tool
```

## Command line

```
Move and export messages from a and to a RabbitMQ

Usage:
  amqp-go-tool [command]

Available Commands:
  copy        Copy messages from one queue to another one
  export      Export the messages from a RabbitMQ queue
  help        Help about any command
  move        Move messages from one queue to another one
  
Flags:
      --config string     config file (default is $HOME/.amqp-go-tool.yaml)
  -h, --help              help for amqp-go-tool
      --host string       RabbitMQ host name (default "localhost")
      --password string   RabbitMQ password (default "guest")
      --port int          RabbitMQ port (default 5672)
      --username string   RabbitMQ username (default "guest")
	  --version           version for amqp-go-tool

Use "amqp-go-tool [command] --help" for more information about a command.
```

### `export` command

```
Usage:
  amqp-go-tool export [queue] [flags]

Flags:
      --auto-ack                 Auto ACK the messages after exported
      --count int                Messages to export (0 for keep waiting for messages)
      --file string              Output file for messages (no value for stdout)
      --formatPostfix string     Post-fix value for the message list
      --formatPrefix string      Prefix value for the message list
      --formatSeparator string   Separator between messages (default "\n")
  -h, --help                     help for export
      --prefetch int             Prefetch value to consumer messages (default 1)

Global Flags:
      --config string     config file (default is $HOME/.amqp-go-tool.yaml)
      --host string       RabbitMQ host name (default "localhost")
      --password string   RabbitMQ password (default "guest")
      --port int          RabbitMQ port (default 5672)
      --username string   RabbitMQ username (default "guest")
```

### `copy` command

```
Usage:
  amqp-go-tool copy [origin_queue] [destiny_queue] [flags]

Flags:
      --count int                Messages to export (0 for keep waiting for messages)
      --file string              Output file for messages (no value for stdout)
      --formatPostfix string     Post-fix value for the message list
      --formatPrefix string      Prefix value for the message list
      --formatSeparator string   Separator between messages (default "\n")
  -h, --help                     help for copy
      --prefetch int             Prefetch value to consumer messages (default 1)

Global Flags:
      --config string     config file (default is $HOME/.amqp-go-tool.yaml)
      --host string       RabbitMQ host name (default "localhost")
      --password string   RabbitMQ password (default "guest")
      --port int          RabbitMQ port (default 5672)
      --username string   RabbitMQ username (default "guest")
```

### `move` command

```
Usage:
  amqp-go-tool move [origin_queue] [destiny_queue] [flags]

Flags:
      --count int                Messages to export (0 for keep waiting for messages)
      --file string              Output file for messages (no value for stdout)
      --formatPostfix string     Post-fix value for the message list
      --formatPrefix string      Prefix value for the message list
      --formatSeparator string   Separator between messages (default "\n")
  -h, --help                     help for move
      --prefetch int             Prefetch value to consumer messages (default 1)

Global Flags:
      --config string     config file (default is $HOME/.amqp-go-tool.yaml)
      --host string       RabbitMQ host name (default "localhost")
      --password string   RabbitMQ password (default "guest")
      --port int          RabbitMQ port (default 5672)
      --username string   RabbitMQ username (default "guest")
```
