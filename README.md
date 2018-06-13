# amqp-go-tool
[![Coverage Status](https://coveralls.io/repos/github/icemobilelab/amqp-go-tool/badge.svg)](https://coveralls.io/github/icemobilelab/amqp-go-tool)
[![Go Report Card](https://goreportcard.com/badge/github.com/icemobilelab/amqp-go-tool)](https://goreportcard.com/report/github.com/icemobilelab/amqp-go-tool)

Command line tool to manage data in RabbitMQ queues and exchanges as a client.

## Install the utility tool

```
go get github.com/icemobilelab/amqp-go-tool

go install github.com/icemobilelab/amqp-go-tool
```

## Parameters

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

Use "amqp-go-tool [command] --help" for more information about a command.
```
