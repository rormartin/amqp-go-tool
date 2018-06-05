# amqp-go-tool

Command line tool to manage data in RabbitMQ queues and exchanges as a client.

Currently, only implemented export functionally (export the messages
from a RabbitMQ to a file standard Stdout)

## Install the utility

```
go get github.com/icemobilelab/amqp-go-tool

go install github.com/icemobilelab/amqp-go-tool
```

## Parameters

```
NAME:
   amqp-go-tool - [import?] and export messages from a and to a RabbitMQ

USAGE:
   amqp-go-tool [global options] command [command options] [arguments...]

VERSION:
   0.0.1

COMMANDS:
     export   Export the content of a queue
     help, h  Shows a list of commands or help for one command

GLOBAL OPTIONS:
   --host value            (default: "localhost")
   --port value            (default: 5672)
   --user value            (default: "guest")
   --password value        (default: "guest")
   --queue value           
   --durable               
   --auto-ack              Acknowledge messages (move out of the queue)
   --count value           0 keeps waiting for new messages (default: 0)
   --prefetch value        (default: 1)
   --file value, -f value  
   --help, -h              show help
   --version, -v           print the version
```
