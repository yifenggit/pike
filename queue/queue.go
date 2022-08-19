package queue

import (
	"context"
	"fmt"
	"log"
	"reflect"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Queueable[T any] interface {
	Perform(p T)
	Async(p T)
	NewClient(p pulsar.ClientOptions) (client pulsar.Client)
	CreateConsumer() (pulsar.Consumer, chan pulsar.ConsumerMessage)
	SetMessage(p pulsar.Message)
	Ack()
	AckSchema() bool
}

type QueueBase[T any] struct {
	Topic string
	pulsar.Client
	pulsar.Producer
	pulsar.Consumer
	pulsar.Message
	MessageChannel chan pulsar.ConsumerMessage
	onceProducer   sync.Once
	onceClient     sync.Once
	onceConsumer   sync.Once
	autoAck        *bool
}

var queues map[string]Queueable[any]

func Register[T any](q Queueable[T]) Queueable[T] {
	k := reflect.TypeOf(q).String()
	queues[k] = nil
	return q
}

func (m *QueueBase[T]) SetTopic(topic string) {
	m.Topic = topic
}

func (m *QueueBase[T]) SetMessage(message pulsar.Message) {
	m.Message = message
}

func (m *QueueBase[T]) NewClient(options pulsar.ClientOptions) (client pulsar.Client) {
	m.onceClient.Do(func() {
		client, err := pulsar.NewClient(options)
		if err != nil {
			log.Fatalf("Could not instantiate Pulsar client: %v", err)
		} else {
			m.Client = client
		}
	})
	if m.Client != nil {

	}
	return m.Client
}

func (m *QueueBase[T]) setClient() (client pulsar.Client) {
	if m.Client == nil {
		m.Client = pulsarClient
	}
	return m.Client
}

func (m *QueueBase[T]) schema() pulsar.Schema {
	def := `{"type":"record","name":"Example","namespace":"test","fields":[]}`
	properties := make(map[string]string)
	var schema pulsar.Schema
	var t T
	switch reflect.TypeOf(t).Kind() {
	case reflect.Int8:
		schema = pulsar.NewInt8Schema(properties)
	case reflect.Int16:
		schema = pulsar.NewInt16Schema(properties)
	case reflect.Int, reflect.Int32:
		schema = pulsar.NewInt32Schema(properties)
	case reflect.Int64:
		schema = pulsar.NewInt64Schema(properties)
	case reflect.String:
		schema = pulsar.NewStringSchema(properties)
	case reflect.Float32:
		schema = pulsar.NewFloatSchema(properties)
	case reflect.Float64:
		schema = pulsar.NewDoubleSchema(properties)
	case reflect.Array:
		schema = pulsar.NewBytesSchema(properties)
	default:
		schema = pulsar.NewProtoSchema(def, properties)
	}
	return schema
}

func (m *QueueBase[T]) createProducer() pulsar.Producer {
	m.onceProducer.Do(func() {
		producer, err := m.Client.CreateProducer(pulsar.ProducerOptions{
			Topic:  m.Topic,
			Schema: m.schema(),
		})
		if err != nil {
			log.Fatal(err)
		}
		m.Producer = producer
	})
	return m.Producer
}

func (m *QueueBase[T]) Async(p T) {
	m.setClient()
	m.createProducer()
	var value any
	if v := reflect.ValueOf(p).Kind(); v == reflect.Struct {
		value = &p
	} else {
		value = p
	}
	_, err := m.Producer.Send(context.Background(), &pulsar.ProducerMessage{
		Value: value,
	})
	if err != nil {
		log.Fatal(err)
	}
}

func (m *QueueBase[T]) CreateConsumer() (pulsar.Consumer, chan pulsar.ConsumerMessage) {
	m.setClient()
	m.onceConsumer.Do(func() {
		m.MessageChannel = make(chan pulsar.ConsumerMessage, 100)
		consumer, err := m.Client.Subscribe(pulsar.ConsumerOptions{
			Topic:                       m.Topic,
			SubscriptionName:            fmt.Sprintf("subscribe_%s", reflect.TypeOf(m).String()),
			Schema:                      m.schema(),
			SubscriptionInitialPosition: pulsar.SubscriptionPositionEarliest,
			MessageChannel:              m.MessageChannel,
			Type:                        pulsar.Shared,
		})
		if err != nil {
			log.Fatal(err)
		}
		m.Consumer = consumer
	})
	return m.Consumer, m.MessageChannel
}

func (m *QueueBase[T]) SetManualAck() {
	b := false
	m.autoAck = &b
}

func (m *QueueBase[T]) AckSchema() bool {
	if m.autoAck == nil {
		return true
	}
	return *m.autoAck
}

func (m *QueueBase[T]) Ack() {
	m.Consumer.Ack(m.Message)
}

func Subscribe[T any](p Queueable[T]) {
	go func(m Queueable[T]) {
		defer func ()  {
			if r := recover(); r != nil {
				fmt.Println("Recovered in f", r)
			}
		}()
		vmethod := reflect.ValueOf(&m).Elem().MethodByName("Perform")
		if vmethod.IsValid() {
			v := new(T)
			consumer, channel := m.CreateConsumer()
			for cm := range channel {
				msg := cm.Message
				m.SetMessage(msg)
				var err error
				if t := reflect.ValueOf(v); t.Elem().Kind() == reflect.String {
					err = msg.GetSchemaValue(&v)
				} else {
					err = msg.GetSchemaValue(v)
				}
				if err != nil {
					log.Fatalln(err)
				}
				if vmethod.Type().NumIn() > 0 {
					vmethod.Call([]reflect.Value{reflect.ValueOf(v).Elem()})
				} else {
					vmethod.Call([]reflect.Value{})
				}
				if m.AckSchema() {
					consumer.Ack(msg)
				}
			}
		}
	}(p)
}

func Wait() {
	wait := make(chan int, 1)
	<-wait
}
