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

func (m *QueueBase[T]) protoSchemaDef() *pulsar.ProtoSchema {
	def := `{"type":"record","name":"Example","namespace":"test","fields":[]}`
	protoSchema := pulsar.NewProtoSchema(def, nil)
	return protoSchema
}

func (m *QueueBase[T]) createProducer() pulsar.Producer {
	m.onceProducer.Do(func() {
		producer, err := m.Client.CreateProducer(pulsar.ProducerOptions{
			Topic:  m.Topic,
			Schema: m.protoSchemaDef(),
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
	_, err := m.Producer.Send(context.Background(), &pulsar.ProducerMessage{
		Value: &p,
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
			Schema:                      m.protoSchemaDef(),
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
		vmethod := reflect.ValueOf(&m).Elem().MethodByName("Perform")
		if vmethod.IsValid() {
			subv := new(T)
			consumer, channel := m.CreateConsumer()
			for cm := range channel {
				msg := cm.Message
				m.SetMessage(msg)
				err := msg.GetSchemaValue(subv)
				if err != nil {
					log.Fatalln(err)
				}
				if vmethod.Type().NumIn() > 0 {
					vmethod.Call([]reflect.Value{reflect.ValueOf(subv).Elem()})
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
