package queue

import (
	"context"
	"errors"
	"fmt"
	"log"
	"reflect"
	"regexp"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type Callback func(pulsar.MessageID, *pulsar.ProducerMessage, error)
type Queueable[T any] interface {
	Perform(p T)
	Send(p T) (pulsar.MessageID, error)
	SendAsync(p T, callback Callback)
	NewClient(p pulsar.ClientOptions) (client pulsar.Client)
	CreateConsumer() (pulsar.Consumer, chan pulsar.ConsumerMessage)
	SetMessage(message pulsar.Message)
	ReadMessage(messageID pulsar.MessageID) (pulsar.Message, error)
	Ack()
	AckID(messageID pulsar.MessageID)
	AckSchema() bool
}

type QueueBase[T any] struct {
	Topic string
	pulsar.Client
	pulsar.Producer
	pulsar.Consumer
	pulsar.Message
	*pulsar.ProducerMessage
	MessageChannel chan pulsar.ConsumerMessage
	onceProducer   sync.Once
	onceClient     sync.Once
	onceConsumer   sync.Once
	autoAck        *bool
}

var registers map[string]any
var registersOnce sync.Once

func StructNameShort(p any) string {
	key := reflect.ValueOf(p).Elem().Type().Name()
	re := regexp.MustCompile(`\[.*\]$`)
	key = re.ReplaceAllString(key, "")
	return key
}

func Register(data ...any) {
	registersOnce.Do(func() {
		registers = make(map[string]any)
	})
	for _, p := range data {
		key := StructNameShort(p)
		re := regexp.MustCompile(`\[.*\]$`)
		key = re.ReplaceAllString(key, "")
		registers[key] = p
	}
}

type call struct {
	Q any
}

func Call(key string) *call {
	v, ok := registers[key]
	if ok {
		return &call{Q: v}
	} else {
		fmt.Printf("Not initialized %s struct\n", key)
	}
	return nil
}

func (m *call) Send(p any) (pulsar.MessageID, error) {
	vmethod := reflect.ValueOf(m.Q).MethodByName("Send")
	if vmethod.IsValid() {
		var returns []reflect.Value
		if vmethod.Type().NumIn() > 0 {
			returns = vmethod.Call([]reflect.Value{reflect.ValueOf(p)})
		} else {
			returns = vmethod.Call([]reflect.Value{})
		}
		for _, v := range returns {
			fmt.Printf("%v\n", v)
		}
		messageID, _ := returns[0].Interface().(pulsar.MessageID)
		err, _ := returns[1].Interface().(error)
		return messageID, err
	}
	return nil, errors.New("Not found method Send")
}
func (m *call) SendAsync(p any, callback Callback) {
	vmethod := reflect.ValueOf(m.Q).MethodByName("SendAsync")
	if vmethod.IsValid() {
		if vmethod.Type().NumIn() > 0 {
			vmethod.Call([]reflect.Value{reflect.ValueOf(p), reflect.ValueOf(callback)})
		} else {
			vmethod.Call([]reflect.Value{})
		}
	}
}

func (m *QueueBase[T]) SetTopic(topic string) {
	m.Topic = topic
}

func (m *QueueBase[T]) SetMessage(message pulsar.Message) {
	m.Message = message
}

func (m *QueueBase[T]) ReadMessage(messageID pulsar.MessageID) (pulsar.Message, error) {
	m.setClient()
	reader, err := m.Client.CreateReader(pulsar.ReaderOptions{
		Topic:                   m.Topic,
		StartMessageID:          messageID,
		StartMessageIDInclusive: true,
	})
	if err != nil {
		return nil, err
	}
	message, err := reader.Next(context.Background())
	if err != nil {
		return nil, err
	} else {
		return message, nil
	}
}

func (m *QueueBase[T]) Ack() {
	consumer, _ := m.CreateConsumer()
	consumer.Ack(m.Message)
}

func (m *QueueBase[T]) AckID(messageID pulsar.MessageID) {
	consumer, _ := m.CreateConsumer()
	consumer.AckID(messageID)
}

func (m *QueueBase[T]) NewClient(options pulsar.ClientOptions) (client pulsar.Client) {
	m.onceClient.Do(func() {
		client, err := pulsar.NewClient(options)
		if err != nil {
			log.Fatalf("Could not instantiate pulsar client: %v", err)
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

func initProducer[T any](m *QueueBase[T], p T) {
	m.setClient()
	m.createProducer()
	if m.ProducerMessage == nil {
		m.ProducerMessage = new(pulsar.ProducerMessage)
	}
	var value any
	if v := reflect.ValueOf(p).Kind(); v == reflect.Struct {
		value = &p
	} else {
		value = p
	}
	m.ProducerMessage.Value = value
}

func (m *QueueBase[T]) Set(pm *pulsar.ProducerMessage) *QueueBase[T] {
	m.ProducerMessage = pm
	return m
}

func (m *QueueBase[T]) Send(p T) (pulsar.MessageID, error) {
	initProducer(m, p)
	messageID, err := m.Producer.Send(context.Background(), m.ProducerMessage)
	if err != nil {
		log.Fatal(err)
		return nil, err
	}
	return messageID, nil
}

func (m *QueueBase[T]) SendAsync(p T, cb Callback) {
	initProducer(m, p)
	m.Producer.SendAsync(context.Background(), m.ProducerMessage, cb)
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

func Subscribe[T any](p Queueable[T]) {
	go func(m Queueable[T]) {
		defer func() {
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
