package queue

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type call struct {
	Q any
}

var registers map[string]any
var registersOnce sync.Once

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

func Call(key string) *call {
	v, ok := registers[key]
	if ok {
		c := new(call)
		c.Q = v
		c.ResetSet()
		return c
	} else {
		fmt.Printf("Not initialized %s struct\n", key)
	}
	return nil
}

func (m *call) ResetSet() *call {
	vmethod := reflect.ValueOf(m.Q).MethodByName("ResetSet")
	println("*********************ResetSet****************************")
	println(vmethod.IsValid(), vmethod.Type().NumIn())
	println("*********************ResetSet****************************")
	if vmethod.IsValid() {
		vmethod.Call([]reflect.Value{})
	}
	return m
}

func (m *call) Set(pm pulsar.ProducerMessage) *call {
	vmethod := reflect.ValueOf(m.Q).MethodByName("Set")
	if vmethod.IsValid() {
		if vmethod.Type().NumIn() > 0 {
			vmethod.Call([]reflect.Value{reflect.ValueOf(pm)})
		}
	}
	return m
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
