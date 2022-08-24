package queue

import (
	"errors"
	"fmt"
	"reflect"
	"regexp"
	"sync"

	"github.com/apache/pulsar-client-go/pulsar"
)

type CallQueue struct {
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

func Call(key string) *CallQueue {
	v, ok := registers[key]
	if ok {
		c := &CallQueue{Q: v}
		return c.ResetSet()
	} else {
		fmt.Printf("Not initialized %s struct\n", key)
	}
	return nil
}

func CallMethod(q any, methodName string) (vm reflect.Value, ok bool) {
	vm = reflect.ValueOf(q).MethodByName(methodName)
	ok = vm.IsValid()
	return
}

func (m *CallQueue) ResetSet() *CallQueue {
	if vm, ok := CallMethod(m.Q, "ResetSet"); ok {
		vm.Call([]reflect.Value{})
	}
	return m
}

func (m *CallQueue) Set(pm pulsar.ProducerMessage) *CallQueue {
	if vm, ok := CallMethod(m.Q, "Set"); ok {
		if vm.Type().NumIn() > 0 {
			vm.Call([]reflect.Value{reflect.ValueOf(pm)})
		}
	}
	return m
}

func (m *CallQueue) Send(p any) (pulsar.MessageID, error) {
	if vm, ok := CallMethod(m.Q, "Send"); ok {
		var returns []reflect.Value
		if vm.Type().NumIn() > 0 {
			returns = vm.Call([]reflect.Value{reflect.ValueOf(p)})
		} else {
			returns = vm.Call([]reflect.Value{})
		}
		messageID, _ := returns[0].Interface().(pulsar.MessageID)
		err, _ := returns[1].Interface().(error)
		return messageID, err
	}
	return nil, errors.New("Not found method Send")
}

func (m *CallQueue) SendAsync(p any, callback Callback) {
	if vm, ok := CallMethod(m.Q, "SendAsync"); ok {
		if vm.Type().NumIn() > 0 {
			vm.Call([]reflect.Value{reflect.ValueOf(p), reflect.ValueOf(callback)})
		} else {
			vm.Call([]reflect.Value{})
		}
	}
}

func (m *CallQueue) ReadMessage(messageID pulsar.MessageID) (pulsar.Message, error) {
	if vm, ok := CallMethod(m.Q, "ReadMessage"); ok {
		if vm.Type().NumIn() > 0 {
			returns := vm.Call([]reflect.Value{reflect.ValueOf(messageID)})
			message, _ := returns[0].Interface().(pulsar.Message)
			err, _ := returns[1].Interface().(error)
			return message, err
		}
	}
	return nil, errors.New("Not found method ReadMessage")
}

func (m *CallQueue) AckID(messageID pulsar.MessageID) {
	if vm, ok := CallMethod(m.Q, "AckID"); ok {
		if vm.Type().NumIn() > 0 {
			vm.Call([]reflect.Value{reflect.ValueOf(messageID)})
		}
	}
}
