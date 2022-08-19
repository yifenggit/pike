package jobs

import (
	"fmt"
	"sync"

	"github.com/yifenggit/pike/queue"
)

type Hello[T any] struct {
	queue.QueueBase[T]
}

var hello *Hello[string]
var helloOnce sync.Once

func NewHello() *Hello[string] {
	helloOnce.Do(func() {
		hello = new(Hello[string])
		hello.SetTopic("hello")
	})
	return hello
}

func (m *Hello[T]) Perform(id string) {
	println("*********************************************************")
	fmt.Printf("Hello Result: %s \n", id)
	println("*********************************************************")
}
