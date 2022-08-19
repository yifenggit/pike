package jobs

import (
	"fmt"
	"sync"

	"github.com/yifenggit/pike/queue"
)

type Calc[T any] struct {
	queue.QueueBase[T]
}

var calc *Calc[int32]
var calcOnce sync.Once

func NewCalc() *Calc[int32] {
	calcOnce.Do(func() {
		calc = new(Calc[int32])
		calc.SetTopic("calc")
	})
	return calc
}

func (m *Calc[T]) Perform(id int32) {
	println("*********************************************************")
	fmt.Printf("Calc Result: %d \n", id)
	println("*********************************************************")
}
