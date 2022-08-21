package jobs

import (
	"fmt"
	"sync"

	"github.com/yifenggit/pike/examples/jobs/protos/pb"
	"github.com/yifenggit/pike/queue"
)

type Teacher[T any] struct {
	queue.QueueBase[T]
}

var teacher *Teacher[pb.Teacher]
var teacherOnce sync.Once

func NewTeacher() *Teacher[pb.Teacher] {
	teacherOnce.Do(func() {
		teacher = new(Teacher[pb.Teacher])
		teacher.SetTopic(queue.StructNameShort(teacher))
	})
	return teacher
}

func (m *Teacher[T]) Perform(p pb.Teacher) {
	println("*********************************************************")
	fmt.Printf("Teacher Result: Name:%s\n Age:%d \n", p.Name, p.Age)
	println("*********************************************************")
}
