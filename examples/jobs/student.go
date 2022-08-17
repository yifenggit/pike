package jobs

import (
	"fmt"
	"sync"

	"github.com/yifenggit/pike/examples/jobs/protos/pb"
	"github.com/yifenggit/pike/queue"
)

type Student[T any] struct {
	queue.QueueBase[T]
}

var student *Student[pb.Student]
var studentOnce sync.Once

func NewStudent() *Student[pb.Student] {
	studentOnce.Do(func() {
		student = new(Student[pb.Student])
		student.SetTopic("student")
	})
	return student
}

func (m *Student[T]) Perform(p pb.Student) {
	println("*********************************************************")
	fmt.Printf("Student Result: Name:%s\n Age:%d \n", p.Name, p.Age)
	println("*********************************************************")
}
