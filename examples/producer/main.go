package main

import (
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/yifenggit/pike/examples/jobs"
	"github.com/yifenggit/pike/examples/jobs/protos/pb"
	"github.com/yifenggit/pike/queue"
)

func main() {
	queue.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	for i := 1; i <= 100; i++ {
		jobs.NewStudent().Async(pb.Student{Name: "xiaoming", Age: int32(i)})
		jobs.NewTeacher().Async(pb.Teacher{Name: "xiaowang", Age: int32(i)})
		jobs.NewCalc().Async(int32(i))
		jobs.NewHello().Async("Hello World!")
	}
}
