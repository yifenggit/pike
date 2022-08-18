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
	var i int32
	for i = 0; i < 100; i++ {
		jobs.NewStudent().Async(pb.Student{Name: "xiaoming", Age: i})
		jobs.NewTeacher().Async(pb.Teacher{Name: "xiaowang", Age: i})
	}
}
