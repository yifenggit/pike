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
	queue.Subscribe[pb.Student](jobs.NewStudent())
	queue.Subscribe[pb.Teacher](jobs.NewTeacher())
	queue.Subscribe[int32](jobs.NewCalc())
	queue.Subscribe[string](jobs.NewHello())
	queue.Wait()

	// TODO 1.支持设置延迟执行  2.DeleteMessage 3.ReadMessage 4.解决递归调用

}
