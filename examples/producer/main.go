package main

import (
	"fmt"
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
	queue.Register(jobs.NewStudent(), jobs.NewTeacher(), jobs.NewCalc(), jobs.NewHello())
	for i := 1; i <= 1; i++ {
		jobs.NewStudent().Send(pb.Student{Name: "Jennie", Age: 20})
		queue.Call("Teacher").Send(pb.Teacher{Name: "Tom", Age: 25})
		queue.Call("Calc").Send(int32(i))
		queue.Call("Hello").Send("Hello World More!")
		queue.Call("Hello").SendAsync("Hello World!", func(mi pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
			println("*********************Async Callback****************************")
			fmt.Printf("%v\n", mi)
			println("*********************Async Callback****************************")
		})
	}
	queue.Wait()
}
