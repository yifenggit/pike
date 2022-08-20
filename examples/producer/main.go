package main

import (
	"fmt"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/yifenggit/pike/examples/jobs"
	"github.com/yifenggit/pike/queue"
)

func main() {
	queue.NewClient(pulsar.ClientOptions{
		URL:               "pulsar://localhost:6650",
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	queue.Register(jobs.NewStudent(), jobs.NewTeacher(), jobs.NewCalc(), jobs.NewHello())
	for i := 1; i <= 100; i++ {
		queue.Call("Hello").SendAsync("Hello World!", func(mi pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
			println("*************************************************")
			fmt.Printf("%v\n", mi)
			println("*************************************************")
		})
	}
	queue.Wait()
}
