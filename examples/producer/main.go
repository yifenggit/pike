package main

import (
	"encoding/base64"
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
	messageID, _ := queue.Call("Hello").Set(pulsar.ProducerMessage{DeliverAfter: 5 * time.Second}).Send("Hello World Wait 5s!")
	message, _ := queue.Call("Hello").ReadMessage(messageID)
	messageID2, _ := pulsar.DeserializeMessageID(message.ID().Serialize())

	pulsarID := base64.StdEncoding.EncodeToString(messageID.Serialize())
	pulsarID2 := base64.StdEncoding.EncodeToString(messageID2.Serialize())

	bys, _ := base64.StdEncoding.DecodeString(pulsarID)
	msgID, _ := pulsar.DeserializeMessageID(bys)

	bys2, _ := base64.StdEncoding.DecodeString(pulsarID2)
	msgID2, _ := pulsar.DeserializeMessageID(bys2)
	println("*********************main ReadMessage****************************")
	fmt.Printf("%v %v %v %v %v %v %v %v\n", messageID, messageID2, messageID == messageID2, pulsarID, pulsarID2, pulsarID == pulsarID2, msgID, msgID2)
	println("*********************main ReadMessage****************************")
	queue.Call("Hello").AckID(msgID2)
	// for i := 1; i <= 1; i++ {
	// 	jobs.NewStudent().Send(pb.Student{Name: "Jennie", Age: 20})
	// 	queue.Call("Teacher").Send(pb.Teacher{Name: "Tom", Age: 25})
	// 	queue.Call("Calc").Send(int32(i))
	// 	queue.Call("Hello").Send("Hello World More!")
	// 	// queue.Call("Hello").SendAsync("Hello World!", func(mi pulsar.MessageID, pm *pulsar.ProducerMessage, err error) {
	// 	// 	println("*********************Async Callback****************************")
	// 	// 	fmt.Printf("%v\n", mi)
	// 	// 	println("*********************Async Callback****************************")
	// 	// })
	// }
	// // queue.Wait() // SendAsync 调用的时候取消注释
}
