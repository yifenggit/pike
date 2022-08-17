package main

import (
	"github.com/yifenggit/pike/examples/jobs"
	"github.com/yifenggit/pike/examples/jobs/protos/pb"
	"github.com/yifenggit/pike/queue"
)

func main() {
	queue.Subscribe[pb.Student](jobs.NewStudent())
	queue.Subscribe[pb.Teacher](jobs.NewTeacher())
	queue.Wait()
}
