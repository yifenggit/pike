package main

import (
	"github.com/yifenggit/pike/examples/jobs"
	"github.com/yifenggit/pike/examples/jobs/protos/pb"
)

func main() {
	var i int32
	// student := jobs.NewStudent()
	// teacher := jobs.NewTeacher()
	for i = 0; i < 100; i++ {
		jobs.NewStudent().Async(pb.Student{Name: "xiaoming", Age: i})
		// jobs.NewTeacher().Async(pb.Teacher{Name: "xiaowang", Age: i})
	}
}
