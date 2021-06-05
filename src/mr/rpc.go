package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type GetJobReq struct {
	WorkerID int32
}

type GetJobResp struct {
	NReduce    int
	NMap       int
	ImBasePath string
	Job        *Job
}

type HeartBeatReq struct {
	WorkerID int32
}

type HeartBeatResp struct {
	WorkerID int32
	HaveWork bool
}

type JobSucceedReq struct {
	WorkerID int32
	Job      *Job
}

type JobSucceedResp struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
