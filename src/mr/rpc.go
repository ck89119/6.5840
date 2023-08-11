package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type Args struct {
}

type Reply struct {
	// 0: map, 1: reduce, 2: all task finished, exit, 3: running, worker go to sleep
	TaskType int
	// for map task
	FileName string
	Number   int
	// for reduce task
	BucketId int
	NReduce  int
}

type FinishArgs struct {
	TaskType int
	// for map task
	Number int
	// for reduce task
	BucketId int
}

type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
