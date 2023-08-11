package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type TaskStatus struct {
	// 0: unassigned, 1: running, 2:finished
	status     int
	assignTime time.Time
}

type Coordinator struct {
	// 0: map, 1: reduce, 2: finished
	stage int

	mapTaskStatus map[int]*TaskStatus

	reduceTaskStatus map[int]*TaskStatus

	fileNames []string

	nReduce int

	mutex sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Handle(args *Args, reply *Reply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	c.checkStatus()
	if c.stage == 0 {
		for k, v := range c.mapTaskStatus {
			if v.status == 0 {
				//log.Printf("assign map task, file name: %v, number = %v\n", c.fileNames[k], k)
				*reply = Reply{
					TaskType: 0,
					FileName: c.fileNames[k],
					Number:   k,
					NReduce:  c.nReduce,
				}
				c.mapTaskStatus[k] = &TaskStatus{1, time.Now()}
				return nil
			}
		}

		// all tasks running, or partial tasks finished
		reply.TaskType = 3
	} else if c.stage == 1 {
		for k, v := range c.reduceTaskStatus {
			if v.status == 0 {
				//log.Printf("assign reduce task, bucketId: %v\n", k)
				*reply = Reply{
					TaskType: 1,
					BucketId: k,
				}
				c.reduceTaskStatus[k] = &TaskStatus{1, time.Now()}
				return nil
			}
		}

		// all tasks running, or partial tasks finished
		reply.TaskType = 3
	} else {
		// all task finished
		reply.TaskType = 2
	}

	return nil
}

func (c *Coordinator) checkStatus() {
	for k, v := range c.mapTaskStatus {
		if v.status == 1 {
			if time.Now().Sub(v.assignTime) > 10*time.Second {
				//log.Printf("map task %v died, reset its status to unassigned", k)
				c.mapTaskStatus[k].status = 0
			}
		}
	}

	for k, v := range c.reduceTaskStatus {
		if v.status == 1 {
			if time.Now().Sub(v.assignTime) > 10*time.Second {
				//log.Printf("reduce task %v died, reset its status to unassigned", k)
				c.reduceTaskStatus[k].status = 0
			}
		}
	}
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishArgs) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	if args.TaskType == 0 {
		c.mapTaskStatus[args.Number].status = 2

		allFinished := true
		for _, v := range c.mapTaskStatus {
			if v.status != 2 {
				allFinished = false
				break
			}
		}
		if allFinished {
			c.stage = 1
		}
	} else {
		c.reduceTaskStatus[args.BucketId].status = 2

		allFinished := true
		for _, v := range c.reduceTaskStatus {
			if v.status != 2 {
				allFinished = false
				break
			}
		}
		if allFinished {
			c.stage = 2
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.stage == 2
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	mapTaskStatus := make(map[int]*TaskStatus)
	for i := range files {
		//log.Printf("MakeCoordinator, file name: %v\n", files[i])
		mapTaskStatus[i] = &TaskStatus{}
	}

	reduceTaskStatus := make(map[int]*TaskStatus)
	for i := 0; i < nReduce; i++ {
		reduceTaskStatus[i] = &TaskStatus{}
	}

	c := Coordinator{
		stage:            0,
		mapTaskStatus:    mapTaskStatus,
		reduceTaskStatus: reduceTaskStatus,
		fileNames:        files,
		nReduce:          nReduce,
	}
	c.server()
	return &c
}
