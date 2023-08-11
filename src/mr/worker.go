package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	lastActiveTime := time.Now()
	for {
		args := Args{}
		reply := Reply{}
		ok := call("Coordinator.Handle", &args, &reply)
		if ok {
			lastActiveTime = time.Now()
			if reply.TaskType == 0 {
				//log.Printf("get map task\n")
				//log.Printf("reply.FileName: %v, number = %v\n", reply.FileName, reply.Number)
				Map(reply.FileName, reply.Number, reply.NReduce, mapf)
			} else if reply.TaskType == 1 {
				//log.Printf("get reduce task\n")
				Reduce(reply.BucketId, reducef)
			} else if reply.TaskType == 2 {
				//log.Printf("all tasks finished, stop worker!\n")
				return
			} else {
				//log.Printf("no available task, sleep\n")
				time.Sleep(100 * time.Millisecond)
			}
		} else {
			fmt.Printf("call failed!\n")

			if time.Now().Sub(lastActiveTime) >= 10*time.Second {
				fmt.Printf("coordinator inactive over 10 seconds, stop worker!\n")
				return
			}
		}
	}
}

func Map(inputFileName string, number int, nReduce int, mapf func(string, string) []KeyValue) {
	inputFile, err := os.Open(inputFileName)
	if err != nil {
		log.Fatalf("cannot open %v", inputFileName)
	}
	content, err := io.ReadAll(inputFile)
	if err != nil {
		log.Fatalf("cannot read %v", inputFileName)
	}
	inputFile.Close()

	intermediate := mapf(inputFileName, string(content))

	outputFiles := make([]*os.File, nReduce)
	encoders := make([]*json.Encoder, nReduce)
	for bucketId := 0; bucketId < nReduce; bucketId++ {
		outputFiles[bucketId], _ = os.Create(fmt.Sprintf("%v-%v-%v", time.Now(), number, bucketId))
		encoders[bucketId] = json.NewEncoder(outputFiles[bucketId])
	}
	for _, kv := range intermediate {
		bucketId := ihash(kv.Key) % nReduce
		if err := encoders[bucketId].Encode(&kv); err != nil {
			fmt.Printf("encode error, error = %s\n", err.Error())
		}
	}
	for bucketId, outputFile := range outputFiles {
		os.Rename(outputFile.Name(), fmt.Sprintf("mr-%v-%v", number, bucketId))
		outputFile.Close()
	}

	call("Coordinator.Finish", &FinishArgs{TaskType: 0, Number: number}, &FinishReply{})
}

func Reduce(bucketId int, reducef func(string, []string) string) {
	dirEntries, err := os.ReadDir(".")
	if err != nil {
		log.Fatal(err)
	}

	intermediateMap := make(map[string][]string)
	for _, dirEntry := range dirEntries {
		if dirEntry.IsDir() || !strings.HasPrefix(dirEntry.Name(), "mr-") {
			continue
		}

		fileName := dirEntry.Name()
		splits := strings.Split(fileName, "-")
		if len(splits) != 3 {
			continue
		}
		if n, err := strconv.Atoi(splits[2]); err != nil || n != bucketId {
			continue
		}

		//log.Printf("Reduce fileName = %v", fileName)
		file, err := os.Open(fileName)
		if err != nil {
			log.Fatalf("cannot open %v", fileName)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}

			intermediateMap[kv.Key] = append(intermediateMap[kv.Key], kv.Value)
		}

		file.Close()
	}

	outputFile, _ := os.Create(fmt.Sprintf("mr-out-%d", bucketId))
	for key, values := range intermediateMap {
		// for the remain part
		output := reducef(key, values)
		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(outputFile, "%v %v\n", key, output)
	}
	outputFile.Close()

	call("Coordinator.Finish", &FinishArgs{TaskType: 1, BucketId: bucketId}, &FinishReply{})
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
