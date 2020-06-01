package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net/rpc"
	"hash/fnv"
	"os"
	"io/ioutil"
	"sort"
	"time"
	"errors"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	args := ApplyForTaskArgs{-1, -1}
	for {
		reply, err := CallApplyForTask(args)
		if err != nil {
			log.Fatal("Error: ", err)
		}
		if reply.Status == 1 {	// no idle map/reduce tasks, and  there are map/reduce tasks not finished
			time.Sleep(1000 * time.Millisecond)
			args.FinishedTaskType = -1
			args.FinishedIndex = -1
		} else if reply.Status == 2 {	// all tasks are finished, worker exit
			// fmt.Println("All tasks are finished, this worker exit")
			break
		} else if reply.Status == 0 {	// assigned a task
			if reply.TaskType == 0 {
				doMap(reply.FileName, reply.MapIndex, reply.NReduce, mapf)
				args.FinishedTaskType = 0
				args.FinishedIndex = reply.MapIndex		// continue to apply for next task when current task is finished
			} else if reply.TaskType == 1 {
				doReduce(reply.ReduceIndex, reply.NMap, reducef)
				args.FinishedTaskType = 1
				args.FinishedIndex = reply.ReduceIndex
			} else {
				fmt.Println("TaskType Error!")
				break
			}
		} else {
			fmt.Println("Status Error!")
			break
		}
	}
}

// do map function
func doMap(filename string, mapIndex int, nReduce int, mapf func(string, string) []KeyValue) {
	filename = fmt.Sprintf("%v", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Map Task Error: cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Map Task Error: cannot read %v", filename)
	}
	file.Close()
	kva := mapf(filename, string(content))	// []mr.KeyValue

	writeIntermedia(kva, mapIndex, nReduce)
}

// write list(<k, v>) to temp json file
func writeIntermedia(kva []KeyValue, mapIndex int, nReduce int) {
	path := "./"
	tempFiles := make([]*os.File, nReduce)
	// create temp files to ensure that nobody observes partially written files in the presence of crashes
	for i := 0; i < nReduce; i += 1 {
		tempFileName := fmt.Sprintf("mr-%v-%v_tmp", mapIndex, i)	// map idx - reduce idx
		tempFile, err := ioutil.TempFile(path, tempFileName)
		if err != nil {
			log.Fatalf("Temp file %v create error!", tempFileName)
		}
		tempFiles[i] = tempFile
	}
	for _, kv := range kva {
		reduceIndex := ihash(kv.Key) % nReduce
		enc := json.NewEncoder(tempFiles[reduceIndex])
		err := enc.Encode(&kv)
		if err != nil {
			log.Fatalf("Temp file %v write error !", tempFiles[reduceIndex].Name())
		}
	}

	for _, f := range tempFiles {
		f.Close()
	}
	// master will rename the temp files
}

//do reduce function
func doReduce(reduceIndex int, nMap int, reducef func(string, []string) string) {
	intermediate := getIntermedia(reduceIndex, nMap)

	sort.Sort(ByKey(intermediate))
	oname := fmt.Sprintf("mr-out-%v", reduceIndex)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()
}

// get intermedia files of reduceIndex
func getIntermedia(reduceIndex int, nMap int) []KeyValue {
	kva := make([]KeyValue, 0)
	for i := 0; i < nMap; i += 1 {
		filename := fmt.Sprintf("mr-%v-%v", i, reduceIndex)
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("Intermedia file %v get error!", filename)
		}
		dec := json.NewDecoder((file))
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return kva
}

//
func CallApplyForTask(args ApplyForTaskArgs) (ApplyForTaskReply, error) {
	reply := ApplyForTaskReply{}
	if call("Master.ApplyForTask", &args, &reply) {
		return reply, nil
	}
	return reply, errors.New("Request Error!")
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func CallExample() {
//
//	// declare an argument structure.
//	args := ExampleArgs{}
//
//	// fill in the argument(s).
//	args.X = 99
//
//	// declare a reply structure.
//	reply := ExampleReply{}
//
//	// send the RPC request, wait for the reply.
//	call("Master.Example", &args, &reply)
//
//	// reply.Y should be 100.
//	fmt.Printf("reply.Y %v\n", reply.Y)
//}