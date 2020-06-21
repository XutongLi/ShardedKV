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


type ApplyForTaskArgs struct {
	FinishedTaskType	int 	// type of finished task, set to -1 if there isn't a task finished
	FinishedIndex		int		// index of finished task, set to -1 if there isn't a task finished
}

type ApplyForTaskReply struct {
	Status				int		// 0 - success, assigned a task   1 - retry, no tasks can be assigned  2 - all tasks finished, exit
	TaskType			int		// 0 - map  1 - reduce
	FileName			string	// intput file of map task
	MapIndex 			int		// index of file, for naming the intermedia files
	NMap				int 	// num of map task (num of input files)
	NReduce				int 	// num of reduce task
	ReduceIndex			int 	// index of reduce tasks
}


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}


//
// example to show how to declare the arguments
// and reply for an RPC.
//

//type ExampleArgs struct {
//	X int
//}
//
//type ExampleReply struct {
//	Y int
//}
