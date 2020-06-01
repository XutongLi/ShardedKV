package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"errors"
	"path/filepath"
)


type mapTaskDetail struct {
	filename  			string 			// name of input file
	status				int				// 0 - not started   1 - processing  2 - finished
	startTime			time.Time
}

type reduceTaskDetail struct {
	status				int				// 0 - not started   1 - processing  2 - finished
	startTime 			time.Time
}

type Master struct {
	nMap 				int				// M, num of map tasks (num of input files)
	nReduce				int				// R, num of reduce tasks
	mapTask 			[]mapTaskDetail
	reduceTask			[]reduceTaskDetail
	allMapFinished		bool			// weither all map tasks are finished
	allReduceFinished	bool			// weither all reduce tasks are finished
	mutex				sync.Mutex
}

// RPC handlers for the worker to call.
func (m *Master) ApplyForTask(args *ApplyForTaskArgs, reply *ApplyForTaskReply) error {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if args.FinishedTaskType == 0 && m.mapTask[args.FinishedIndex].status == 1 {		// finish a map task
		renameIntermedia(args.FinishedIndex, m.nReduce)
		m.mapTask[args.FinishedIndex].status = 2
		allMapFinished := true
		for _, mapDetail := range m.mapTask {
			if mapDetail.status != 2 {
				allMapFinished = false
				break
			}
		}
		m.allMapFinished = allMapFinished

	} else if args.FinishedTaskType == 1 && m.reduceTask[args.FinishedIndex].status == 1 {	//finish a reduce task
		m.reduceTask[args.FinishedIndex].status = 2
		allReduceFinished := true
		for _, reduceDetail := range m.reduceTask {
			if reduceDetail.status != 2 {
				allReduceFinished = false
				break
			}
		}
		m.allReduceFinished = allReduceFinished
	}

	if !m.allMapFinished {
		for index, mapDetail := range m.mapTask {
			if mapDetail.status == 2 {	// finished
				continue
			} else if mapDetail.status == 1 { // processing
				timeout := m.mapTask[index].startTime.Add(10 * time.Second).Before(time.Now())
				if !timeout {
					continue
				}
			}
			// mapDetail.status == 1 && timeout  ||  mapDetail.status == 0
			m.mapTask[index].status = 1
			m.mapTask[index].startTime = time.Now()
			reply.Status = 0
			reply.NMap = m.nMap
			reply.NReduce = m.nReduce
			reply.TaskType = 0
			reply.MapIndex = index
			reply.FileName = m.mapTask[index].filename
			return nil
		}
		// no idle map tasks, but some of them are not finished
		reply.Status = 1
		return nil

	} else if m.allMapFinished && !m.allReduceFinished {
		for index, reduceDetial := range m.reduceTask {
			if reduceDetial.status == 2 {	// finished
				continue
			} else if reduceDetial.status == 1 {	// processing
				timeout := m.reduceTask[index].startTime.Add(10 * time.Second).Before(time.Now())
				if !timeout {
					continue
				}
			}
			// reduceDetail.status == 1 && timeout  ||  reduceDetail.status == 0
			m.reduceTask[index].status = 1
			m.reduceTask[index].startTime = time.Now()
			reply.Status = 0
			reply.NMap = m.nMap
			reply.NReduce = m.nReduce
			reply.TaskType = 1
			reply.ReduceIndex = index
			return nil
		}
		// no idle reduce tasks, but some of them are not finished
		reply.Status = 1
		return nil

	} else if m.allMapFinished && m.allReduceFinished {
		reply.Status = 2
		return nil
	} else {
		return errors.New("Task state is wrong")
	}
}

// rename temp intermediate files of map with index mapIndex
func renameIntermedia(mapIndex int, nReduce int) {
	for i := 0; i < nReduce; i += 1 {
		tempFilePrefix := fmt.Sprintf("mr-%v-%v_tmp*", mapIndex, i)
		tempFileNames, err := filepath.Glob(tempFilePrefix)
		if err != nil {
			log.Fatalf("Find temp file prefix %v error", tempFilePrefix)
		}
		if len(tempFileNames) == 0 {
			continue
		}
		maxLenFile := tempFileNames[0]
		maxLen := int64(0)
		for _, filename := range tempFileNames {
			file, err := os.Open(filename)
			if err != nil {
				log.Fatalf("Open temp file %v error", filename)
			}
			fileInfo, err := file.Stat()
			if err != nil {
				log.Fatalf("Stat temp file %v error", filename)
			}
			if fileInfo.Size() > maxLen {	// complete file has the max length
				maxLen = fileInfo.Size()
				maxLenFile = filename
			}
			file.Close()
		}
		newFileName := fmt.Sprintf("mr-%v-%v", mapIndex, i)
		os.Rename(maxLenFile, newFileName)
	}
	//log.Printf("All temp file of map %v are renamed\n", mapIndex)
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)	// Serve creates a new goroutine for every incoming request
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := m.allMapFinished && m.allReduceFinished
	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nMap = len(files)
	m.nReduce = nReduce
	m.allMapFinished = false
	m.allReduceFinished = false
	m.mapTask = make([]mapTaskDetail, m.nMap)
	for index, file := range files {
		mapDetail := mapTaskDetail{}
		mapDetail.filename = file
		mapDetail.status = 0
		m.mapTask[index] = mapDetail
	}
	m.reduceTask = make([]reduceTaskDetail, nReduce)
	for index, _ := range m.reduceTask {
		m.reduceTask[index].status = 0
	}
	m.server()
	return &m
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
//func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
//	reply.Y = args.X + 1
//	return nil
//}
