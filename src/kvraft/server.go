package kvraft

import (
	"../labgob"
	"../labrpc"
	"log"
	"../raft"
	"sync"
	"sync/atomic"
	"fmt"
	"time"
)

const Debug = 1

// print debug log
func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

const (
	PUT		= "Put"
	APPEND	= "Append"
	GET 	= "Get"
	APPLYCHECKINTERVAL	= 10	// time interval of apply message check
	APPLYCHECKTIMEOUT = 700		// timeout of apply message check
)

// entry of historical records
type Record struct {
	Err		Err
	Value 	string
}


type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key 		string
	Value 		string
	OpType 		string
	OpId		int64
	ClientId 	int64
}

type KVServer struct {
	mu      			sync.Mutex
	me      			int
	rf      			*raft.Raft
	applyCh 			chan raft.ApplyMsg
	dead    			int32 // set by Kill()

	maxraftstate 		int // snapshot if log grows this big

	// Your definitions here.
	kvStorage			map[string]string		// kv storage
	lastAppliedIndex	int
	clientOpRecord 		map[string]Record		// historical records <clientId-opId, record>
}

// Get RPC Handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("[Server %d] get GET request from %d, key is %v", kv.me, args.ClientId, args.Key)
	op := Op{
		Key:      args.Key,
		Value:    "",
		OpType:   GET,
		OpId:     args.OpId,
		ClientId: args.ClientId,
	}
	err, value := kv.execRaft(op)
	reply.Err = err
	reply.Value = value
}

// PutAppendRPC Handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("[Server %d] get %v request from %d, <%v, %v>", kv.me, args.Op, args.ClientId, args.Key, args.Value)
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		OpId:     args.OpId,
		ClientId: args.ClientId,
	}
	err, _ := kv.execRaft(op)
	reply.Err = err
}

// append log entry to raft, and check the applied result
func (kv *KVServer) execRaft(op Op) (Err, string) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	term, isLeader := kv.rf.GetState()
	if !isLeader {
		return ErrWrongLeader, ""
	}

	clientOp := fmt.Sprintf("%v-%v", op.ClientId, op.OpId)
	if record, dup := kv.clientOpRecord[clientOp]; dup {	// check duplicate
		return record.Err, record.Value
	}
	kv.rf.Start(op)		// give an op to raft

	curTime := time.Now()
	for {
		kv.mu.Unlock()
		time.Sleep(time.Duration(APPLYCHECKINTERVAL) * time.Millisecond)
		kv.mu.Lock()

		if kv.killed() || time.Since(curTime) > time.Duration(APPLYCHECKTIMEOUT) * time.Millisecond {
			break
		}
		curTerm, _ := kv.rf.GetState()
		if curTerm != term {	// the leader has changed
			break
		}
		record, ok := kv.clientOpRecord[clientOp]
		if ok { 		// op result has applied
			return record.Err, record.Value
		}
	}
	return ErrWrongLeader, ""
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


// get applyMsg gouroutine
func (kv *KVServer) applyCommitEntry() {
	for {
		applyMsg := <-kv.applyCh
		kv.lastAppliedIndex = applyMsg.CommandIndex
		op := applyMsg.Command.(Op)

		// apply op to kv storage
		kv.mu.Lock()
		clientOp := fmt.Sprintf("%v-%v", op.ClientId, op.OpId)
		if _, dup := kv.clientOpRecord[clientOp]; !dup {
			if op.OpType == GET {
				value, hasKey := kv.kvStorage[op.Key]
				if !hasKey {
					kv.clientOpRecord[clientOp] = Record{ErrNoKey, ""}
				} else {
					kv.clientOpRecord[clientOp] = Record{OK, value}
				}
				DPrintf("[Server %d] apply GET, key - %v", kv.me, op.Key)
			} else if op.OpType == PUT {
				kv.kvStorage[op.Key] = op.Value
				kv.clientOpRecord[clientOp] = Record{OK, ""}
				DPrintf("[Server %d] apply PUT <%v, %v>", kv.me, op.Key, op.Value)
			} else if op.OpType == APPEND {
				value, hasKey := kv.kvStorage[op.Key]
				if !hasKey {
					kv.kvStorage[op.Key] = op.Value
				} else {
					kv.kvStorage[op.Key] = value + op.Value
				}
				kv.clientOpRecord[clientOp] = Record{OK, ""}
				DPrintf("[Server %d] apply APPEND <%v, %v>", kv.me, op.Key, op.Value)
			} else {
				DPrintf("[Server %d] applied a unkown op", kv.me)
			}
		}
		kv.mu.Unlock()
	}
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	//kv.clientLastOpId = make(map[int64]int64)
	kv.clientOpRecord = make(map[string]Record)
	kv.lastAppliedIndex = 0

	go kv.applyCommitEntry()
	return kv
}
