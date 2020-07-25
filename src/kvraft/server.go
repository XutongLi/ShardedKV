package kvraft

import (
	"../labgob"
	"../labrpc"
	"../raft"
	"bytes"
	"log"
	"sync"
	"sync/atomic"
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
	Ch 			chan Record
}

type KVServer struct {
	mu      			sync.Mutex
	me      			int
	rf      			*raft.Raft
	applyCh 			chan raft.ApplyMsg
	dead    			int32 // set by Kill()

	maxraftstate 		int // snapshot if log grows this big
	killChan 			chan bool

	// Your definitions here.
	kvStorage			map[string]string		// kv storage
	lastAppliedIndex	int
	clientOpId 			map[int64]int64
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
		Ch:    	  make(chan Record),
	}
	ok, record := kv.execRaft(op)
	reply.Err = record.Err
	if !ok {
		reply.Err = ErrWrongLeader
	}
	reply.Value = record.Value
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
		Ch:    	  make(chan Record),
	}
	ok, record := kv.execRaft(op)
	reply.Err = record.Err
	if !ok {
		reply.Err = ErrWrongLeader
	}
}

// append log entry to raft, and check the applied result
func (kv *KVServer) execRaft(op Op) (bool, Record) {
	kv.mu.Lock()

	if op.OpId > 0 && kv.isRepeated(op.ClientId, op.OpId, false) {
		defer kv.mu.Unlock()
		return true, Record{}
	}

	_, _, isLeader := kv.rf.Start(op)		// give an op to raft
	if !isLeader {
		defer kv.mu.Unlock()
		return false, Record{}
		}
	kv.mu.Unlock()

	//kv.saveSnapShot()	// check raft size after log is appended

	select {
		case record := <- op.Ch:
			return true, record
		case <- time.After(time.Millisecond * APPLYCHECKTIMEOUT):

	}
	return false, Record{}
}

// true if is repeated
func (kv *KVServer) isRepeated(clientId int64, OpId int64, update bool) bool {
	res := false
	index, ok := kv.clientOpId[clientId]
	if ok {
		res = index >= OpId
	}
	if update && !res {
		kv.clientOpId[clientId] = OpId
	}
	return res
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
	kv.killChan <- true
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}


// get applyMsg gouroutine
func (kv *KVServer) applyCommitEntry() {
	for {
		select {
			case <-kv.killChan:
				return
			case applyMsg := <-kv.applyCh:
				if !applyMsg.CommandValid {
					kv.updateSnapshot(applyMsg.Snapshot)
					kv.mu.Lock()
					kv.lastAppliedIndex = applyMsg.CommandIndex
					kv.mu.Unlock()
					// kv.saveSnapShot()
					continue
				}

				op := applyMsg.Command.(Op)
				kv.mu.Lock()
				kv.lastAppliedIndex = applyMsg.CommandIndex
				record := Record{}
				if op.OpType == GET {
					value, hasKey := kv.kvStorage[op.Key]
					if !hasKey {
						record.Value = ""
						record.Err = ErrNoKey
					} else {
						record.Value = value
						record.Err = OK
					}
				} else if op.OpType == PUT {
					if !kv.isRepeated(op.ClientId, op.OpId, true) {
						kv.kvStorage[op.Key] = op.Value
					}
					record.Err = OK
				} else if op.OpType == APPEND {
					if !kv.isRepeated(op.ClientId, op.OpId, true) {
						value, hasKey := kv.kvStorage[op.Key]
						if !hasKey {
							kv.kvStorage[op.Key] = op.Value
						} else {
							kv.kvStorage[op.Key] = value + op.Value
						}
					}
					record.Err = OK
				} else {
					DPrintf("[Server %d] applied a unkown op", kv.me)
				}
				kv.mu.Unlock()

				select {
					case op.Ch <- record:
					default:
				}
				kv.saveSnapShot()

		}
	}
}

// snapshot

func (kv *KVServer) saveSnapShot() {
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		return
	}
	kv.mu.Lock()
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.kvStorage)
	encoder.Encode(kv.clientOpId)
	snapshot := writer.Bytes()
	lastAppliedIndex := kv.lastAppliedIndex
	kv.mu.Unlock()
	kv.rf.SaveStateAndSnapshot(lastAppliedIndex, snapshot)
}

func (kv *KVServer) updateSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	kvStorage := make(map[string]string)
	clientOpId := make(map[int64]int64)
	if decoder.Decode(&kvStorage) != nil ||
		decoder.Decode(&clientOpId) != nil {
		DPrintf("[Server %d] decode fails", kv.me)
	} else {
		kv.kvStorage = kvStorage
		kv.clientOpId = clientOpId
		//kv.lastAppliedIndex = lastAppliedIndex
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
	kv.killChan = make(chan bool)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvStorage = make(map[string]string)
	kv.lastAppliedIndex = 0
	kv.clientOpId = make(map[int64]int64)
	kv.updateSnapshot(kv.rf.ReadSnapshot())

	go kv.applyCommitEntry()
	return kv
}
