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
	APPLYCHECKTIMEOUT = 700		// timeout of apply message check
)

type Op struct {
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
	killChan 			chan bool
	persist 			*raft.Persister
	lastApplied		 	int
	kvStorage			map[string]string		// kv storage
	clientOpId 			map[int64]int64			// <clientId, max OpId of this client >
	opChan				map[int]chan Op			// <index, chan>
}

// Get RPC Handler
func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:      args.Key,
		Value:    "",
		OpType:   GET,
		OpId:     args.OpId,
		ClientId: args.ClientId,
	}
	reply.ServerId = kv.me
	reply.Err = ErrWrongLeader
	getOp, success := kv.execRaft(op)
	if !success {
		return
	}
	if kv.equalTo(op, getOp) {
		reply.Err = OK
		kv.mu.Lock()
		reply.Value = kv.kvStorage[op.Key]
		kv.mu.Unlock()
	}
}

// PutAppendRPC Handler
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		OpId:     args.OpId,
		ClientId: args.ClientId,
	}
	reply.ServerId = kv.me
	reply.Err = ErrWrongLeader
	getOp, success := kv.execRaft(op)
	if !success {
		return
	}
	if kv.equalTo(op, getOp) {
		reply.Err = OK
	}
}

// send command to raft, and wait for applying
func (kv *KVServer) execRaft(op Op) (Op, bool) {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Op{}, false
	}
	//DPrintf("[Server %d] get %v request from %d, <%v, %v>", kv.me, op, op.ClientId, op.Key, op.Value)
	opCh := kv.putOpCh(logIndex)
	select {
	case getOp := <-opCh:
		return getOp, true
	case <- time.After(time.Duration(APPLYCHECKTIMEOUT) * time.Millisecond):
		return Op{}, false
	}
}

// set op channel by log index
func (kv *KVServer) putOpCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.opChan[index]; !ok {
		kv.opChan[index] = make(chan Op, 1)
	}
	return kv.opChan[index]
}

//Check that a and b are equal
func (kv *KVServer) equalTo(a Op, b Op) bool {
	return a.ClientId == b.ClientId && a.OpId == b.OpId && a.Key == b.Key && a.Value == b.Value && a.OpType == b.OpType
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
		case <- kv.killChan:
			return
		case applyMsg := <- kv.applyCh:
			if !applyMsg.CommandValid {
				kv.updateSnapshot(applyMsg.Snapshot, applyMsg.CommandIndex)
				DPrintf("[Server %d] lastIncludedIndex = %d", kv.me, applyMsg.CommandIndex)
				continue
			}
			kv.mu.Lock()
			DPrintf("[Server %d] apply %d, %v", kv.me, applyMsg.CommandIndex, applyMsg.Command)
			if applyMsg.CommandIndex > kv.lastApplied {
				kv.lastApplied = applyMsg.CommandIndex
			}
			op := applyMsg.Command.(Op)
			maxOpId, hasClient := kv.clientOpId[op.ClientId]
			if !hasClient || op.OpId > maxOpId {	// not repeated
				if op.OpType == PUT {
					kv.kvStorage[op.Key] = op.Value
				} else if op.OpType == APPEND {
					kv.kvStorage[op.Key] += op.Value
				}
				kv.clientOpId[op.ClientId] = op.OpId
			}
			kv.mu.Unlock()
			opCh := kv.putOpCh(applyMsg.CommandIndex)
			go kv.saveSnapShot()
			// send op to opCh
			select {
			case <- opCh:	// clear the buffered channel
			default:
			}
			opCh <- op		// send op
		}
	}
}

// save snapshot
func (kv *KVServer) saveSnapShot(/*lastSnapshotIndex int*/) {
	kv.mu.Lock()
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate /*|| kv.lastSnapshotIndex >= lastSnapshotIndex*/ {
		defer kv.mu.Unlock()
		return
	}
	DPrintf("[Server %d] save snapshot, lastIncludedIndex = %d", kv.me, kv.lastApplied)
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.kvStorage)
	encoder.Encode(kv.clientOpId)
	encoder.Encode(kv.lastApplied)
	snapshot := writer.Bytes()
	lastSnapshotIndex := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.SaveStateAndSnapshot(lastSnapshotIndex, snapshot)
}

// read snapshot
func (kv *KVServer) updateSnapshot(snapshot []byte, lastIncludedIndex int) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if kv.lastApplied > lastIncludedIndex {	// for linearizability
		return
	}

	reader := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(reader)
	kvStorage := make(map[string]string)
	clientOpId := make(map[int64]int64)
	var lastSnapshotIndex int

	if decoder.Decode(&kvStorage) != nil ||
		decoder.Decode(&clientOpId) != nil ||
		decoder.Decode(&lastSnapshotIndex) != nil {
		DPrintf("[Server %d] decode fails", kv.me)
	} else {
		kv.kvStorage = kvStorage
		kv.clientOpId = clientOpId
		kv.lastApplied = lastSnapshotIndex
		DPrintf("[Server %d] update snapshot, lastIncludedIndex = %d", kv.me, lastSnapshotIndex)
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
	kv.persist = persister
	kv.killChan = make(chan bool, 1)
	kv.applyCh = make(chan raft.ApplyMsg)

	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.kvStorage = make(map[string]string)
	kv.opChan = make(map[int]chan Op)
	kv.clientOpId = make(map[int64]int64)
	kv.lastApplied = 0
	kv.updateSnapshot(kv.rf.ReadSnapshot(), 0)

	go kv.applyCommitEntry()
	return kv
}
