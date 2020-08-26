package shardkv

/*
 * Design:
 * periodly get new configure, add it to raft log
 * * when new configure log applied, fill sendShards and getShards, delete old kv data
 * periodly get shard in getShards from other groups via RPC
 * * when get OK reply, add it to raft log
 * * when Migrate reply log applied, update DB with migrated data, delete data in getShards, update garbages
 * periodly collect garbage, send shards in garbages to other groups via RPC
 * * when server get GC RPC, add it to raft log
 * * when GC log applied, delete shards in sendShards
 */

/*
 * Challenge:
 * Garbage Collection:
 * * when new configure log applied, fill sendShards and getShards, delete old kv data
 * * when Migrate reply log applied, update DB with migrated data, delete data in getShards, update garbages
 * * when GC log applied, delete shards in sendShards
 * Keys in unaffected shards continue to execute during a configuration change
 * * data that would be got is in getShards, data that would be sent is in sendShards
 * * so the unaffected shards can continue to execute
 * Raft groups start serving shards the moment they are able to
 * * when Migrate Reply log applied, the DB will be changed, then the new shards can be served
 */

import (
	"../shardmaster"
	"../labrpc"
	"../raft"
	"strconv"
	"sync"
	"../labgob"
	"time"
	"bytes"
)

type Op struct {
	Key 		string
	Value 		string
	OpType 		string
	ClientId 	int64
	OpId 		int
}

type ShardKV struct {
	mu           	sync.Mutex
	me           	int
	rf           	*raft.Raft
	applyCh      	chan raft.ApplyMsg
	make_end     	func(string) *labrpc.ClientEnd
	gid          	int
	masters      	[]*labrpc.ClientEnd
	maxraftstate 	int // snapshot if log grows this big
	killChan 		chan bool

	mck 			*shardmaster.Clerk
	cfg 			shardmaster.Config
	persist 		*raft.Persister
	kvStorage		map[string]string		// kv storage
	clientOpId 		map[int64]int			// <clientId, max OpId of this client >
	opChan			map[int]chan Op			// <index, chan>
	lastApplied		int

	containShards	map[int]bool			// whether this server contains a shard, <shard, bool>
	sendShards 		map[int]map[int]map[string]string	// shards to be send to other raft group, < cfgNum, <shard, kv> >
	getShards		map[int]int 			// shards to be get from other raft group, <shard, config num>
	garbages		map[int]map[int]bool	// <cfgNum, shards>
}

// Get RPC Handler
func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	op := Op{
		Key:      args.Key,
		Value:    "",
		OpType:   GET,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	reply.Err = ErrWrongLeader
	getOp, _ := kv.execRaft(op)
	//if !success {
	//	return
	//}
	if kv.equalTo(op, getOp) {
		reply.Err = OK
		reply.Value = getOp.Value
		return
	}
	if getOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	}
}

// PutAppend RPC Handler
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		OpType:   args.Op,
		ClientId: args.ClientId,
		OpId:     args.OpId,
	}
	reply.Err = ErrWrongLeader
	getOp, _ := kv.execRaft(op)
	//if !success {
	//	return
	//}
	if kv.equalTo(op, getOp) {
		reply.Err = OK
		return
	}
	if getOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	}
}

// send command to raft, and wait for applying
func (kv *ShardKV) execRaft(op Op) (Op, bool) {
	logIndex, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return Op{}, false
	}
	DPrintf("[Server %d] get %v request from %d, <%v, %v>", kv.me, op, op.ClientId, op.Key, op.Value)
	opCh := kv.putOpCh(logIndex)
	select {
	case getOp := <-opCh:
		return getOp, true
	case <- time.After(time.Duration(APPLYCHECKTIMEOUT) * time.Millisecond):
		return Op{}, false
	}
}

// set op channel by log index
func (kv *ShardKV) putOpCh(index int) chan Op {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.opChan[index]; !ok {
		kv.opChan[index] = make(chan Op, 1)
	}
	return kv.opChan[index]
}

//Check that a and b are equal
func (kv *ShardKV) equalTo(a Op, b Op) bool {
	return a.ClientId == b.ClientId && a.OpId == b.OpId && a.Key == b.Key && a.OpType == b.OpType
}


//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	select {
	case <-kv.killChan:
	default:
	}
	kv.killChan <- true
}

func (kv *ShardKV) gouroutine(function func(), sleepTime int) {
	for {
		select {
		case <- kv.killChan:
			return
		default:
			function()
		}
		time.Sleep(time.Duration(sleepTime) * time.Millisecond)
	}
}

//  periodically poll the shardmaster to learn about new configurations
func (kv *ShardKV) pollShardmaster() {
	//DPrintf("[Server %v] pollShardmaster", kv.me)
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.getShards) > 0 {	// refuse to query shardmaster while there are shards to be get
		defer kv.mu.Unlock()
		return
	}
	next := kv.cfg.Num + 1
	kv.mu.Unlock()
	cfg := kv.mck.Query(next)
	if cfg.Num == next {
		DPrintf("[Server %d] add config change log", kv.me)
		kv.rf.Start(cfg)
	}
}

// periodically get shards from other raft group
func (kv *ShardKV) migrateShard() {
	//DPrintf("[Server %v] migrateShard", kv.me)
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.getShards) == 0 {
		defer kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for shard, idx := range kv.getShards {
		wait.Add(1)
		go func(shard int, cfg shardmaster.Config) {
			defer wait.Done()
			args := MigrateArgs{
				Shard:     shard,
				ConfigNum: cfg.Num,
			}
			gid := cfg.Shards[shard]
			for _, server := range cfg.Groups[gid] {
				svr := kv.make_end(server)
				reply := MigrateReply{}
				ok := svr.Call("ShardKV.ShardMigration", &args, &reply)
				if ok && reply.Err == OK {
					DPrintf("[Server %v] add new shard to raft log", kv.me)
					kv.rf.Start(reply)	// add new shards to raft log
				}
			}
		}(shard, kv.mck.Query(idx))
	}
	kv.mu.Unlock()
	wait.Wait()
}

// ShardMigration RPC Handler
// reply data in sendShards of shards indicated by args
func (kv *ShardKV) ShardMigration(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	reply.Shard = args.Shard
	reply.ConfigNum = args.ConfigNum
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Err = ErrWrongGroup
	if args.ConfigNum >= kv.cfg.Num {
		return
	}
	reply.Err = OK
	reply.Data = make(map[string]string)
	for key, val := range kv.sendShards[args.ConfigNum][args.Shard] {
		reply.Data[key] = val
	}
	reply.ClientOpId = make(map[int64]int)
	for clientId, opId := range kv.clientOpId {
		reply.ClientOpId[clientId] = opId
	}
}

// periodically collect garbage
// tell other group to remove sendShards of some shard
func (kv *ShardKV) garbageCollect() {
	//DPrintf("[Server %v] garbageCollect", kv.me)
	_, isLeader := kv.rf.GetState()
	kv.mu.Lock()
	if !isLeader || len(kv.garbages) == 0 {
		defer kv.mu.Unlock()
		return
	}
	var wait sync.WaitGroup
	for cfgNum, shards := range kv.garbages {
		for shard := range shards {
			wait.Add(1)
			go func(shard int, cfg shardmaster.Config) {
				defer wait.Done()
				args := MigrateArgs{
					Shard:     shard,
					ConfigNum: cfg.Num,
				}
				gid := cfg.Shards[shard]
				for _, server := range cfg.Groups[gid] {
					svr := kv.make_end(server)
					reply := MigrateReply{}
					ok := svr.Call("ShardKV.GarbageCollection", &args, &reply)
					if ok && reply.Err == OK {
						kv.mu.Lock()
						defer kv.mu.Unlock()
						delete(kv.garbages[cfgNum], shard)
						if len(kv.garbages[cfgNum]) == 0 {
							delete(kv.garbages, cfgNum)
						}
					}
				}
			}(shard, kv.mck.Query(cfgNum))
		}
	}
	kv.mu.Unlock()
	wait.Wait()
}

func (kv *ShardKV) GarbageCollection(args *MigrateArgs, reply *MigrateReply) {
	reply.Err = ErrWrongLeader
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sendShards[args.ConfigNum]; !ok {
		return
	}
	if _, ok := kv.sendShards[args.ConfigNum][args.Shard]; !ok {
		return
	}
	op := Op{
		Key:      strconv.Itoa(args.ConfigNum),
		Value:    "",
		OpType:   GC,
		ClientId: Nrand(),
		OpId:     args.Shard,
	}
	//reply.Err = ErrWrongLeader
	getOp, success := kv.execRaft(op)
	if !success {
		return
	}
	if kv.equalTo(op, getOp) {
		reply.Err = OK
		return
	}
	if getOp.OpType == ErrWrongGroup {
		reply.Err = ErrWrongGroup
	}
}


// get applyMsg gouroutine
func (kv *ShardKV) applyCommitEntry() {
	for {
		select {
		case <- kv.killChan:
			return
		case applyMsg := <- kv.applyCh:
			if !applyMsg.CommandValid {
				kv.updateSnapshot(applyMsg.Snapshot, applyMsg.CommandIndex)
				//DPrintf("[Server %d] lastIncludedIndex = %d", kv.me, applyMsg.CommandIndex)
				continue
			}
			kv.mu.Lock()
			DPrintf("[Server %d] apply %d, %v", kv.me, applyMsg.CommandIndex, applyMsg.Command)
			if applyMsg.CommandIndex > kv.lastApplied {
				kv.lastApplied = applyMsg.CommandIndex
			}
			kv.mu.Unlock()
			if cfg, ok := applyMsg.Command.(shardmaster.Config); ok {
				kv.execNewConfig(cfg)
			} else if migData, ok := applyMsg.Command.(MigrateReply); ok {
				kv.updateDBWithMigrateData(migData)
			} else {
				op := applyMsg.Command.(Op)
				if op.OpType == GC {
					cfgNum, _ := strconv.Atoi(op.Key)
					kv.execGC(cfgNum, op.OpId)
				} else {
					kv.applyCommand(&op)
				}
				opCh := kv.putOpCh(applyMsg.CommandIndex)
				// send op to opCh
				select {
				case <- opCh:	// clear the buffered channel
				default:
				}
				opCh <- op		// send op
			}
			go kv.saveSnapShot()
		}
	}
}

// apply Get/Put/Append Command
func (kv *ShardKV) applyCommand(op *Op) {
	shard := key2shard(op.Key)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.containShards[shard]; !ok {
		op.OpType = ErrWrongGroup
	} else {
		DPrintf("[Server %v] not wrong group", kv.me)
		maxOpId, hasClient := kv.clientOpId[op.ClientId]
		if !hasClient || op.OpId > maxOpId {	// not repeated
			if op.OpType == PUT {
				kv.kvStorage[op.Key] = op.Value
			} else if op.OpType == APPEND {
				kv.kvStorage[op.Key] += op.Value
			}
			kv.clientOpId[op.ClientId] = op.OpId
		}
		if op.OpType == GET {
			op.Value = kv.kvStorage[op.Key]
		}
	}
}

// execute garbage collect
func (kv *ShardKV) execGC(cfgNum int, shard int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if _, ok := kv.sendShards[cfgNum]; ok {
		delete(kv.sendShards[cfgNum], shard)
		if len(kv.sendShards[cfgNum]) == 0 {
			delete(kv.sendShards, cfgNum)
		}
	}
}

// update shard to new configuration
// fill getShards and sendShards
func (kv *ShardKV) execNewConfig(cfg shardmaster.Config) {
	DPrintf("[Server %d] get new config", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if cfg.Num <= kv.cfg.Num {
		return
	}
	oldCfg, containShards := kv.cfg, kv.containShards
	kv.containShards, kv.cfg = make(map[int]bool), cfg
	for shard, gid := range cfg.Shards {
		if gid != kv.gid {
			continue
		}
		//kv.containShards[shard] = true
		if _, ok := containShards[shard]; ok || oldCfg.Num == 0 {	// existed shard
			kv.containShards[shard] = true
			delete(containShards, shard)
		} else {	// mew shard
			kv.getShards[shard] = oldCfg.Num
		}
	}
	if len(containShards) > 0 {	// containShards include shards need to be remove
		kv.sendShards[oldCfg.Num] = make(map[int]map[string]string)
		for shard := range containShards {
			outdb := make(map[string]string)
			for key, val := range kv.kvStorage {
				if key2shard(key) == shard {
					outdb[key] = val
					delete(kv.kvStorage, key)
				}
			}
			kv.sendShards[oldCfg.Num][shard] = outdb
		}
	}
}

// add new migrate data to kv
func (kv *ShardKV) updateDBWithMigrateData(migrateData MigrateReply) {
	DPrintf("[Server %v] update DB with MigrateData", kv.me)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if migrateData.ConfigNum != kv.cfg.Num - 1 {
		return
	}
	delete(kv.getShards, migrateData.Shard)
	if _, ok := kv.containShards[migrateData.Shard]; !ok {
		kv.containShards[migrateData.Shard] = true
		for key, val := range migrateData.Data {
			kv.kvStorage[key] = val
		}
		for clientId, opId := range migrateData.ClientOpId {
			kv.clientOpId[clientId] = Max(opId, kv.clientOpId[clientId])
		}
		if _, ok := kv.garbages[migrateData.ConfigNum]; !ok {
			kv.garbages[migrateData.ConfigNum] = make(map[int]bool)
		}
		kv.garbages[migrateData.ConfigNum][migrateData.Shard] = true
	}
}

// save snapshot
func (kv *ShardKV) saveSnapShot() {
	kv.mu.Lock()
	if kv.maxraftstate == -1 || kv.rf.RaftStateSize() < kv.maxraftstate {
		defer kv.mu.Unlock()
		return
	}
	DPrintf("[Server %d] save snapshot, lastIncludedIndex = %d", kv.me, kv.lastApplied)
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(kv.kvStorage)
	encoder.Encode(kv.clientOpId)
	encoder.Encode(kv.lastApplied)
	encoder.Encode(kv.sendShards)
	encoder.Encode(kv.getShards)
	encoder.Encode(kv.containShards)
	encoder.Encode(kv.garbages)
	snapshot := writer.Bytes()
	lastSnapshotIndex := kv.lastApplied
	kv.mu.Unlock()
	kv.rf.SaveStateAndSnapshot(lastSnapshotIndex, snapshot)
}

// read snapshot
func (kv *ShardKV) updateSnapshot(snapshot []byte, lastIncludedIndex int) {
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
	clientOpId := make(map[int64]int)
	sendShards := make(map[int]map[int]map[string]string)
	getShards := make(map[int]int)
	containShards := make(map[int]bool)
	garbages := make(map[int]map[int]bool)
	var lastSnapshotIndex int

	if decoder.Decode(&kvStorage) != nil ||
		decoder.Decode(&clientOpId) != nil ||
		decoder.Decode(&lastSnapshotIndex) != nil ||
		decoder.Decode(&sendShards) != nil ||
		decoder.Decode(&getShards) != nil ||
		decoder.Decode(&containShards) != nil ||
		decoder.Decode(&garbages) != nil {
		DPrintf("[Server %d] decode fails", kv.me)
	} else {
		kv.kvStorage = kvStorage
		kv.clientOpId = clientOpId
		kv.lastApplied = lastSnapshotIndex
		kv.sendShards = sendShards
		kv.getShards = getShards
		kv.containShards = containShards
		kv.garbages = garbages
		DPrintf("[Server %d] update snapshot, lastIncludedIndex = %d", kv.me, lastSnapshotIndex)
	}
}


//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	labgob.Register(Op{})
	labgob.Register(MigrateArgs{})
	labgob.Register(MigrateReply{})
	labgob.Register(shardmaster.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters
	kv.persist = persister

	// Use something like this to talk to the shardmaster:
	kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.cfg = shardmaster.Config{}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.killChan = make(chan bool, 1)
	kv.kvStorage = make(map[string]string)
	kv.opChan = make(map[int]chan Op)
	kv.clientOpId = make(map[int64]int)
	kv.lastApplied = 0
	kv.garbages = make(map[int]map[int]bool)
	kv.getShards = make(map[int]int)
	kv.sendShards = make(map[int]map[int]map[string]string)
	kv.containShards = make(map[int]bool)

	kv.updateSnapshot(kv.rf.ReadSnapshot(), 0)

	go kv.gouroutine(kv.pollShardmaster, POLLSHARDMASTER)
	go kv.gouroutine(kv.migrateShard, MIGRATESHARD)
	go kv.gouroutine(kv.garbageCollect, GARBAGETIME)
	go kv.applyCommitEntry()

	return kv
}
