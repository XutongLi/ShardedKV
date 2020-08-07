package shardmaster

import (
	"../raft"
	"../labrpc"
	"math"
	"sync"
	"../labgob"
	"time"
)


type ShardMaster struct {
	mu      	sync.Mutex
	me      	int
	rf      	*raft.Raft
	applyCh 	chan raft.ApplyMsg

	configs 	[]Config 			// indexed by config num
	killChan 	chan bool
	clientOpId 	map[int64]int64		// <clientId, max OpId of this client >
	opChan 		map[int]chan Op		// <log index, chan>
}


type Op struct {
	OpType 		string
	OpId		int64
	ClientId 	int64
	Args 		interface{}	// QueryQrgs\JoinArgs\LeaveArgs\MoveArgs
}


func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		OpType:   JOIN,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Args:     *args,
	}
	reply.WrongLeader = true
	getOp, success := sm.execRaft(op)
	if success && sm.equalTo(op, getOp) {
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		OpType:   LEAVE,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Args:     *args,
	}
	reply.WrongLeader = true
	getOp, success := sm.execRaft(op)
	if success && sm.equalTo(op, getOp) {
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		OpType:   MOVE,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Args:     *args,
	}
	reply.WrongLeader = true
	getOp, success := sm.execRaft(op)
	if success && sm.equalTo(op, getOp) {
		reply.WrongLeader = false
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		OpType:   QUERY,
		OpId:     args.OpId,
		ClientId: args.ClientId,
		Args:     *args,
	}
	reply.WrongLeader = true
	getOp, success := sm.execRaft(op)
	if success && sm.equalTo(op, getOp) {
		reply.WrongLeader = false
		sm.mu.Lock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			//  If the number is -1 or bigger than the biggest known configuration number, the shardmaster should reply with the latest configuration.
			reply.Config = sm.configs[len(sm.configs) - 1]
		}
		sm.mu.Unlock()
	}

}

// send command to raft, and wait for applying
func (sm *ShardMaster) execRaft(op Op) (Op, bool) {
	logIndex, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return Op{}, false
	}
	opCh := sm.putOpCh(logIndex)
	select {
	case getOp := <-opCh:
		return getOp, true
	case <- time.After(time.Duration(APPLYCHECKTIMEOUT) * time.Millisecond):
		return Op{}, false
	}
}

// set op channel by log index
func (sm *ShardMaster) putOpCh(index int) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.opChan[index]; !ok {
		sm.opChan[index] = make(chan Op, 1)
	}
	return sm.opChan[index]
}

//Check that a and b are equal
func (sm *ShardMaster) equalTo(a Op, b Op) bool {
	return a.ClientId == b.ClientId && a.OpId == b.OpId  && a.OpType == b.OpType
}


//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.killChan <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// create new configuration
func (sm *ShardMaster) createNewConfig() Config {
	oldConfig := sm.configs[len(sm.configs) - 1]
	newConfig := Config{
		Num:    oldConfig.Num + 1,
		Shards: oldConfig.Shards,
		Groups: make(map[int][]string),
	}
	for gid, servers := range oldConfig.Groups {
		newConfig.Groups[gid] = append([]string{}, servers...)
	}
	return newConfig
}

// update configuration
func (sm *ShardMaster) updateConfig(opType string, args interface{}) {
	newConfig := sm.createNewConfig()
	if opType == JOIN {
		joinArgs := args.(JoinArgs)
		for gid, servers := range joinArgs.Servers {
			newConfig.Groups[gid] = append(make([]string, 0), servers...)
			sm.rebalance(&newConfig, gid, opType)
		}
	} else if opType == LEAVE {
		leaveArgs := args.(LeaveArgs)
		for _, gid := range leaveArgs.GIDs {
			delete(newConfig.Groups, gid)
			sm.rebalance(&newConfig, gid, opType)
		}
	} else if opType == MOVE {
		moveArgs := args.(MoveArgs)
		if _, has := newConfig.Groups[moveArgs.GID]; has {
			newConfig.Shards[moveArgs.Shard] = moveArgs.GID
		}
	}
	sm.configs = append(sm.configs, newConfig)
}

// re-balance shard after join and leave
func (sm *ShardMaster) rebalance(config *Config, gid int, opType string) {
	gidShard := sm.getGidShardMap(config)
	if opType == JOIN {
		avgNum := NShards / len(config.Groups)
		for i := 0; i < avgNum; i += 1 {
			maxGid := sm.maxShardGid(gidShard)
			config.Shards[gidShard[maxGid][0]] = gid
			gidShard[maxGid] = gidShard[maxGid][1: ]
		}
	} else if opType == LEAVE {
		shards, has := gidShard[gid]
		if !has {
			return
		}
		delete(gidShard, gid)
		if len(config.Groups) == 0 {
			config.Shards = [NShards]int{}
			return
		}
		for _, shard := range shards {
			minGid := sm.minShardGid(gidShard)
			config.Shards[shard] = minGid
			gidShard[minGid] = append(gidShard[minGid], shard)
		}
	}
}

// get <gid, shard list> map
func (sm *ShardMaster) getGidShardMap(config *Config) map[int][]int {
	gidShard := map[int][]int{}
	for gid, _ := range config.Groups {
		gidShard[gid] = []int{}
	}
	for shard, gid := range config.Shards {
		gidShard[gid] = append(gidShard[gid], shard)
	}
	return gidShard
}

// get gid with the max shard num
func (sm *ShardMaster) maxShardGid(gidShard map[int][]int) int {
	maxi := -1
	maxGid := 0
	for gid, shards := range gidShard {
		if maxi < len(shards) {
			maxi = len(shards)
			maxGid = gid
		}
	}
	return maxGid
}

// get gid with the min shard num
func (sm *ShardMaster) minShardGid(gidShard map[int][]int) int {
	mini := math.MaxInt32
	minGid := 0
	for gid, shards := range gidShard {
		if mini > len(shards) {
			mini = len(shards)
			minGid = gid
		}
	}
	return minGid
}

// apply committed log entry
func (sm *ShardMaster) applyCommitEntry() {
	for {
		select {
		case <- sm.killChan:
			return
		case applyMsg := <-sm.applyCh:
			if !applyMsg.CommandValid {
				continue
			}
			sm.mu.Lock()
			op := applyMsg.Command.(Op)
			maxOpId, hasClient := sm.clientOpId[op.ClientId]
			if !hasClient || op.OpId > maxOpId {
				sm.updateConfig(op.OpType, op.Args)
				sm.clientOpId[op.ClientId] = op.OpId
			}
			sm.mu.Unlock()
			opCh := sm.putOpCh(applyMsg.CommandIndex)
			// send op to opCh
			select {
			case <- opCh:	// clear the buffered channel
			default:
			}
			opCh <- op		// send op
		}
	}
}
//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	labgob.Register(QueryArgs{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.killChan = make(chan bool, 1)
	sm.opChan = make(map[int]chan Op)
	sm.clientOpId = make(map[int64]int64)

	go sm.applyCommitEntry()
	return sm
}
