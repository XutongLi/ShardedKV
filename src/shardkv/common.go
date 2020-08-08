package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time raft.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.

const (
	OK             		= "OK"
	ErrNoKey       		= "ErrNoKey"
	ErrWrongGroup  		= "ErrWrongGroup"
	ErrWrongLeader 		= "ErrWrongLeader"
	PUT					= "Put"
	APPEND				= "Append"
	GET 				= "Get"
	GC 					= "GC"
	APPLYCHECKTIMEOUT 	= 700		// timeout of apply message check
	POLLSHARDMASTER		= 50
	MIGRATESHARD		= 80
	GARBAGETIME			= 100
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   		string
	Value 		string
	Op    		string // "Put" or "Append"
	ClientId	int64
	OpId 		int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key 		string
	ClientId 	int64
	OpId 		int64
}

type GetReply struct {
	Err   		Err
	Value 		string
}

type MigrateArgs struct {
	Shard 		int
	ConfigNum 	int
}

type MigrateReply struct {
	Err			Err
	Shard 		int
	ConfigNum 	int
	Data 		map[string]string
	ClientOpId 	map[int64]int64
}