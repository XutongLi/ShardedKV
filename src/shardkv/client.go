package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"../labrpc"
	"../shardmaster"
	"crypto/rand"
	"math/big"
	"time"
)

//
// which shard is a key in?
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       	*shardmaster.Clerk
	config   	shardmaster.Config
	make_end 	func(string) *labrpc.ClientEnd
	leaderId	int
	clientId 	int64
	opId 		int
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	ck.clientId = Nrand()
	ck.leaderId = 0
	ck.opId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	ck.opId += 1
	args := GetArgs{
		Key:      	key,
		ClientId: 	ck.clientId,
		OpId:	  	ck.opId,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
			for si := 0; si < len(servers); si ++ {
				svr := (si + ck.leaderId) % len(servers)
				srv := ck.make_end(servers[svr])
				var reply GetReply
				DPrintf("[Client %v] send Get", ck.clientId)
				ok := srv.Call("ShardKV.Get", &args, &reply)
				DPrintf("[Client %v] get Get Resp %v", ck.clientId, reply.Err)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					ck.leaderId = svr
					return reply.Value
				}
				if ok && (reply.Err == ErrWrongGroup) {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)	// get new congiguration
	}

	return ""
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.opId += 1
	args := PutAppendArgs{
		Key:      	key,
		Value:    	value,
		Op:       	op,
		ClientId: 	ck.clientId,
		OpId:	  	ck.opId,
	}
	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si ++ {
				svr := (si + ck.leaderId) % len(servers)
				srv := ck.make_end(servers[svr])
				var reply PutAppendReply
				DPrintf("[Client %v] send PutAppend", ck.clientId)
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				DPrintf("[Client %v] get PutAppend Resp %v", ck.clientId, reply.Err)
				if ok && reply.Err == OK {
					ck.leaderId = svr
					return
				}
				if ok && reply.Err == ErrWrongGroup {
					break
				}
				// ... not ok, or ErrWrongLeader
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
