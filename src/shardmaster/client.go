package shardmaster

//
// Shardmaster clerk.
//

import (
	"../labrpc"
	"time"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	servers 		[]*labrpc.ClientEnd
	clientId 		int64
	leaderId 		int
	opId 			int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.opId = 0
	return ck
}

// query configuration
func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{
		Num:      num,
		ClientId: ck.clientId,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}
	for {
		reply := QueryReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Query", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return reply.Config
	}
}

// join group with servers (GID is key of map)
func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{
		Servers:  servers,
		ClientId: ck.clientId,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}
	for {
		reply := JoinReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Join", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

// leave groups
func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{
		GIDs:     gids,
		ClientId: ck.clientId,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}
	for {
		reply := LeaveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Leave", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}

// move shard to specified group
func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{
		Shard:    shard,
		GID:      gid,
		ClientId: ck.clientId,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}
	for {
		reply := MoveReply{}
		ok := ck.servers[ck.leaderId].Call("ShardMaster.Move", &args, &reply)
		if !ok || reply.WrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			time.Sleep(100 * time.Millisecond)
			continue
		}
		return
	}
}
