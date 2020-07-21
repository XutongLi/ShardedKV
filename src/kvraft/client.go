package kvraft

import (
	"../labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)



type Clerk struct {
	servers 	[]*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId	int64		// Id of this client
	leaderId	int			// Id of the leader server this client connect to (or the previous leaderId)
	opId 		int64		// current operation Id of this client
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// init clerk
func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand()
	ck.leaderId = 0
	ck.opId = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	val := ""
	getArgs := GetArgs{
		Key:      	key,
		ClientId: 	ck.clientId,
		OpId:		atomic.AddInt64(&ck.opId, 1),
	}
	for {
		getReply := GetReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.Get", &getArgs, &getReply)
		if !ok || getReply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)	// choose another server
			continue
		}
		DPrintf("[Client %d] <Resp> Get op gets resp from server %d", ck.clientId, ck.leaderId)
		if getReply.Err == ErrNoKey {
			val = ""
			DPrintf("[Client %d] <Resp> Get op -- no key %v", ck.clientId, key)
		} else {	// getReply.Err == OK
			val = getReply.Value
			DPrintf("[Client %d] <Resp> Get op -- <%v, %v>", ck.clientId, key, val)
		}
		break
	}
	return val
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	putAppendArgs := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		OpId:     atomic.AddInt64(&ck.opId, 1),
	}
	for	{
		putAppendReply := PutAppendReply{}
		ok := ck.servers[ck.leaderId].Call("KVServer.PutAppend", &putAppendArgs, &putAppendReply)
		if !ok || putAppendReply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			continue
		}
		DPrintf("[Client %d] <Resp> %v op gets resp from server %d", ck.clientId, op, ck.leaderId)
		if putAppendReply.Err == OK {
			DPrintf("[Client %d] <Resp> %v <%v, %v> success", ck.clientId, op, key, value)
		} else {
			DPrintf("[Client %d] <Resp> %v <%v, %v> fails", ck.clientId, op, key, value)
		}
		break
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
