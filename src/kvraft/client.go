package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	clientId int64
	seq      int64
	leaderId int
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
	ck.seq = 0
	ck.leaderId = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.seq += 1

	for {
		args := GetArgs{
			Key:      key,
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
		reply := GetReply{}
		done := make(chan bool)

		DPrintf("call KVServer[%d].Get start, args = %s\n", ck.leaderId, &args)
		go func() {
			done <- ck.servers[ck.leaderId].Call("KVServer.Get", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call KVServer[%d].Get timeout, retry, args = %s\n", ck.leaderId, &args)
			continue
		}

		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("call KVServer[%d].Get failed, retry, reply = %s\n", ck.leaderId, &reply)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call KVServer[%d].Get finished, ErrWrongTerm, retry\n", ck.leaderId)
			continue
		}

		return reply.Value
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.seq += 1

	for {
		args := PutAppendArgs{
			Key:      key,
			Value:    value,
			Op:       op,
			ClientId: ck.clientId,
			Seq:      ck.seq,
		}
		reply := PutAppendReply{}
		done := make(chan bool)

		DPrintf("call KVServer[%d].PutAppend start, args = %s\n", ck.leaderId, &args)
		go func() {
			done <- ck.servers[ck.leaderId].Call("KVServer.PutAppend", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call KVServer[%d].PutAppend timeout, retry, args = %s\n", ck.leaderId, &args)
			continue
		}

		if !ok || reply.Err == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("call KVServer[%d].PutAppend failed, retry, reply = %s\n", ck.leaderId, &reply)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call KVServer[%d].PutAppend finished, ErrWrongTerm, retry\n", ck.leaderId)
			continue
		}

		return
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
