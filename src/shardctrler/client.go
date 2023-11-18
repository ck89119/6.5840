package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
	"sync"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers  []*labrpc.ClientEnd
	mu       sync.Mutex
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

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()

	for {
		args := QueryArgs{
			Num: num,
		}
		reply := QueryReply{}
		done := make(chan bool)

		DPrintf("call ShardCtrler[%d].Query start, args = %s\n", leaderId, &args)
		go func() {
			done <- ck.servers[leaderId].Call("ShardCtrler.Query", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call ShardCtrler[%d].Query timeout, retry, args = %s\n", leaderId, &args)
			continue
		}

		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Query failed, retry, args = %s\n", leaderId, &args)
			continue
		}

		DPrintf("call ShardCtrler[%d].Query finished, reply = %v\n", leaderId, reply)

		if reply.WrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Query finished, ErrWrongLeader, retry\n", leaderId)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call ShardCtrler[%d].Query finished, ErrWrongTerm, retry\n", leaderId)
			continue
		}

		DPrintf("call ShardCtrler[%d].Query finished, success\n", leaderId)

		ck.mu.Lock()
		ck.leaderId = leaderId
		ck.mu.Unlock()
		return reply.Config
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.seq += 1
	seq := ck.seq
	ck.mu.Unlock()

	for {
		args := JoinArgs{
			Servers:  servers,
			ClientId: ck.clientId,
			Seq:      seq,
		}
		reply := JoinReply{}
		done := make(chan bool)

		DPrintf("call ShardCtrler[%d].Join start, args = %s\n", leaderId, &args)
		go func() {
			done <- ck.servers[leaderId].Call("ShardCtrler.Join", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call ShardCtrler[%d].Join timeout, retry, args = %s\n", leaderId, &args)
			continue
		}

		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Join failed, retry, args = %s\n", leaderId, &args)
			continue
		}

		if reply.WrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Join finished, ErrWrongLeader, retry\n", leaderId)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call ShardCtrler[%d].Join finished, ErrWrongTerm, retry\n", leaderId)
			continue
		}

		ck.mu.Lock()
		ck.leaderId = leaderId
		ck.mu.Unlock()
		return
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.seq += 1
	seq := ck.seq
	ck.mu.Unlock()

	for {
		args := LeaveArgs{
			GIDs:     gids,
			ClientId: ck.clientId,
			Seq:      seq,
		}
		reply := LeaveReply{}
		done := make(chan bool)

		DPrintf("call ShardCtrler[%d].Leave start, args = %s\n", leaderId, &args)
		go func() {
			done <- ck.servers[leaderId].Call("ShardCtrler.Leave", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call ShardCtrler[%d].Leave timeout, retry, args = %s\n", leaderId, &args)
			continue
		}

		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Leave failed, retry, args = %s\n", leaderId, &args)
			continue
		}

		if reply.WrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Leave finished, ErrWrongLeader, retry\n", leaderId)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call ShardCtrler[%d].Leave finished, ErrWrongTerm, retry\n", leaderId)
			continue
		}

		ck.mu.Lock()
		ck.leaderId = leaderId
		ck.mu.Unlock()
		return
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.seq += 1
	seq := ck.seq
	ck.mu.Unlock()

	for {
		args := MoveArgs{
			Shard:    shard,
			GID:      gid,
			ClientId: ck.clientId,
			Seq:      seq,
		}
		reply := MoveReply{}
		done := make(chan bool)

		DPrintf("call ShardCtrler[%d].Move start, args = %s\n", leaderId, &args)
		go func() {
			done <- ck.servers[leaderId].Call("ShardCtrler.Move", &args, &reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call ShardCtrler[%d].Move timeout, retry, args = %s\n", leaderId, &args)
			continue
		}

		if !ok {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Move failed, retry, args = %s\n", leaderId, &args)
			continue
		}

		if reply.WrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call ShardCtrler[%d].Move finished, ErrWrongLeader, retry\n", leaderId)
			continue
		}

		if reply.Err == ErrWrongTerm {
			DPrintf("call ShardCtrler[%d].Move finished, ErrWrongTerm, retry\n", leaderId)
			continue
		}

		ck.mu.Lock()
		ck.leaderId = leaderId
		ck.mu.Unlock()
		return
	}
}
