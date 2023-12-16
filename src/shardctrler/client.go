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
	clientId int64
	seq      int64
	leaderId int
	mu       sync.Mutex
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
	ck.seq += 1
	args := QueryArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		Num: num,
	}
	reply := ck.call("ShardCtrler.Query", "Query", &args)
	return reply.(*QueryReply).Config
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.seq += 1
	args := JoinArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		Servers: servers,
	}
	ck.call("ShardCtrler.Join", "Join", &args)
}

func (ck *Clerk) Leave(gids []int) {
	ck.seq += 1
	args := LeaveArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		GIDs: gids,
	}
	ck.call("ShardCtrler.Leave", "Leave", &args)
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.seq += 1
	args := MoveArgs{
		BaseArgs: BaseArgs{
			ClientId: ck.clientId,
			Seq:      ck.seq,
		},
		Shard: shard,
		GID:   gid,
	}
	ck.call("ShardCtrler.Move", "Move", &args)
}

func (ck *Clerk) call(method, typeName string, args Args) Reply {
	ck.mu.Lock()
	leaderId := ck.leaderId
	ck.mu.Unlock()
	for {
		reply := NewReply(typeName)
		done := make(chan bool)

		DPrintf("call %s[%d] start, args = %s\n", method, leaderId, args)
		go func() {
			done <- ck.servers[leaderId].Call(method, args, reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call %s[%d] timeout, retry, args = %s\n", method, leaderId, args)
			continue
		}

		if !ok || reply.getErr() == ErrWrongLeader {
			leaderId = (leaderId + 1) % len(ck.servers)
			DPrintf("call %s[%d] !ok || ErrWrongLeader, retry\n", method, leaderId)
			continue
		}

		if reply.getErr() == ErrWrongTerm {
			DPrintf("call %s[%d] ErrWrongTerm, retry\n", method, leaderId)
			continue
		}

		DPrintf("call %s[%d] success, reply = %s\n", method, leaderId, reply)
		ck.mu.Lock()
		ck.leaderId = leaderId
		ck.mu.Unlock()
		return reply
	}
}
