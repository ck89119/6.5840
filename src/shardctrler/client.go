package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
)
import "time"
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

func (ck *Clerk) call(method string, args Args, reply Reply) {
	for {
		done := make(chan bool)

		DPrintf("call %s[%d] start, args = %s\n", method, ck.leaderId, args)
		go func() {
			done <- ck.servers[ck.leaderId].Call(method, args, reply)
		}()

		var ok bool
		select {
		case ok = <-done:
		case <-time.After(time.Second):
			// timeout, retry
			DPrintf("call %s[%d] timeout, retry, args = %s\n", method, ck.leaderId, args)
			continue
		}

		if !ok || reply.getErr() == ErrWrongLeader {
			ck.leaderId = (ck.leaderId + 1) % len(ck.servers)
			DPrintf("call %s[%d] !ok || ErrWrongLeader, retry\n", method, ck.leaderId)
			continue
		}

		if reply.getErr() == ErrWrongTerm {
			DPrintf("call %s[%d] ErrWrongTerm, retry\n", method, ck.leaderId)
			continue
		}

		DPrintf("call %s[%d] success, reply = %s\n", method, ck.leaderId, reply)
		return
	}
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
	reply := QueryReply{}
	ck.call("ShardCtrler.Query", &args, &reply)
	return reply.Config
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
	reply := JoinReply{}
	ck.call("ShardCtrler.Join", &args, &reply)
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
	reply := LeaveReply{}
	ck.call("ShardCtrler.Leave", &args, &reply)
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
	reply := MoveReply{}
	ck.call("ShardCtrler.Move", &args, &reply)
}
