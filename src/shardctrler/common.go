package shardctrler

import "fmt"

//
// Shard controller: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10
const InvalidGroup = 0

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

func (config *Config) clone() Config {
	clone := Config{
		Num:    config.Num + 1,
		Shards: [NShards]int{},
		Groups: map[int][]string{},
	}
	for i := range config.Shards {
		clone.Shards[i] = config.Shards[i]
	}
	for k, v := range config.Groups {
		clone.Groups[k] = v
	}
	return clone
}

func (config *Config) getMinMax() (minCnt, minGid, maxCnt, maxGid int) {
	cnt := make(map[int]int)
	for gid := range config.Groups {
		cnt[gid] = 0
	}
	for i, gid := range config.Shards {
		if _, ok := config.Groups[gid]; ok {
			cnt[gid] += 1
		} else {
			config.Shards[i] = InvalidGroup
			cnt[InvalidGroup] += 1
		}
	}

	minCnt = NShards
	minGid = 0
	maxCnt = 0
	maxGid = 0
	for k, v := range cnt {
		if k == InvalidGroup {
			continue
		}

		if v < minCnt {
			minCnt = v
			minGid = k
		} else if v == minCnt {
			minGid = Max(minGid, k)
		}

		if v > maxCnt {
			maxCnt = v
			maxGid = k
		} else if v == maxCnt {
			maxGid = Max(maxGid, k)
		}
	}

	if cnt[InvalidGroup] > 0 {
		maxCnt = cnt[InvalidGroup]
		maxGid = InvalidGroup
	}
	return
}

func (config *Config) findFirstIndexByGid(target int) int {
	for i, gid := range config.Shards {
		if gid == target {
			return i
		}
	}
	return NShards
}

func (config *Config) balance() {
	for {
		minCnt, minGid, maxCnt, maxGid := config.getMinMax()
		//DPrintf("minCnt = %v, minGid = %v, maxCnt = %v, maxGid = %v\n", minCnt, minGid, maxCnt, maxGid)
		if minGid == InvalidGroup {
			for i := range config.Shards {
				config.Shards[i] = InvalidGroup
			}
			break
		}
		if maxGid != InvalidGroup && minCnt+1 >= maxCnt {
			break
		}

		maxGidIdx := config.findFirstIndexByGid(maxGid)
		config.Shards[maxGidIdx] = minGid
	}
}

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
	ErrWrongTerm   = "ErrWrongTerm"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings
	ClientId int64
	Seq      int64
}

func (args *JoinArgs) String() string {
	return fmt.Sprintf("{Servers = %v, ClientId = %d, Seq = %d}", args.Servers, args.ClientId, args.Seq)
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

func (reply *JoinReply) String() string {
	return fmt.Sprintf("{WrongLeader = %v, Err = %v}", reply.WrongLeader, reply.Err)
}

type LeaveArgs struct {
	GIDs     []int
	ClientId int64
	Seq      int64
}

func (args *LeaveArgs) String() string {
	return fmt.Sprintf("{GIDs = %v, ClientId = %d, Seq = %d}", args.GIDs, args.ClientId, args.Seq)
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

func (reply *LeaveReply) String() string {
	return fmt.Sprintf("{WrongLeader = %v, Err = %v}", reply.WrongLeader, reply.Err)
}

type MoveArgs struct {
	Shard    int
	GID      int
	ClientId int64
	Seq      int64
}

func (args *MoveArgs) String() string {
	return fmt.Sprintf("{Shard = %v, GID = %v, ClientId = %d, Seq = %d}", args.Shard, args.GID, args.ClientId, args.Seq)
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

func (reply *MoveReply) String() string {
	return fmt.Sprintf("{WrongLeader = %v, Err = %v}", reply.WrongLeader, reply.Err)
}

type QueryArgs struct {
	Num int // desired config number
}

func (args *QueryArgs) String() string {
	return fmt.Sprintf("{Num = %v}", args.Num)
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

func (reply *QueryReply) String() string {
	return fmt.Sprintf("{WrongLeader = %v, Err = %v, Config.Num = %v}", reply.WrongLeader, reply.Err, reply.Config.Num)
}

func Max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
