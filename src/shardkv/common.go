package shardkv

import "log"

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	SKeep              = "Keep"
	SKeepAndSendout    = "KeepAndSendout"
	SRequest           = "Request"
	SRequesting        = "Requesting"
	SNoKeep            = "NoKeep"
	SkeepAndRequest    = "SkeepAndRequest"
	SkeepAndRequesting = "SkeepAndRequesting"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Err string

type void struct{}

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	UId     int64
	LastUId int64
	Shard   int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	UId     int64
	LastUId int64
	Shard   int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestShardArgs struct {
	ShardNum  int
	ConfigNum int
}

type RequestShardReply struct {
	Err      Err
	ShardNum int
	Shard    map[string]string
	Uniset   map[int64]void
}

type ConfirmArgs struct {
	ShardNum  int
	ConfigNum int
}

type ConfirmReply struct {
	Err Err
}
