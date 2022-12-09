package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	dead    int32
	applyCh chan raft.ApplyMsg

	// Your data here.
	noti   map[int64]chan bool
	uniSet map[int64]void

	configs []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Ope     string
	Args    interface{}
	UId     int64
	LastUId int64
}

func (sc *ShardCtrler) task(op Op, uid int64) bool {

	_, _, isleader := sc.rf.Start(op)
	if isleader == false {
		return false
	}
	sc.mu.Lock()
	sc.noti[uid] = make(chan bool)
	ch := sc.noti[uid]
	sc.mu.Unlock()

	done := false

	select {
	case <-time.After(500 * time.Millisecond):
		fakech := make(chan void)
		go func() {
			for {
				select {
				case <-ch:
				case <-fakech:
					close(fakech)
					return
				}
			}
		}()
		sc.mu.Lock()
		fakech <- void{}
	case <-ch:
		fakech := make(chan void)
		go func() {
			for {
				select {
				case <-ch:
				case <-fakech:
					close(fakech)
					return
				}
			}
		}()
		sc.mu.Lock()
		fakech <- void{}
		done = true
	}
	close(sc.noti[uid])
	delete(sc.noti, uid)
	sc.mu.Unlock()
	return done
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Ope:     "join",
		Args:    JoinArgs{Servers: args.Servers},
		UId:     args.UId,
		LastUId: args.LastUId,
	}

	if ok := sc.task(op, args.UId); ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Ope:     "leave",
		Args:    LeaveArgs{GIDs: args.GIDs},
		UId:     args.UId,
		LastUId: args.LastUId,
	}
	if ok := sc.task(op, args.UId); ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Ope:     "move",
		Args:    MoveArgs{Shard: args.Shard, GID: args.GID},
		UId:     args.UId,
		LastUId: args.LastUId,
	}
	if ok := sc.task(op, args.UId); ok {
		reply.WrongLeader = false
		reply.Err = OK
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Ope:     "query",
		Args:    QueryArgs{Num: args.Num},
		UId:     args.UId,
		LastUId: args.LastUId,
	}
	if ok := sc.task(op, args.UId); ok {
		reply.WrongLeader = false
		reply.Err = OK
		if args.Num == -1 || args.Num >= len(sc.configs) {
			reply.Config = sc.configs[len(sc.configs)-1]
		} else {
			reply.Config = sc.configs[args.Num]
		}
	} else {
		reply.WrongLeader = true
	}
}

func (sc *ShardCtrler) applyChReciver() {
	for sc.killed() == false {
		msg := <-sc.applyCh
		op := (msg.Command).(Op)
		sc.mu.Lock()
		if _, ok := sc.uniSet[op.UId]; ok == false {
			if op.Ope == "join" {
				servers := (op.Args).(JoinArgs).Servers
				newconfig := Config{}
				newconfig.Num = sc.configs[len(sc.configs)-1].Num + 1
				newconfig.Groups = make(map[int][]string)
				var keys []int
				for k := range servers {
					newconfig.Groups[k] = servers[k]
					keys = append(keys, k)
				}
				for k := range sc.configs[len(sc.configs)-1].Groups {
					newconfig.Groups[k] = sc.configs[len(sc.configs)-1].Groups[k]
					keys = append(keys, k)
				}
				sort.Ints(keys)
				if len(keys) > 0 {
					for i := range newconfig.Shards {
						newconfig.Shards[i] = keys[i%len(keys)]
					}
				}
				sc.configs = append(sc.configs, newconfig)
			} else if op.Ope == "leave" {
				gids := (op.Args).(LeaveArgs).GIDs
				newconfig := Config{}
				newconfig.Num = sc.configs[len(sc.configs)-1].Num + 1
				newconfig.Groups = make(map[int][]string)
				var keys []int
				for k := range sc.configs[len(sc.configs)-1].Groups {
					newconfig.Groups[k] = sc.configs[len(sc.configs)-1].Groups[k]
				}
				for _, k := range gids {
					delete(newconfig.Groups, k)
				}
				for k := range newconfig.Groups {
					keys = append(keys, k)
				}
				sort.Ints(keys)
				if len(keys) > 0 {
					for i := range newconfig.Shards {
						newconfig.Shards[i] = keys[i%len(keys)]
					}
				}
				sc.configs = append(sc.configs, newconfig)
			} else if op.Ope == "move" {
				shard := (op.Args).(MoveArgs).Shard
				gid := (op.Args).(MoveArgs).GID
				newconfig := Config{}
				newconfig.Num = sc.configs[len(sc.configs)-1].Num + 1
				newconfig.Groups = make(map[int][]string)
				newconfig.Shards = sc.configs[len(sc.configs)-1].Shards
				for k := range sc.configs[len(sc.configs)-1].Groups {
					newconfig.Groups[k] = sc.configs[len(sc.configs)-1].Groups[k]
				}
				newconfig.Shards[shard] = gid
				sc.configs = append(sc.configs, newconfig)
			}
			sc.uniSet[op.UId] = void{}
			delete(sc.uniSet, op.LastUId)
		}
		if _, ok := sc.noti[op.UId]; ok {
			sc.noti[op.UId] <- true
		}
		sc.mu.Unlock()
	}
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.

	sc.uniSet = make(map[int64]void)
	sc.noti = make(map[int64]chan bool)
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	go sc.applyChReciver()

	return sc
}
