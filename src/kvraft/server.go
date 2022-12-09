package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Ope     string
	Key     string
	Value   string
	UniId   int64
	LastUid int64
}

type void struct{}
type CidtoUniId struct {
	CId    int64
	UniqId int64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	persister *raft.Persister

	// Your definitions here.
	table map[string]string
	noti  map[int64](chan bool)
	debug string
	// unit   map[int64]int64
	uniSet map[int64]void

	rfIndex int // save the last index hand by rf
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("%vserver: %v got Get for key: %v\n", kv.debug, kv.me, args.Key)
	// delete(kv.uniSet, args.LastUid)
	// kv.mu.Lock()
	// kv.mu.Unlock()
	op := Op{
		Ope:     "Get",
		Key:     args.Key,
		Value:   "",
		UniId:   args.UniId,
		LastUid: args.LastUid,
	}
	_, _, isleader := kv.rf.Start(op)
	DPrintf("%vsend op to rt", kv.debug)
	if isleader == false {
		reply.Err = ErrWrongLeader
		// DPrintf("%v WrongLeader\n", kv.debug)
		return
	}
	// if _, ok := kv.uniSet[args.UniId]; ok {
	// 	if reply.Value, ok = kv.table[args.Key]; ok {
	// 		reply.Err = OK
	// 	} else {
	// 		reply.Err = ErrNoKey
	// 	}
	// 	return
	// }
	// map struct need protecttion
	// go wont allow any concurrent modify op to map struct
	kv.mu.Lock()
	kv.noti[args.UniId] = make(chan bool)
	ch := kv.noti[args.UniId]
	kv.mu.Unlock()
	// DPrintf("%vserver: %vgot for key: %v issused to raft index :%v\n", kv.debug, kv.me, args.Key, index)
	select {
	case <-time.After(500 * time.Millisecond):
		fakech := make(chan void)
		// client may request a single op several times,
		// if request is send to a stale leader
		// and this server win an election just right after it connect to the majority
		// all this same op will get commit by raft and send to applych
		// the notification channel should avoid sisuation like
		// no channel reciver before the channel get close
		// once any case get lock
		// can make sure no noti will send to this channel
		// due to channel exit check
		go func(ch chan bool, fakech chan void) {
			for {
				select {
				case <-ch:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
				case <-fakech:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
					close(fakech)
					return
				}
			}
		}(ch, fakech)
		kv.mu.Lock()
		// DPrintf("%v get got lock", kv.debug)
		fakech <- void{}
		reply.Err = ErrWrongLeader
	case <-ch:
		fakech := make(chan void)
		go func(ch chan bool, fakech chan void) {
			for {
				select {
				case <-ch:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
				case <-fakech:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
					close(fakech)
					return
				}
			}
		}(ch, fakech)
		var ok bool
		// DPrintf("%vserver: %vserver for key: %v :%v\n", kv.debug, kv.me, args.Key, index)
		kv.mu.Lock()
		fakech <- void{}
		// DPrintf("%v get got lock", kv.debug)
		if reply.Value, ok = kv.table[args.Key]; ok {
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
		// if _, ok := kv.unit[args.CId]; ok {
		// 	delete(kv.uniSet, kv.unit[args.CId])
		// }
		// kv.unit[args.CId] = args.UniId
	}
	close(kv.noti[args.UniId])
	delete(kv.noti, args.UniId)
	kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	DPrintf("%vserver: %v got Put for key: %v\n", kv.debug, kv.me, args.Key)
	// kv.mu.Lock()
	// // DPrintf("%vlocking test", kv.debug)
	// // delete(kv.uniSet, args.LastUid)
	// kv.mu.Unlock()
	op := Op{
		Ope:     args.Op,
		Key:     args.Key,
		Value:   args.Value,
		UniId:   args.UniId,
		LastUid: args.LastUid,
	}
	_, _, isleader := kv.rf.Start(op)
	DPrintf("%vsend op to rt", kv.debug)
	if isleader == false {
		reply.Err = ErrWrongLeader
		// DPrintf("%v WrongLeader\n", kv.debug)
		return
	}
	// if _, ok := kv.uniSet[args.UniId]; ok {
	// 	reply.Err = OK
	// 	return
	// }
	kv.mu.Lock()
	kv.noti[args.UniId] = make(chan bool)
	ch := kv.noti[args.UniId]
	kv.mu.Unlock()
	// DPrintf("%vserver: %vPut for key: %v issused to raft index :%v\n", kv.debug, kv.me, args.Key, index)
	select {
	case <-time.After(500 * time.Millisecond):
		fakech := make(chan void)
		go func(ch chan bool, fakech chan void) {
			// DPrintf("%vtimeout:index%v", kv.debug, index)
			for {
				select {
				case <-ch:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
				case <-fakech:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
					close(fakech)
					return
				}
			}
		}(ch, fakech)
		kv.mu.Lock()
		// DPrintf("%v Put got lock", kv.debug)
		fakech <- void{}
		reply.Err = ErrWrongLeader
	case <-ch:
		fakech := make(chan void)
		go func(ch chan bool, fakech chan void) {
			for {
				select {
				case <-ch:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
				case <-fakech:
					// DPrintf("%vuuid never use%v\n", kv.debug, op.UniId)
					close(fakech)
					return
				}
			}
		}(ch, fakech)
		// DPrintf("%vserver: %vserver for key: %v :%v\n", kv.debug, kv.me, args.Key, index)
		kv.mu.Lock()
		fakech <- void{}
		// DPrintf("%v Put got lock", kv.debug)
		// if _, ok := kv.unit[args.CId]; ok {
		// 	delete(kv.uniSet, kv.unit[args.CId])
		// }
		// kv.unit[args.CId] = args.UniId
		reply.Err = OK
	}
	close(kv.noti[args.UniId])
	delete(kv.noti, args.UniId)
	kv.mu.Unlock()
}

func (kv *KVServer) applyChReciver() {
	var snapshotindex = 0
	for kv.killed() == false {
		msg := <-kv.applyCh
		DPrintf("%v got msg index %v from applCh\n", kv.debug, msg.CommandIndex)
		if msg.CommandValid {
			op := (msg.Command).(Op)
			kv.mu.Lock()
			// DPrintf("%v applyCh got lock\n", kv.debug)
			if _, ok := kv.uniSet[op.UniId]; ok == false {
				if op.Ope == "Put" {
					kv.table[op.Key] = op.Value
					// DPrintf("%v set key :%v ", kv.debug, op.Key)
				} else if op.Ope == "Append" {
					if _, ok := kv.table[op.Key]; ok {
						kv.table[op.Key] = kv.table[op.Key] + op.Value
					} else {
						kv.table[op.Key] = op.Value
					}
					// DPrintf("%v set key :%v", kv.debug, op.Key)
				}
				kv.uniSet[op.UniId] = void{}
				delete(kv.uniSet, op.LastUid)
			}
			kv.rfIndex = msg.CommandIndex
			// only when commited index update
			// will snapshot help rf to reduce its log
			if kv.maxraftstate > 0 && kv.persister.RaftStateSize() > kv.maxraftstate && kv.rfIndex > snapshotindex+10 {
				// DPrintf("%v sendSnapshot to rf with index %v", kv.debug, kv.rfIndex)
				snapshotindex = msg.CommandIndex
				kv.sendSnapshot()
			}
			if _, ok := kv.noti[op.UniId]; ok {
				// DPrintf("%v apply ok,uuid%v\n", kv.debug, op.UniId)
				kv.noti[op.UniId] <- true
				// one posible thing
				// the later msg send from rf just before notification got the lock
				// and applych got lock again and issue another notification
				// the later notification got the lock first
			}
			// DPrintf("%v applyCh unlock \n", kv.debug)
			kv.mu.Unlock()
		} else if msg.SnapshotValid {
			// DPrintf("%v recived snapshot from rf install", kv.debug)
			kv.mu.Lock()
			// DPrintf("%v recived snapshot from rf install", kv.debug)
			kv.readSnapshot(msg.Snapshot)
			kv.mu.Unlock()
		}
	}
}

// send snapshot to raft
func (kv *KVServer) sendSnapshot() {
	// kv.mu.Lock()
	// defer kv.mu.Unlock()

	DPrintf("%v sendSnapshot to rf with index %v", kv.debug, kv.rfIndex)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.table)
	e.Encode(kv.rfIndex)
	e.Encode(kv.uniSet)

	snapshot := w.Bytes()

	kv.rf.Snapshot(kv.rfIndex, snapshot)
}

// load snapshot after recovery
// or hand from rf due to stale
func (kv *KVServer) readSnapshot(data []byte) {
	// DPrintf("%v readsnapshot", kv.debug)
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var table map[string]string
	var index int
	var uniset map[int64]void

	if d.Decode(&table) != nil || d.Decode(&index) != nil || d.Decode(&uniset) != nil {
		// DPrintf("%v readsnapshot fail", kv.debug)
	} else {
		kv.table = table
		kv.rfIndex = index
		kv.uniSet = uniset
	}
	DPrintf("%vreadsnapshot at index :%v", kv.debug, index)
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.persister = persister

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.table = make(map[string]string)
	kv.noti = make(map[int64](chan bool))

	// kv.unit = make(map[int64]int64)

	kv.uniSet = make(map[int64]void)

	kv.rfIndex = 0

	for i := 0; i < kv.me; i++ {
		kv.debug += "                               "
	}

	DPrintf("%v wake up", kv.debug)
	kv.readSnapshot(kv.persister.ReadSnapshot())

	go kv.applyChReciver()

	// You may need initialization code here.

	return kv
}
