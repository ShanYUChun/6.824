package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"math/rand"

	"6.824/labgob"
	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	currentTerm int        // the latest term rt in
	votedFor    int        // initial to be -1, update when votefor leader
	log         []LogEntry // all logs the rt hold
	isLeader    bool

	commitIndex int // index in log that have been commited
	lastApplied int // index in log that have been applied

	nextIndex  []int // only leader, next index leader will send to each peers
	matchIndex []int // the latest each peers match in leader's log

	lastHeartBeat int64 // last time heartbeat

	applyCh     chan ApplyMsg
	applychCond sync.Cond
	mu_peers    []sync.Mutex // mu for appendentry
	mu_speers   []sync.Mutex // mu for snapshot

	snapShotTerm  int
	snapShotIndex int

	debug string
}

type LogEntry struct {
	LogTerm  int
	LogIndex int
	Command  interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.isLeader
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	// fmt.Printf("%v server: %v persist log\n", rf.debug, rf.me)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)

	data := w.Bytes()

	// fmt.Printf("%v server: %v persist log ans snapshot\n", rf.debug, rf.me)

	rf.persister.SaveStateAndSnapshot(data, snapshot)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	// // fmt.Printf("%v server : %v read its persist\n", rf.debug, rf.me)
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		// // fmt.Printf("%v Error: reading no persist", rf.debug)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.isLeader = false
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	if args.LastIncludedIndex <= rf.snapShotIndex || args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	// fmt.Printf("%v install snapshot %v\n", rf.debug, args.LastIncludedIndex)
	rf.applyCh <- ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if rf.log[len(rf.log)-1].LogIndex > args.LastIncludedIndex && rf.log[args.LastIncludedIndex-rf.snapShotIndex].LogTerm == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIndex-rf.snapShotIndex:]
		rf.snapShotIndex = args.LastIncludedIndex
		rf.snapShotTerm = args.LastIncludedTerm
	} else {
		rf.log = rf.log[0:0]
		rf.log = append(rf.log, LogEntry{args.LastIncludedTerm, args.LastIncludedIndex, ""})
		rf.snapShotIndex = args.LastIncludedIndex
		rf.snapShotTerm = args.LastIncludedTerm
	}

	rf.commitIndex = args.LastIncludedIndex

	rf.persistWithSnapshot(args.Data)
}

type InstallSnapshotArgs struct {
	Term              int // leader's currentTerm
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte // snapshort
}

// there is only one passible situation for Installsnapshot fail
// leader's Term < rf's Term
// if leader's Term > follower's Term , even the leader is stale the snapshot is harmless
// the snapshot only contain log which is commited
// if leader's Term >= rf's Term situation like lastincludedindex < rf.s' firstlogindex wont happen
// leader keep tracks the rf's match index, is a rf is more stale than the leader
// the matchindex will be the first log in leader's term that will > rf's last index
type InstallSnapshotReply struct {
	Term int // rf's currentTerm for leader update
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	// Snapshot should return immediately
	// snapshot will block the applych while sending snapshot in the test
	// if snapshot should return without waiting for the mu
	// applychsender will lock the mu if applych is blocked, and if mu locked Snapshot will never return, applych will blocked
	// is a deadlock
	// this will happen when commitindex is greater than snapshor index
	// applychshender wants to -> more log to applych but since snapshot never return, it never sucesuss
	go rf.snapshot(index, snapshot)
}

func (rf *Raft) snapshot(index int, snapshot []byte) {
	// fmt.Printf("%v term, got snapshot with index%v\n", rf.debug, index)
	rf.mu.Lock()
	// fmt.Printf("%v term, got snapshot with index%v\n", rf.debug, index)
	defer rf.mu.Unlock()
	// if index < last snapshot index just return
	if index < rf.snapShotIndex {
		return
	}
	// rf.log[index] is regard as index 0 ,it wont be commit agian
	rf.snapShotTerm = rf.log[index-rf.snapShotIndex].LogTerm
	rf.log = rf.log[index-rf.snapShotIndex:]
	rf.snapShotIndex = index
	rf.persistWithSnapshot(snapshot)

	// check whether rf is the leader and other server have catch up
	if rf.isLeader {
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.snapShotIndex,
			LastIncludedTerm:  rf.snapShotTerm,
			Data:              snapshot,
		}
		// fmt.Printf("%v leader sending snapshot index%v\n", rf.debug, index)
		for server := range rf.peers {
			if server == rf.me || rf.matchIndex[server] >= rf.snapShotIndex {
				continue
			}
			go rf.installSnapshot(server, args)
		}
	}
}

func (rf *Raft) installSnapshot(server int, args InstallSnapshotArgs) {
	// rf.mu_speers[server].Lock()
	// defer rf.mu_speers[server].Unlock()
	rf.mu.Lock()
	// fmt.Printf("%v sending snapshot hold lock\n", rf.debug)
	if rf.isLeader == false || rf.matchIndex[server] >= args.LastIncludedIndex || args.LastIncludedIndex < rf.snapShotIndex {
		rf.mu.Unlock()
		return
	}
	// fmt.Printf("%v sending snapshot to server%v index %v\n", rf.debug, server, args.LastIncludedIndex)
	rf.mu.Unlock()
	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(server, &args, &reply); ok {
		// fmt.Printf("%v sending snapshot to server%v index %v sucesss\n", rf.debug, server, args.LastIncludedIndex)
		rf.mu.Lock()
		defer rf.mu.Unlock()
		// 防止在 call installsnapshot 的时候 matchindex 发生变化，超过了LastIncludedIndex
		if reply.Term <= rf.currentTerm && rf.matchIndex[server] < args.LastIncludedIndex {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		} else if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.isLeader = false
			rf.votedFor = -1
			rf.persist()
		}
	} else {
		// fmt.Printf("%v sending snapshot to server%v index %v unsucesss\n", rf.debug, server, args.LastIncludedIndex)
		time.Sleep(100 * time.Millisecond)
		go rf.installSnapshot(server, args)
	}

}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate's id
	LastLogIndex int // Index for candidates's last entry
	LastLogTerm  int // term for candidates's last entry
	// 这里 为什么只需要 lastlogindex 和 logterm --> lastterm 就可以保证 这个 rf 一定参与了上一次 term
	// logindex log只会 append 在 对应的index上，如果 candidate miss了任何 log，之后的log 都append 不上
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // follower's currentTerm, if candidate's term less than currentTerm, update it
	VoteGranted bool // true for vote, false when candidate's term < currentTerm || already voted in this term
}

type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // leader's id, for follower to redirect client
	PrevLogIndex int        // last log index
	PrevLogTerm  int        // term for last log index
	Entries      []LogEntry // maybe more than one
	LeaderCommit int        // leader's commitindex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm
	Success bool // true for follower entries log match with prelog and prelogterm

	ConflitTerm int // reject from leader, reply with the conflict log term
	TermIndex   int // index for begin conflict term
	Lenlog      int // the whole log long
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
	} else if rf.currentTerm == args.Term && rf.votedFor != -1 {
		reply.VoteGranted = false
		// only when the last logTerm is equal then check the index,
	} else if (rf.log[len(rf.log)-1].LogTerm == args.LastLogTerm && rf.log[len(rf.log)-1].LogIndex > args.LastLogIndex) || rf.log[len(rf.log)-1].LogTerm > args.LastLogTerm {
		reply.VoteGranted = false
		// incase candidate's Term greater than server so server can update it's self
		if rf.currentTerm < args.Term {
			// leader can only lead for one term
			// if one server term is greater than leader leader should turn to follower
			rf.currentTerm = args.Term
			rf.isLeader = false
			// if update the term, then update votefor
			// when candidate find a server's term greater than its own, trun to follower
			rf.votedFor = -1
		}
		rf.persist()
	} else {
		reply.VoteGranted = true
		// if electiontimeout and not recieve communication from leader of granted vote for candidates
		// this prevent two follower election time out at the same time, if one recive voterequest it have to sleep another election timeout
		rf.lastHeartBeat = time.Now().UnixMilli()
		rf.votedFor = args.CandidateId
		if rf.currentTerm < args.Term {
			rf.currentTerm = args.Term
			rf.isLeader = false
		}
		rf.persist()
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	DPrintf("%v**func Start holding lock**\n", rf.debug)
	defer rf.mu.Unlock()

	isLeader = rf.isLeader
	index = rf.log[len(rf.log)-1].LogIndex + 1
	term = rf.currentTerm

	if isLeader {
		// leader first append cmd to its own
		// for every
		// fmt.Printf("%vterm %v recived command index :%v\n", rf.debug, rf.currentTerm, index)
		rf.log = append(rf.log, LogEntry{term, index, command})
		rf.persist()
		go rf.addEntries()
	}
	return index, term, isLeader
}

// addEntries used to add all Log from client or leader
func (rf *Raft) addEntries() {
	for i := range rf.peers {
		if i == rf.me || rf.killed() {
			continue
		}
		go rf.appendEntries(i)
	}
}

func (rf *Raft) appendEntries(server int) {
	rf.mu.Lock()
	if rf.matchIndex[server] < rf.snapShotIndex && rf.killed() == false {
		// wait for installsnapshot succuess
		// check wether rf is killed
		// implicit loop will run out the compulity of vm
		go func(server int) {
			time.Sleep(100 * time.Millisecond)
			rf.mu.Lock()
			if rf.matchIndex[server] < rf.snapShotIndex {
				go rf.trySendSnapshot(server)
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
			rf.appendEntries(server)
		}(server)
		rf.mu.Unlock()
		return
	}
	rf.mu.Unlock()
	rf.mu_peers[server].Lock()
	defer rf.mu_peers[server].Unlock()
	rf.mu.Lock()
	DPrintf("%v**func appendEntries holding lock**\n", rf.debug)
	if rf.isLeader == false || rf.nextIndex[server] > rf.log[len(rf.log)-1].LogIndex {
		// check if installsnapshot have installed all the log in leader'slog
		// to makesure leader wont pass empty logs to server in appendentry func
		// when erase print language this can be erase, this appendentry just like a heartbeat
		rf.mu.Unlock()
		return
	}
	// fmt.Printf("%v server%v\n", rf.debug, server)
	if rf.matchIndex[server] < rf.snapShotIndex {
		// wait for installsnapshot succuess
		go rf.appendEntries(server)
		rf.mu.Unlock()
		return
	}
	// fmt.Printf("%v last match %v begin from %v for serve %v\n", rf.debug, rf.matchIndex[server], rf.nextIndex[server], server)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: rf.matchIndex[server],
		PrevLogTerm:  rf.log[rf.matchIndex[server]-rf.snapShotIndex].LogTerm,
		Entries:      rf.log[rf.nextIndex[server]-rf.snapShotIndex:],
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()
	// fmt.Printf("%v server %v:append entry %v -> %v\n", rf.debug, server, args.Entries[0].LogIndex, args.Entries[len(args.Entries)-1].LogIndex)
	reply := AppendEntriesReply{}
	ok := make(chan bool)
	go func() {
		time.Sleep(500 * time.Millisecond)
		ok <- false
	}()
	go func() {
		ok <- rf.sendAppendEntries(server, &args, &reply)
	}()
	if <-ok {
		rf.mu.Lock()
		DPrintf("%v**func appendEntries ok holding lock**\n", rf.debug)
		defer rf.mu.Unlock()
		if rf.isLeader == false {
			// if rf is not leader
			// there is not need to preocess
			// and early return can prevent access log which already snapshoted
			// stale leader can update is term according to other server
			// later reply from another server with then same meaasge will cause leader to locate the index
			// which is not nessary and dangerous for visiting
			return
		}
		if reply.Term > rf.currentTerm {
			// sisuation like stale leader has already changed his term due to last reply
			rf.isLeader = false
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		} else if reply.Success {
			// fmt.Printf("%v%v sppend entry %v -> %vsuccess\n", rf.debug, server, args.Entries[0].LogIndex, args.Entries[len(args.Entries)-1].LogIndex)
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1
			// // fmt.Printf("%vupdate server %v lastmatch to%v\n", rf.debug, server, rf.matchIndex[server])
			// update commit index
			// leader itself must the most uptpdate, count itself
			count := 1
			for i := range rf.matchIndex {
				// check do not conut itself twice
				// there may cause a bug
				// if count itself twice the the minority might commit,
				// this cisuation happend when the leader become leader in new term and with a lot of uncommit log
				// and the it partition to a minority
				// all his uncommit log will get commited
				// but in the marjority
				// another story is on
				if rf.matchIndex[server] <= rf.matchIndex[i] && i != rf.me {
					count++
				}
			}
			// // fmt.Printf("----------------------%v num of server reach agreement on index %v------------------\n", count, rf.matchIndex[server])
			//  原文 Only entry in the current Term commited by counting replica( 避免 Old entry replicate in marjority but  still but overwriten)
			//  entry from previous term commited indirectly when a current entry commited( one entry is commited commitedindex increase to N any index before N will commited)
			if count > len(rf.peers)/2 && rf.matchIndex[server] > rf.commitIndex && rf.log[rf.matchIndex[server]-rf.snapShotIndex].LogTerm == rf.currentTerm {
				// // fmt.Printf("%vupdate commitindex\n", rf.debug)
				// if count itself twice there will cause panic
				// when leader in minority(it not realize it in the minority)
				// finally connect to majority
				// its log may change according to the appendentry
				// at that time this commitindex may greator than it's last index in its log
				rf.commitIndex = rf.matchIndex[server]
				rf.applychCond.Broadcast()
			}
		} else {
			// case 1: server have conflict in the index and the term leader didnot have
			if reply.TermIndex > rf.snapShotIndex && reply.ConflitTerm != rf.log[reply.TermIndex-rf.snapShotIndex].LogTerm {
				// // fmt.Printf("%v leader dont have that term", rf.debug)
				rf.matchIndex[server] = reply.TermIndex - 1
				rf.nextIndex[server] = reply.TermIndex
			} else if reply.TermIndex >= rf.snapShotIndex && reply.ConflitTerm != 0 {
				// there should go to the end of the term
				// // fmt.Printf("%v leader have that term", rf.debug)
				i := reply.TermIndex
				for ; rf.log[i-rf.snapShotIndex].LogTerm == reply.ConflitTerm; i++ {
				}
				rf.matchIndex[server] = i - 1
				rf.nextIndex[server] = i
			} else {
				// } else if reply.Lenlog >= rf.snapShotIndex {
				// // fmt.Printf("%v log short", rf.debug)
				if reply.ConflitTerm != 0 {
					rf.matchIndex[server] = reply.TermIndex - 1
					rf.nextIndex[server] = reply.TermIndex
				} else {
					rf.matchIndex[server] = reply.Lenlog
					rf.nextIndex[server] = reply.Lenlog + 1
				}
				// } else {
				// if server's log is too short that has been discard by leader, just wait for snapshot to handle this
				// to prevent leader fail and matchindex will satrt from the last index in leader's log
				// snapshot will loss
				if rf.matchIndex[server] < rf.snapShotIndex {
					go rf.trySendSnapshot(server)
					// fmt.Printf("%v %v Unlock\n", rf.debug, server)
					// go func() {
					// 	<-ok
					// 	close(ok)
					// }()
					// return
				}
			}
			go func(server int) {
				time.Sleep(500 * time.Millisecond)
				rf.appendEntries(server)
			}(server)
		}
	} else {
		// if fail to connect to server retry
		// // fmt.Printf("%v fail to connect %v,retry", rf.debug, server)
		go func(server int) {
			time.Sleep(500 * time.Millisecond)
			rf.appendEntries(server)
		}(server)
	}
	go func() {
		<-ok
		close(ok)
	}()
}

// this method will check wehter leader is sending snapshot to server
// if not leader will read its presist snapshot and send to it
// this to prevent leader from recover and miss the snapshot should send to server
// or leader changed
func (rf *Raft) trySendSnapshot(server int) {
	// fmt.Printf("%v try send snapshot to server%v\n", rf.debug, server)
	// rf.mu_speers[server].Lock()
	// defer rf.mu_speers[server].Unlock()
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.snapShotIndex,
		LastIncludedTerm:  rf.snapShotTerm,
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.isLeader == false || rf.matchIndex[server] >= args.LastIncludedIndex || args.LastIncludedIndex < rf.snapShotIndex {
		return
	}
	args.Data = rf.persister.snapshot
	// args.Data = snapshot
	go rf.installSnapshot(server, args)
}

func (rf *Raft) applyChSender() {
	start := 0
	for rf.killed() == false {
		rf.applychCond.L.Lock()
		rf.applychCond.Wait()
		rf.mu.Lock()
		DPrintf("%v**func applyChSender holding lock**\n", rf.debug)
		if start < rf.snapShotIndex {
			start = rf.snapShotIndex
		}
		for start < rf.commitIndex {
			start++
			// fmt.Printf("%vcommand :%v\n", rf.debug, rf.log[start-rf.snapShotIndex].Command)
			rf.applyCh <- ApplyMsg{
				CommandValid: true,
				Command:      rf.log[start-rf.snapShotIndex].Command,
				CommandIndex: start,
			}
			DPrintf("%v**update commitindex -> %v**\n", rf.debug, start)
		}
		// fmt.Printf("%v func applyChSender Unlock\n", rf.debug)
		rf.mu.Unlock()
		rf.applychCond.L.Unlock()
	}
}

func (rf *Raft) lead(term int) {
	rf.mu.Lock()
	// fmt.Printf("%v func lead holding lock\n", rf.debug)
	// leader is bounded with term only can be a leader in the term it start election
	if term != rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	rf.isLeader = true
	// 初始化 matchindex
	for i := range rf.peers {
		rf.matchIndex[i] = rf.log[len(rf.log)-1].LogIndex
		rf.nextIndex[i] = rf.matchIndex[i] + 1
	}
	// send none op entry tot make sure every entry before will commit
	// fmt.Printf("%v term :%v ,begin to lead\n", rf.debug, rf.currentTerm)
	rf.mu.Unlock()
	for rf.isLeader && rf.killed() == false {
		rf.mu.Lock()
		rf.lastHeartBeat = time.Now().UnixMilli()
		rf.mu.Unlock()
		for i := range rf.peers {
			if i == rf.me || rf.killed() || rf.isLeader == false {
				continue
			}
			go rf.heartBeat(i)
		}
		time.Sleep(150 * time.Millisecond)
	}
}

func (rf *Raft) heartBeat(server int) {
	rf.mu.Lock()
	args := AppendEntriesArgs{
		Term:     rf.currentTerm,
		LeaderId: rf.me,
		// 这里 作为heartbeat 严格使用最后一个log的index，server接收到之后要么是最后一条
		PrevLogIndex: rf.log[len(rf.log)-1].LogIndex,
		PrevLogTerm:  rf.log[len(rf.log)-1].LogTerm,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	// // // // fmt.Printf("%v,  %v\n", server, args)
	rf.mu.Unlock()
	reply := AppendEntriesReply{}
	if ok := rf.sendAppendEntries(server, &args, &reply); ok {
		// 如果发现任何比当前term大的term 都应该停止
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.isLeader = false
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.persist()
		}
		rf.mu.Unlock()
	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	// // // // fmt.Printf("server %v got killed\n", rf.me)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	// // // // fmt.Printf("____________________________________________________")
	// i := 0
	for rf.killed() == false {

		// // // // fmt.Printf("server : %v, newsleep %v\n", rf.me, i)
		// i++

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		// broadcasttime << electiontimeout << MTBF(average time between failure in a single machine)
		// hearbeats should less than 10 per second, broadcasttime is far short than that
		// hearbeart every 150 ms --> electiontimeout 400 ms -- 600ms
		rf.mu.Lock()
		electiontimeout := rand.Int31n(1500) + 600 + int32(rf.lastHeartBeat-time.Now().UnixMilli())
		rf.mu.Unlock()
		// // // // fmt.Printf("server : %v ,time is %v , sleep for %v\n", rf.me, time.Now().UnixMilli(), electiontimeout)
		// // // // fmt.Printf("server : %v sleep over\n", rf.me)
		time.Sleep(time.Duration(electiontimeout * int32(time.Millisecond)))
		// // // // fmt.Printf("server : %v sleep over\n", rf.me)
		rf.mu.Lock()
		if time.Now().UnixMilli()-rf.lastHeartBeat > int64(electiontimeout) {
			rf.lastHeartBeat = time.Now().UnixMilli()
			// // // // fmt.Printf("server : %v ,electiontimeout,go election\n", rf.me)
			rf.mu.Unlock()
			go rf.election()
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) election() {
	// // fmt.Printf("%vserver%v start voting\n", rf.debug, rf.me)
	rf.mu.Lock()
	rf.currentTerm += 1
	// first vote for it self
	rf.votedFor = rf.me

	rf.persist()
	// send request
	// track vote recived
	voteRecived := 1
	votetotal := 1
	electionTerm := rf.currentTerm
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].LogIndex,
		LastLogTerm:  rf.log[len(rf.log)-1].LogTerm,
	}

	rf.mu.Unlock()

	// call to disconnected server will cost time , so if use a for loop and wait every server to responce will run out of time
	// use go routine to requestvote

	// // // // fmt.Printf("server : %v ,election, term is %v, sending voterequest\n", rf.me, rf.currentTerm)
	for i := range rf.peers {
		if rf.killed() == false && i != rf.me {
			// // // fmt.Printf("server : %v ,election, term is %v,sending request to %v\n", rf.me, rf.currentTerm, i)
			go func(i int) {
				reply := RequestVoteReply{}
				if ok := rf.sendRequestVote(i, &args, &reply); ok {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					votetotal++
					if reply.VoteGranted {
						// // // fmt.Printf("server : %v ,election, term is %v, votedby %v\n", rf.me, rf.currentTerm, i)
						voteRecived++
					} else if reply.Term > rf.currentTerm {
						// // // fmt.Printf("server : %v ,election, term is %v,%v not vote,Term is %v,eletion false\n", rf.me, rf.currentTerm, i, reply.Term)
						// if find any
						rf.currentTerm = reply.Term
						rf.votedFor = -1
						rf.persist()
						return
					}
				}
			}(i)
		}
	}
	for voteRecived <= len(rf.peers)/2 && votetotal-voteRecived < len(rf.peers)/2 {
		time.Sleep(10 * time.Millisecond)
		// // // // fmt.Printf("server : %v , election wating for vote", rf.me)
	}
	// // // fmt.Printf("server : %v ,election, voteRecived %v, suceess %v\n", rf.me, voteRecived, voteRecived > len(rf.peers)/2)
	if voteRecived > len(rf.peers)/2 && rf.votedFor == rf.me {
		go rf.lead(electionTerm)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	// two condition
	// Old leader which stale send AppendEntries Request or this rf is disconnected and constantly increase its term ans vote

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// // fmt.Printf("%v server %v is stale ,term %v < current%v\n", rf.debug, args.LeaderId, args.Term, rf.currentTerm)
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}
	rf.lastHeartBeat = time.Now().UnixMilli()
	// if old leader recieved heartbeat or request from leader turn to follower
	rf.isLeader = false

	// reset last heartbeat

	// AppendEntries must from leader so check current term match with leader's
	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// prelogindex and prelogterm should match
	// leader will try prelog from last to the begin
	// this will prevent rf to commit its stale log that have conflits with leaders
	// if rf havent catch up to leader's request no commit happend
	// >= , =的情况可能发生在 送heartbeat的时候
	if args.PrevLogIndex < rf.snapShotIndex {
		// this happen when meaasge dely
		// leader first send appendentry the send installsnapshot
		// installsnapshot come first, follower change its log the appendentry comes
		//
		reply.Term = rf.currentTerm
		reply.Success = false
		reply.ConflitTerm = 0
		reply.TermIndex = 0
		reply.Lenlog = rf.snapShotIndex
		return
	}
	if args.PrevLogIndex > rf.log[len(rf.log)-1].LogIndex || rf.log[args.PrevLogIndex-rf.snapShotIndex].LogTerm != args.PrevLogTerm {
		// when server dont recieve log in this term or stale for couple terms and may append terms that not commit from old leader
		reply.Term = rf.currentTerm
		reply.Success = false
		// if leader's log have conflit with its own
		// rf is respone with its log information to help quicker locate nextIndex

		// assuming log shorter than index
		reply.ConflitTerm = 0
		reply.TermIndex = 0

		// case 1: log for PrevLogIndex dont match with term
		// case 2: log short
		if args.PrevLogIndex <= rf.log[len(rf.log)-1].LogIndex {
			reply.ConflitTerm = rf.log[args.PrevLogIndex-rf.snapShotIndex].LogTerm
			i := args.PrevLogIndex - rf.snapShotIndex
			for ; i >= 0 && rf.log[args.PrevLogIndex-rf.snapShotIndex].LogTerm == rf.log[i].LogTerm; i-- {
			}
			reply.TermIndex = i + 1
		}
		reply.Lenlog = rf.log[len(rf.log)-1].LogIndex
		return
	}

	// rf will drop all log after prelogindex and if message is not heartbeat then append entries in message
	if args.Entries != nil {
		rf.log = append(rf.log[:args.PrevLogIndex+1-rf.snapShotIndex], args.Entries...)
		// // fmt.Printf("%v:append entry %v -> %v\n", rf.debug, args.Entries[0].LogIndex, args.Entries[len(args.Entries)-1].LogIndex)
		rf.persist()
		// // // // fmt.Printf("server : %v , logs %v\n", rf.me, rf.log)
	}
	// update commitedindex

	if rf.commitIndex < args.LeaderCommit {
		if rf.log[len(rf.log)-1].LogIndex < args.LeaderCommit && rf.commitIndex < rf.log[len(rf.log)-1].LogIndex {
			rf.commitIndex = rf.log[len(rf.log)-1].LogIndex
			rf.applychCond.Broadcast()
		} else if rf.log[len(rf.log)-1].LogIndex >= args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			rf.applychCond.Broadcast()
		}
	}

	reply.Term = rf.currentTerm
	reply.Success = true

}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.lastHeartBeat = time.Now().UnixMilli()
	rf.log = make([]LogEntry, 0)
	rf.log = append(rf.log, LogEntry{0, 0, ""})
	rf.currentTerm = 0
	// rf.nextIndex = []int{-1,-1}
	rf.commitIndex = 0
	rf.lastApplied = -1
	rf.votedFor = -1
	rf.isLeader = false
	rf.applychCond = *sync.NewCond(&sync.Mutex{})
	rf.mu_peers = make([]sync.Mutex, len(peers))
	rf.mu_speers = make([]sync.Mutex, len(peers))

	rf.snapShotIndex = 0
	rf.snapShotTerm = 0

	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))

	rf.applyCh = applyCh

	for i := 0; i < rf.me; i++ {
		rf.debug += "                                "
	}

	// applyCh = make(chan ApplyMsg)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	rf.snapShotIndex = rf.log[0].LogIndex
	rf.snapShotTerm = rf.log[0].LogTerm

	// start ticker goroutine to start elections
	go rf.applyChSender()
	// // // fmt.Printf("server : %v started, time is %v\n", rf.me, rf.lastHeartBeat)
	go rf.ticker()

	return rf
}
