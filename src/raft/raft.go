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
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"

const (
	ElectionTimeoutMin = 300
	ElectionTimeoutMax = 600
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type Log struct {
	Command interface{}
	Term    int
}

type State int

const (
	Follower State = iota
	Candidate
	Leader
)

var States = [...]string{
	"Follower",
	"Candidate",
	"Leader",
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu              sync.Mutex          // Lock to protect shared access to this peer's state
	peers           []*labrpc.ClientEnd // RPC end points of all peers
	persister       *Persister          // Object to hold this peer's persisted state
	me              int                 // this peer's index into peers[]
	dead            int32               // set by Kill()
	leaderHeartbeat bool
	state           State
	votedFor        *int          // Index of peer this server has votedFor (null if none)
	currentTerm     int           // Latest Term server has seen
	commitIndex     int           // Index of highest log entry known to be committed
	applyCh         chan ApplyMsg // Channel to which raft instance will send committed msgs
	lastApplied     int           // Index of last applied log entry
	log             []*Log        // log entries
	nextIndex       []int         // For each peer, the log index to be sent out
	matchIndex      []int         // For each peer, index of highest log entry known to be replicated on server
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (term int, isLeader bool) {

	defer rf.Unlock()
	rf.Lock()
	DPrintf("[%d] Term: %d, State: %d, Heartbeat: %v", rf.me, rf.currentTerm, rf.state, rf.leaderHeartbeat)

	term = rf.currentTerm
	isLeader = rf.state == Leader
	return
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
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	defer rf.Unlock()
	rf.Lock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term {
		reply.VoteGranted = false
		return
	}
	if args.LastLogIndex >= rf.commitIndex && (args.LastLogTerm > rf.log[rf.commitIndex].Term || (args.LastLogTerm == rf.log[rf.commitIndex].Term && 1+args.LastLogIndex >= len(rf.log))) {
		if rf.votedFor == nil {
			DPrintf("[%d] last log index of [%d] is %d", rf.me, args.CandidateId, args.LastLogIndex)
			DPrintf("[%d] my commit index is %d", rf.me, rf.commitIndex)
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
		} else if args.Term > rf.currentTerm {
			reply.VoteGranted = true
			rf.votedFor = &args.CandidateId
		}
	} else {
		DPrintf("[%d] last log index of [%d] is %d", rf.me, args.CandidateId, args.LastLogIndex)
		DPrintf("[%d] my commit index is %d", rf.me, rf.commitIndex)
		DPrintf("[%d] last log term of [%d] is %d", rf.me, args.CandidateId, args.LastLogTerm)
		DPrintf("[%d] my commit index's term is %d", rf.me, rf.log[rf.commitIndex].Term)
		DPrintf("[%d] length of my log is %d", rf.me, len(rf.log))
	}
}

//
// AppendEntries RPC arguments structure.
//
type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []*Log
	LeaderCommit int
}

//
// AppendEntries RPC reply structure.
//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
// AppendEntries RPC handler
//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	defer rf.Unlock()
	rf.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		DPrintf("[%d] received heartbeat from psuedo leader [%d]", rf.me, args.LeaderId)
		reply.Success = false
	} else if len(rf.log) <= args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		DPrintf("[%d] received append entries with mismatching log from [%d]", rf.me, args.LeaderId)
		DPrintf("[%d] log - %+v", rf.me, rf.log)
		DPrintf("[%d] args - %+v", rf.me, *args)
		reply.Success = false
	} else {
		if rf.state != Follower {
			DPrintf("[%d] becoming follower on getting valid heartbeat from [%d]", rf.me, args.LeaderId)
		}
		rf.leaderHeartbeat = true
		rf.currentTerm = args.Term
		rf.state = Follower
		startIndex := args.PrevLogIndex + 1
		for i := startIndex; i-startIndex < len(args.Entries); i++ {
			if len(rf.log) <= i {
				rf.log = append(rf.log, args.Entries[i-startIndex:]...)
				break
			} else if rf.log[i].Term != args.Entries[i-startIndex].Term {
				rf.log = rf.log[:i]
				i--
			}
		}
		reply.Success = true
		DPrintf("[%d] prev log index is %d", rf.me, args.PrevLogIndex)
		DPrintf("[%d] prev log term is %d", rf.me, args.PrevLogTerm)
		DPrintf("[%d] commit Index of leader is %d", rf.me, args.LeaderCommit)
		rf.commitEntries(args.LeaderCommit)
	}
}

func (rf *Raft) sendHeartbeat(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}

func (rf *Raft) sendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) (ok bool) {
	ok = rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return
}

func (rf *Raft) Lock() {
	// DPrintf("[%d] locking", rf.me)
	rf.mu.Lock()
}

func (rf *Raft) Unlock() {
	// DPrintf("[%d] unlocking", rf.me)
	rf.mu.Unlock()
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, done chan bool) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	done <- ok
}

func startElectionTicker(rf *Raft) {
	for {
		time.Sleep(getElectionTimeout())
		rf.Lock()
		if rf.killed() {
			rf.Unlock()
			return
		}
		if rf.state != Leader && !rf.leaderHeartbeat {
			term := rf.currentTerm + 1
			rf.currentTerm = term
			rf.state = Candidate
			rf.votedFor = &rf.me
			rf.Unlock()
			rf.conductElection(term)
		} else {
			rf.leaderHeartbeat = false
			rf.Unlock()
		}
	}
}

func (rf *Raft) conductElection(term int) {
	cond := sync.NewCond(&rf.mu)
	votes := 1
	replies := 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer cond.Broadcast()
			rf.Lock()
			args := RequestVoteArgs{
				Term:         term,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.Unlock()
			reply := RequestVoteReply{}
			rpcStart := time.Now()
			done := make(chan bool)
			rpcTimeout := time.NewTimer(ElectionTimeoutMax * time.Millisecond)
			defer rpcTimeout.Stop()
			defer func() {
				rf.Lock()
				replies++
				rf.Unlock()
			}()
			go rf.sendRequestVote(i, &args, &reply, done)
			select {
			case ok := <-done:
				if !ok {
					DPrintf("[%d] RequestVote RPC for term %d to peer [%d] failed - %v", rf.me, term, i, time.Since(rpcStart))
					return
				}
				break
			case <-rpcTimeout.C:
				DPrintf("[%d] RequestVote RPC for term %d to peer [%d] timed out - %v", rf.me, term, i, time.Since(rpcStart))
				return
			}
			defer rf.Unlock()
			rf.Lock()
			if reply.VoteGranted {
				DPrintf("[%d] received vote for term %d from peer [%d]", rf.me, term, i)
				votes++
			} else {
				DPrintf("[%d] did NOT receive vote for term %d from peer [%d] with term %d", rf.me, term, i, reply.Term)
			}
		}(i)
	}
	rf.Lock()
	for votes <= len(rf.peers)/2 && (replies-votes) < len(rf.peers)/2 && replies < len(rf.peers) {
		cond.Wait()
	}
	if votes > len(rf.peers)/2 {
		DPrintf("[%d] won the election for term %d by getting %d votes out of %d", rf.me, term, votes, len(rf.peers))
		rf.state = Leader
		rf.Unlock()
		initializeLeader(rf)
		sendPeriodicHeartbeats(rf)
	} else {
		DPrintf("[%d] lost the election for term %d", rf.me, term)
		rf.Unlock()
	}
}

func sendPeriodicHeartbeats(rf *Raft) {
	for {
		rf.Lock()
		if rf.state != Leader {
			rf.Unlock()
			return
		}
		term := rf.currentTerm
		rf.Unlock()
		rf.sendHeartbeats(term)
		time.Sleep(100 * time.Millisecond)
	}
}

func (rf *Raft) sendHeartbeats(term int) {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i int) {
			rf.Lock()
			args := AppendEntriesArgs{
				Term:         term,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log[rf.nextIndex[i]-1].Term,
				LeaderCommit: rf.commitIndex,
			}
			rf.Unlock()
			reply := AppendEntriesReply{}
			ok := rf.sendHeartbeat(i, &args, &reply)
			if !ok {
				return
			}
			if !reply.Success {
				defer rf.Unlock()
				rf.Lock()
				if reply.Term > rf.currentTerm {
					DPrintf("[%d] saw term %d on server [%d] which was greater than current term %d", rf.me, reply.Term, i, rf.currentTerm)
					rf.currentTerm = reply.Term
					rf.state = Follower
					rf.leaderHeartbeat = true
					return
				}
			}
		}(i)
	}
}

func getElectionTimeout() time.Duration {
	return time.Duration(ElectionTimeoutMin+rand.Intn(ElectionTimeoutMax-ElectionTimeoutMin)) * time.Millisecond
}

func initializeLeader(rf *Raft) {
	defer rf.Unlock()
	rf.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	for i := range rf.peers {
		rf.nextIndex[i] = len(rf.log)
	}
	rf.matchIndex = make([]int, len(rf.peers))
}

func (rf *Raft) beginConsensus(lastLogIndex int) {
	// send appendEntries to each peer concurrently
	// on reaching success from majority -> commit & apply the command
	// retry if the RPC fails
	// switch to follower if unsuccessful
	cond := sync.NewCond(&rf.mu)
	replies := 1
	for i := range rf.peers {
		// send appendEntries to each peer concurrently
		if i == rf.me {
			continue
		}
		go func(i int) {
			defer cond.Broadcast()
			for {
				rf.Lock()
				if rf.state != Leader {
					rf.Unlock()
					return
				}
				index := rf.nextIndex[i]
				args := AppendEntriesArgs{
					LeaderId:     rf.me,
					Term:         rf.currentTerm,
					LeaderCommit: rf.commitIndex,
					Entries:      rf.log[index:],
					PrevLogIndex: index - 1,
					PrevLogTerm:  rf.log[index-1].Term,
				}
				rf.Unlock()
				reply := AppendEntriesReply{}
				ok := rf.sendEntries(i, &args, &reply)
				if !ok {
					time.Sleep(10 * time.Millisecond)
					continue
				} else if reply.Success {
					rf.Lock()
					DPrintf("[%d] Append Entries on server [%d] successful, send command %+v", rf.me, i, rf.log[lastLogIndex].Command)
					replies++
					rf.nextIndex[i] = lastLogIndex + 1
					rf.Unlock()
					return
				} else {
					rf.Lock()
					if rf.currentTerm < reply.Term {
						DPrintf("[%d] saw term %d on server [%d] which was greater than current term %d", rf.me, reply.Term, i, rf.currentTerm)
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.Unlock()
						return
					} else {
						DPrintf("[%d] Append Entries on server [%d] unsuccessful, retrying with lower index", rf.me, i)
						rf.nextIndex[i]--
						rf.Unlock()
						continue
					}
				}
			}
		}(i)
	}
	defer rf.Unlock()
	rf.Lock()
	for rf.state == Leader && replies <= len(rf.peers)/2 {
		cond.Wait()
	}
	if rf.state == Leader && rf.log[lastLogIndex].Term == rf.currentTerm {
		rf.commitEntries(lastLogIndex)
	}
}

// commitEntries doesn't lock - take care of locking before calling
func (rf *Raft) commitEntries(lastLogIndex int) {
	prevCommitIndex := rf.commitIndex
	if len(rf.log) <= lastLogIndex {
		rf.commitIndex = len(rf.log) - 1
	} else {
		rf.commitIndex = lastLogIndex
	}
	for i := prevCommitIndex + 1; i <= rf.commitIndex; i++ {
		DPrintf("[%d] committing entries", rf.me)
		msg := ApplyMsg{Command: rf.log[i].Command, CommandIndex: i, CommandValid: true}
		rf.applyCh <- msg
	}
	rf.lastApplied = lastLogIndex
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
	// Your code here (2B).
	rf.Lock()
	isLeader := rf.state == Leader
	index := len(rf.log)
	term := rf.currentTerm
	if isLeader {
		DPrintf("[%d] processing command from client - %+v at index %d", rf.me, command, index)
		logEntry := Log{Command: command, Term: term}
		rf.log = append(rf.log, &logEntry)
		rf.Unlock()
		go rf.beginConsensus(index)
	} else {
		rf.Unlock()
	}
	return index, term, isLeader
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	rf.state = Follower
	rf.applyCh = applyCh
	log := Log{Term: 0}
	rf.log = append(rf.log, &log)

	// Your initialization code here (2A, 2B, 2C).
	DPrintf("[%d] starting server as follower", me)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start the election after timeout
	go startElectionTicker(rf)

	return rf
}
