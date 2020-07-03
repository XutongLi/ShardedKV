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
	"../labrpc"
	"sync"
	"sync/atomic"
	"time"
)


// import "bytes"
// import "../labgob"



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
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
}

const (
	NOTVOTED				int = -1
	LEADER 					string = "leader"
	FOLLOWER 				string = "follower"
	CANDIDATE 				string = "candidate"
	ELECTION_TIMEOUT_MIN 	int = 400
	ELECTION_TIMEOUT_MAX 	int = 600
	HEARTBEAT_INTERVAL 		int = 150
	ELECTION_INTERVAL		int = 100
)

type LogEntry struct {
	Index 			int				// index of log entry, start from 1 (0 = fake entry's index)
	Term 			int				// term of log entry
	Command 		interface{}		// command of log entry
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        			sync.Mutex          // Lock to protect shared access to this peer's state
	peers     			[]*labrpc.ClientEnd // RPC end points of all peers
	persister 			*Persister          // Object to hold this peer's persisted state
	me        			int                 // this peer's index into peers[]
	dead      			int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state 				string				// state of server
	currentTerm			int					// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor			int					// candidateId that revceived vote in current term (or null if none)
	committedIndex		int					// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied			int					// index of highest log entry appelied to state machine (initialized to 0, increases monotonically)

	logs				[]LogEntry			// array of log entries

	// on leader
	nextIndex			[]int				// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex			[]int				// for each server, index of highest log entry known to replicated on server (initialized to 0, increases monotonically)

	electionTimeout		time.Duration		// election timeout
	lastHeartBeatTime	time.Time			// time of receiving last heartbeat

	applyCh 			chan ApplyMsg		// message applied to state machine
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
	isleader = (rf.state == LEADER)
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

// reset random election timeout and the time of last heart beat
func (rf *Raft) resetElectionTimeout() {
	rf.lastHeartBeatTime = time.Now()
	rf.electionTimeout = time.Duration(RandTimeout(ELECTION_TIMEOUT_MIN, ELECTION_TIMEOUT_MAX)) * time.Millisecond
	//DPrintf("[%d-%s-%d] reset lastHeartBeat=%v, electionTimeout=%v", rf.me, rf.state, rf.currentTerm, rf.lastHeartBeatTime, rf.electionTimeout)
}

// convert to candidate and start a election
func (rf *Raft) convertToCandidate() {
	rf.state = CANDIDATE
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.resetElectionTimeout()
	DPrintf("[%d-%s-%d] start a election\n", rf.me, rf.state, rf.currentTerm)
}

// convert to leader and start to send heartbeat
func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	lastLogIndex := rf.getLastLogEntry().Index
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i ++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = 0
	}
	go rf.heartbeatTimer()
	DPrintf("[%d-%s-%d] become leader\n", rf.me, rf.state, rf.currentTerm)
}

// convert to follower and set specified new term
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = newTerm
	rf.votedFor = NOTVOTED
	rf.resetElectionTimeout()
	DPrintf("[%d-%s-%d] become follower\n", rf.me, rf.state, rf.currentTerm)
}

// get the last log entry
func (rf *Raft) getLastLogEntry() LogEntry {
	entry := rf.logs[len(rf.logs) - 1]
	return entry
}

// whether the term of rpc request is larger than currentTerm
func (rf *Raft) isTermOutOfDate(rpcTerm int) bool {
	if rpcTerm > rf.currentTerm {
		return true
	} else {
		return false
	}
}

// election timeout timer
// check once per ELECTION_INTERVAL ms
// if election timeout elapses, start a goroutine for election
// the timer never stop unless the server is down
func (rf *Raft) electionTimeoutTimer() {
	for {
		rf.mu.Lock()
		timeline := rf.lastHeartBeatTime.Add(rf.electionTimeout)
		if rf.killed() {
			defer rf.mu.Unlock()
			DPrintf("[%d-%s-%d] is killed", rf.me, rf.state, rf.currentTerm)
			return
		}
		if rf.state != LEADER && timeline.Before(time.Now()) {
			go rf.startElection()
			//DPrintf("[%d-%s-%d] fail and here", rf.me, rf.state, rf.currentTerm)
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ELECTION_INTERVAL) * time.Millisecond)
	}
}

// start election after election timeout
func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.convertToCandidate()		//convert to candidate and reset election timeout
	curTerm := rf.currentTerm
	me := rf.me
	state := rf.state
	lastLogEntry := rf.getLastLogEntry()
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	getVoteNum := 1			// num of votes obtained
	finishedVoteNum := 1	// num of reply of vote request
	cond := sync.NewCond(&rf.mu)
	// rf.resetElectionTimeout()
	rf.mu.Unlock()

	for svr := 0; svr < len(rf.peers); svr ++ {
		if svr == rf.me {
			continue
		}
		go func(me int, state string, svr int, curTerm int, requestVoteArgs RequestVoteArgs) {
			DPrintf("[%d-%s-%d] sending vote request to %d\n", me, state, curTerm, svr)
			requestVoteReply := RequestVoteReply{}
			var voteGranted bool
			ok := rf.sendRequestVote(svr, &requestVoteArgs, &requestVoteReply)		// cannot send PRC request in a lock
			if !ok {
				voteGranted = false
				DPrintf("[%d-%s-%d] fails connect to %d when request vote\n", me, state, curTerm, svr)
			} else {
				voteGranted = requestVoteReply.VoteGranted
			}

			rf.mu.Lock()
			defer rf.mu.Unlock()
			if ok {
				if rf.isTermOutOfDate(requestVoteReply.Term) {
					rf.convertToFollower(requestVoteReply.Term)
					return
				}
				if rf.currentTerm != requestVoteArgs.Term {
					voteGranted = false
				}
			}
			if voteGranted {
				getVoteNum += 1
			}
			finishedVoteNum += 1
			cond.Broadcast()
		}(me, state, svr, curTerm, requestVoteArgs)
	}

	rf.mu.Lock()
	peersNum := len(rf.peers)
	for getVoteNum <= peersNum / 2 && finishedVoteNum != peersNum /*&& rf.state == CANDIDATE*/ {	// conditions of waiting
		cond.Wait()
	}
	if getVoteNum > peersNum / 2 && rf.state == CANDIDATE && rf.currentTerm == curTerm {	// get vote from majority of peers
		rf.convertToLeader()
	} else {
		DPrintf("[%d-%s-%d] fail in election", rf.me, rf.state, rf.currentTerm)
		rf.resetElectionTimeout()		// fail in election, reset election timeout and retry
	}
	rf.mu.Unlock()
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int 	// candidate's term
	CandidateId	int 	// candidate requesting vote
	LastLogIndex 	int 	// index of candidate's last log entry
	LastLogTerm 	int 	// term of candidate's last log entry
}


// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term 			int 	// currentTerm, for candidate to update itself
	VoteGranted		bool 	// true means candidate received vote
}


// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%d-%s-%d] receive vote request from %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	if rf.isTermOutOfDate(args.Term) {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {		// refuse to vote because RPC request'term is less than currentTerm
		DPrintf("[%d-%s-%d] receive outdated vote from %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	lastLogEntry := rf.getLastLogEntry()
	lastLogIndex := lastLogEntry.Index
	lastLogTerm := lastLogEntry.Term
	upToDate := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)
	if upToDate && (rf.votedFor == NOTVOTED || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimeout()
		DPrintf("[%d-%s-%d] vote for %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	} else {
		DPrintf("[%d-%s-%d] refuse to vote for %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
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

// heartbeat timer
// send an AppendEntries per HEARTBEAT_INTERVAL
func (rf *Raft) heartbeatTimer() {
	rf.mu.Lock()
	DPrintf("[%d-%s-%d] start heartbeat", rf.me, rf.state, rf.currentTerm)
	rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.killed() {
			defer rf.mu.Unlock()
			DPrintf("[%d-%s-%d] is killed", rf.me, rf.state, rf.currentTerm)
			return
		}
		if rf.state != LEADER {		// when this peer is no longer a leader, stop heartbeat timer
			defer rf.mu.Unlock()
			return
		}
		rf.sendAppendEntriesToFollower()
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
	}
}

// send AppendEntries to followers
// if there aren't new entries, send empty request
func (rf *Raft) sendAppendEntriesToFollower() {
	DPrintf("[%d-%s-%d] start to send AppendEntries to peers", rf.me, rf.state, rf.currentTerm)
	//nextIndex := rf.nextIndex
	//logs := rf.logs
	me := rf.me
	state := rf.state
	currentTerm := rf.currentTerm
	appendEntriesArgs := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.committedIndex,
	}
	rf.mu.Unlock()

	for svr := 0; svr < len(rf.peers); svr += 1 {
		if svr == me {
			continue
		}
		//if len(logs) > 0 {
		//	appendEntriesArgs.PrevLogIndex = nextIndex[svr] - 1
		//	appendEntriesArgs.PrevLogTerm = logs[appendEntriesArgs.PrevLogIndex - 1].Term
		//}
		//if len(logs) >= nextIndex[svr] {
		//	appendEntriesArgs.Entries = logs[nextIndex[svr] - 1: ]
		//}
		go func(svr int, appendEntriesArgs AppendEntriesArgs) {
			appendEntrriesReply := AppendEntriesReply{}
			if !rf.sendAppendEntries(svr, &appendEntriesArgs, &appendEntrriesReply) {
				DPrintf("[%d-%s-%d] appendEntries sent to %d fail", me, state, currentTerm, svr)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.isTermOutOfDate(appendEntrriesReply.Term) {
				rf.convertToFollower(appendEntrriesReply.Term)
				return
			}
			if rf.state != LEADER {
				return
			}
		}(svr, appendEntriesArgs)
	}

}

// AppendEntries RPC arguments structure
type AppendEntriesArgs struct {
	Term 			int 		// leader's term
	LeaderId		int			// so follower can redict client
	PrevLogIndex	int			// index of log entry immediately preceding new ones
	PrevLogTerm 	int			// term of prevLogIndex entry
	Entries 		[]LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int			// leader's commitIndex
}

type AppendEntriesReply struct {
	Term 			int			// currentTerm, for leader to update itself
	Success			bool		// true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	DPrintf("[%d-%s-%d] receive AppendEntries from %d", rf.me, rf.state, rf.currentTerm, args.LeaderId)
	if rf.isTermOutOfDate(args.Term) {
		rf.convertToFollower(args.Term)
	}
	if args.Term < rf.currentTerm {
		DPrintf("[%d-%s-%d] refuse the AppendEntries from %d because of outdated term", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		defer rf.mu.Unlock()
		return
	}
	rf.resetElectionTimeout()
	reply.Term = rf.currentTerm
	reply.Success = true

	rf.mu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).
	// fake log entry, because the first entry's index should be 1
	rf.logs = make([]LogEntry, 1)
	rf.logs[0].Term = 0
	rf.logs[0].Index = 0

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = NOTVOTED
	rf.committedIndex = 0
	rf.lastApplied = 0
	rf.applyCh = applyCh
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeoutTimer()
	DPrintf("[%d] is initialized\n", rf.me)

	return rf
}
