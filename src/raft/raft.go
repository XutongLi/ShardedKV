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
	"../labgob"
	"../labrpc"
	"bytes"
	"sync/atomic"
	"time"
	"sync"
)


// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = (rf.state == LEADER)
	return term, isleader
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
	rf.persist()
	//DPrintf("[%d-%s-%d] start a election\n", rf.me, rf.state, rf.currentTerm)
	//DPrintf("[%d-%s-%d] last heartbeat is %v, election time is %v", rf.me, rf.state, rf.currentTerm, rf.lastHeartBeatTime, rf.electionTimeout)
	//DPrintf("%v", time.Now())
	rf.resetElectionTimeout()
}

// convert to leader and start to send heartbeat
func (rf *Raft) convertToLeader() {
	rf.state = LEADER
	lastLogIndex := rf.LogIndex(len(rf.logs) - 1)
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for i := 0; i < len(rf.peers); i ++ {
		rf.nextIndex[i] = lastLogIndex + 1
		rf.matchIndex[i] = rf.lastSnapshotIndex
	}
	go rf.heartbeatTimer()
	//DPrintf("[%d-%s-%d] become leader\n", rf.me, rf.state, rf.currentTerm)
}

// convert to follower and set specified new term
func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = FOLLOWER
	rf.currentTerm = newTerm
	rf.votedFor = NOTVOTED
	rf.resetElectionTimeout()
	rf.persist()
	//DPrintf("[%d-%s-%d] become follower\n", rf.me, rf.state, rf.currentTerm)
}

// get log.index by array index of logs
func (rf *Raft) LogIndex(index int) int {
	return index + rf.lastSnapshotIndex
}

//get array index of logs by log.index
func (rf *Raft) ArrayIdx(index int) int {
	return index - rf.lastSnapshotIndex
}

//get the index of the last entry in a specified term
func (rf *Raft) getLastIndexOfTerm(conflictTerm int) int {
	for idx := rf.LogIndex(len(rf.logs) - 1); idx >= rf.lastSnapshotIndex; idx -= 1 {
		if rf.logs[rf.ArrayIdx(idx)].Term == conflictTerm {
			return idx
		}
		if rf.logs[rf.ArrayIdx(idx)].Term < conflictTerm {
			break
		}
	}
	return 0
}

//get the index of the first entry in a specified term
func (rf *Raft) getFirstIndexOfTerm(conflictTerm int) int {
	conflictIndex := rf.lastSnapshotIndex
	for ; conflictIndex <= rf.LogIndex(len(rf.logs) - 1); conflictIndex += 1 {
		if rf.logs[rf.ArrayIdx(conflictIndex)].Term == conflictTerm {
			break
		}
	}
	return conflictIndex
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
		if rf.killed() {
			defer rf.mu.Unlock()
			//DPrintf("[%d-%s-%d] is killed", rf.me, rf.state, rf.currentTerm)
			return
		}
		timeDuration := time.Since(rf.lastHeartBeatTime)
		if rf.state != LEADER && timeDuration >= rf.electionTimeout {
			//DPrintf("[%d-%s-%d] election timeout, %v", rf.me, rf.state, rf.currentTerm, timeDuration)
			rf.convertToCandidate()		//convert to candidate and reset election timeout
			go rf.startElection()
		}
		rf.mu.Unlock()
		time.Sleep(time.Duration(ELECTION_INTERVAL) * time.Millisecond)
	}
}

// start election after election timeout
func (rf *Raft) startElection() {
	rf.mu.Lock()
	curTerm := rf.currentTerm
	me := rf.me
	state := rf.state
	lastLogEntry := rf.logs[len(rf.logs) - 1]
	requestVoteArgs := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogEntry.Index,
		LastLogTerm:  lastLogEntry.Term,
	}
	getVoteNum := 1			// num of votes obtained
	finishedVoteNum := 1	// num of reply of vote request
	cond := sync.NewCond(&rf.mu)
	rf.mu.Unlock()

	for svr := 0; svr < len(rf.peers); svr ++ {
		if svr == rf.me {
			continue
		}
		go func(me int, state string, svr int, curTerm int, requestVoteArgs RequestVoteArgs) {
			//DPrintf("[%d-%s-%d] sending vote request to %d\n", me, state, curTerm, svr)
			requestVoteReply := RequestVoteReply{}
			var voteGranted bool
			ok := rf.sendRequestVote(svr, &requestVoteArgs, &requestVoteReply)		// cannot send PRC request in a lock
			if !ok {
				voteGranted = false
				//DPrintf("[%d-%s-%d] <Resp> fails connect to %d when request vote\n", me, state, curTerm, svr)
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
	for getVoteNum <= peersNum / 2 && finishedVoteNum != peersNum {	// conditions of waiting
		cond.Wait()
	}
	if getVoteNum > peersNum / 2 && rf.state == CANDIDATE && rf.currentTerm == curTerm {	// get vote from majority of peers
		rf.convertToLeader()
	} else {
		//DPrintf("[%d-%s-%d] fail in election", rf.me, rf.state, rf.currentTerm)
		rf.resetElectionTimeout()		// fail in election, reset election timeout and retry
	}
	rf.mu.Unlock()
}



// RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//DPrintf("[%d-%s-%d] receive vote request from %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	if rf.isTermOutOfDate(args.Term) {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {		// refuse to vote because RPC request'term is less than currentTerm
		//DPrintf("[%d-%s-%d] receive outdated vote from %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
		reply.VoteGranted = false
		return
	}

	// if votedFor is null or candidateId, and candidate's log is at least as up-to-date as receiver's log, grant vote
	lastLogEntry := rf.logs[len(rf.logs) - 1]
	lastLogIndex := lastLogEntry.Index
	lastLogTerm := lastLogEntry.Term
	upToDate := lastLogTerm < args.LastLogTerm || (lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex)	// election restriction
	if upToDate && (rf.votedFor == NOTVOTED || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.resetElectionTimeout()
		rf.persist()
		//DPrintf("[%d-%s-%d] vote for %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
	} else {
		//DPrintf("[%d-%s-%d] refuse to vote for %d", rf.me, rf.state, rf.currentTerm, args.CandidateId)
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
	//rf.mu.Lock()
	//DPrintf("[%d-%s-%d] start heartbeat", rf.me, rf.state, rf.currentTerm)
	//rf.mu.Unlock()
	for {
		rf.mu.Lock()
		if rf.killed() {
			defer rf.mu.Unlock()
			//DPrintf("[%d-%s-%d] is killed", rf.me, rf.state, rf.currentTerm)
			return
		}
		if rf.state != LEADER {		// when this peer is no longer a leader, stop heartbeat timer
			defer rf.mu.Unlock()
			return
		}
		rf.sendAppendEntriesToFollower()
		rf.mu.Unlock()
		time.Sleep(time.Duration(HEARTBEAT_INTERVAL) * time.Millisecond)
	}
}

// send AppendEntries to followers
// if there aren't new entries, send empty request
func (rf *Raft) sendAppendEntriesToFollower() {
	DPrintf("[%d-%s-%d] start to send AppendEntries to peers", rf.me, rf.state, rf.currentTerm)
	for svr := 0; svr < len(rf.peers); svr += 1 {
		if svr == rf.me {
			continue
		}
		go func(svr int) {
			rf.mu.Lock()
			if rf.state != LEADER {
				defer rf.mu.Unlock()
				return
			}
			if rf.nextIndex[svr] <= rf.lastSnapshotIndex {	 // Conditions for sending snapshots (send an InstallSnapshot RPC to a follower when the leader has discarded log entries that the follower needs.)
				rf.sendInstallSnapshotToSvr(svr)
				return
			}

			appendEntriesArgs := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				LeaderCommit: rf.commitIndex,
				PrevLogIndex: rf.nextIndex[svr] - 1,
			}
			if rf.nextIndex[svr] - 1 < rf.lastSnapshotIndex {
				appendEntriesArgs.PrevLogTerm = -1
			} else {
				appendEntriesArgs.PrevLogTerm = rf.logs[rf.ArrayIdx(rf.nextIndex[svr] - 1)].Term
			}
			if rf.LogIndex(len(rf.logs)- 1) >= rf.nextIndex[svr] {		// get log entry
				appendEntriesArgs.Entries = append(make([]LogEntry, 0), rf.logs[rf.ArrayIdx(rf.nextIndex[svr]): ]...)
			} else {
				appendEntriesArgs.Entries = make([]LogEntry, 0)
			}
			rf.mu.Unlock()

			appendEntriesReply := AppendEntriesReply{}

			if !rf.sendAppendEntries(svr, &appendEntriesArgs, &appendEntriesReply) {
				//DPrintf("[%d-%s-%d] <Resp> appendEntries sent to %d fail", me, state, currentTerm, svr)
				return
			}

			rf.mu.Lock()
			if rf.isTermOutOfDate(appendEntriesReply.Term) {
				rf.convertToFollower(appendEntriesReply.Term)
				defer rf.mu.Unlock()
				return
			}
			if rf.state != LEADER || rf.currentTerm != appendEntriesArgs.Term {
				// DPrintf("[%d-%s-%d] is no longer a leader", rf.me, rf.state, rf.currentTerm)
				defer rf.mu.Unlock()
				return
			}

			if appendEntriesReply.Success {
				rf.matchIndex[svr] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)
				rf.nextIndex[svr] = rf.matchIndex[svr] + 1
				if rf.commitLogEntry() {	// update commitIndex
					rf.applyCommitEntry()	// apply log entry
				}
				defer rf.mu.Unlock()
				return
			} else {
				newNextIndex := appendEntriesReply.ConflictIndex
				if appendEntriesReply.ConflictTerm != -1 {
					newNextIndex = rf.getLastIndexOfTerm(appendEntriesReply.ConflictTerm) + 1
				}
				rf.nextIndex[svr] = Min(newNextIndex, rf.LogIndex(len(rf.logs)))	// consider prevLogIndex is in the shanpshot of svr
				rf.mu.Unlock()
			}
		}(svr)
	}

}


func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC handler
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	//DPrintf("[%d-%s-%d] receive AppendEntries from %d", rf.me, rf.state, rf.currentTerm, args.LeaderId)

	if args.Term < rf.currentTerm {
		// DPrintf("[%d-%s-%d] refuse the AppendEntries from %d because of outdated term", rf.me, rf.state, rf.currentTerm, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
		defer rf.mu.Unlock()
		return
	}
	if rf.isTermOutOfDate(args.Term) || rf.state == CANDIDATE {
		rf.convertToFollower(args.Term)
	}
	rf.resetElectionTimeout()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = 0
	reply.ConflictTerm = -1
	prevLogTerm := -1
	if args.PrevLogIndex >= rf.lastSnapshotIndex && args.PrevLogIndex <= rf.LogIndex(len(rf.logs) - 1) {
		prevLogTerm = rf.logs[rf.ArrayIdx(args.PrevLogIndex)].Term
	}
	if prevLogTerm != args.PrevLogTerm {
		reply.ConflictIndex = rf.LogIndex(len(rf.logs))
		if prevLogTerm != -1 {
			reply.ConflictTerm = prevLogTerm
			reply.ConflictIndex = rf.getFirstIndexOfTerm(prevLogTerm)
		}
		defer rf.mu.Unlock()
		return
	}

	reply.Success = true
	thisIdx := args.PrevLogIndex
	for i := 0; i < len(args.Entries); i += 1 {
		thisIdx += 1
		if thisIdx < rf.LogIndex(len(rf.logs)) {
			if rf.logs[rf.ArrayIdx(thisIdx)].Term == args.Entries[i].Term {
				continue
			} else {
				rf.logs = rf.logs[: rf.ArrayIdx(thisIdx)]
			}
		}
		rf.logs = append(rf.logs, args.Entries[i: ]...)
		rf.persist()
		break
	}

	// check new commit
	if args.LeaderCommit > rf.commitIndex {
		lastNewIndex := rf.LogIndex(len(rf.logs) - 1)
		rf.commitIndex = Min(args.LeaderCommit, lastNewIndex)	// figure 2 AppendEntries RPC - Receiver implementation 5

		DPrintf("[%d-%s-%d] log entry is %v", rf.me, rf.state, rf.currentTerm, rf.logs)
		DPrintf("[%d-%s-%d] update commitIndex to %d", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		rf.applyCommitEntry()
	}
	rf.mu.Unlock()
}


// InstallSnapshot
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// send InstallSnapshot RPC request to follower svr
func (rf *Raft) sendInstallSnapshotToSvr(svr int) {
	DPrintf("[%v-%v-%v] send InstallSnapshot to %v", rf.me, rf.state, rf.currentTerm, svr)
	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	installSnapshotReply := InstallSnapshotReply{}
	if !rf.sendInstallSnapshot(svr, &installSnapshotArgs, &installSnapshotReply) {
		//DPrintf("[%d-%s-%d] <Resp> InstallReply sent to %d fail", me, state, currentTerm, svr)
		return
	}
	rf.mu.Lock()
	if rf.state != LEADER || rf.currentTerm != installSnapshotArgs.Term {
		defer rf.mu.Unlock()
		return
	}
	if rf.isTermOutOfDate(installSnapshotReply.Term) {
		rf.convertToFollower(installSnapshotReply.Term)
		defer rf.mu.Unlock()
		return
	}
	rf.matchIndex[svr] = rf.lastSnapshotIndex
	rf.nextIndex[svr] = rf.matchIndex[svr] + 1
	if rf.commitLogEntry() {
		rf.applyCommitEntry()
	}
	rf.mu.Unlock()
}

// InstallSnapshot RPC handler
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v-%v-%v] get InstallSnapshot from %v", rf.me, rf.state, rf.currentTerm, args.LeaderId)
	if rf.isTermOutOfDate(args.Term) {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.resetElectionTimeout()

	if args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}

	if args.LastIncludedIndex < rf.LogIndex(len(rf.logs) - 1) {
		rf.logs = append(make([]LogEntry, 0), rf.logs[rf.ArrayIdx(args.LastIncludedIndex): ]...)
	} else {
		rf.logs = make([]LogEntry, 1)
		rf.logs[0].Term = args.LastIncludedTerm
		rf.logs[0].Index = args.LastIncludedIndex
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)
	rf.commitIndex = Max(rf.commitIndex, rf.lastSnapshotIndex)
	rf.lastApplied = Max(rf.lastApplied, rf.lastSnapshotIndex)

	if rf.lastApplied > rf.lastSnapshotIndex { 	// for linearizability
		return
	}
	snapshotMsg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: rf.lastSnapshotIndex,
		Snapshot:     args.Data,
	}
	go func(snapshotMsg ApplyMsg) {
		rf.applyCh <- snapshotMsg
	}(snapshotMsg)
}


// commit entry whose replications are in majority
// return: whether there is a new commit
func (rf *Raft) commitLogEntry() bool {
	oldCommitIndex := rf.commitIndex
	for N := rf.LogIndex(len(rf.logs) - 1); N > rf.commitIndex; N -= 1 {
		repliCnt := 1
		for svr := 0; svr < len(rf.peers); svr += 1 {
			if svr == rf.me {
				continue
			}
			if rf.matchIndex[svr] >= N {
				repliCnt += 1
			}
		}
		if repliCnt > len(rf.peers) / 2 && rf.logs[rf.ArrayIdx(N)].Term == rf.currentTerm {
			rf.commitIndex = N
			break
		}
	}
	if rf.commitIndex > oldCommitIndex {
		DPrintf("[%d-%s-%d] log entry is %v", rf.me, rf.state, rf.currentTerm, rf.logs)
		DPrintf("[%d-%s-%d] update commitIndex to %d", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		return true
	} else {
		return false
	}

}


// apply committed log entry
func (rf *Raft) applyCommitEntry() {
	rf.lastApplied = Max(rf.lastApplied, rf.lastSnapshotIndex)
	rf.commitIndex = Max(rf.commitIndex, rf.lastSnapshotIndex)

	if rf.commitIndex <= rf.lastApplied {
		return
	}

	applyEntires := append([]LogEntry{},
		rf.logs[rf.ArrayIdx(rf.lastApplied + 1): rf.ArrayIdx(rf.commitIndex + 1)]...)

	go func(startIdx int, applyEntries []LogEntry) {
		for idx, entry := range applyEntires {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      entry.Command,
				CommandIndex: startIdx + idx,
				Snapshot:     nil,
			}
			rf.applyCh <- applyMsg
			rf.mu.Lock()
			DPrintf("[%v-%v-%v] apply log %v", rf.me, rf.state, rf.currentTerm, applyMsg.CommandIndex)
			if rf.lastApplied < applyMsg.CommandIndex {
				rf.lastApplied = applyMsg.CommandIndex
			}
			rf.mu.Unlock()
		}
	}(rf.lastApplied + 1, applyEntires)
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
//
func (rf *Raft) encodeState() []byte {
	writer := new(bytes.Buffer)
	encoder := labgob.NewEncoder(writer)
	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.logs)
	encoder.Encode(rf.lastSnapshotIndex)
	state := writer.Bytes()
	return state
}

func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.encodeState())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	reader := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(reader)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	var lastSnapshotIndex int
	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&logs) != nil ||
		decoder.Decode(&lastSnapshotIndex) != nil {
		DPrintf("[%d] decode error", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastSnapshotIndex = lastSnapshotIndex

		rf.commitIndex = rf.lastSnapshotIndex
		rf.lastApplied = rf.lastSnapshotIndex
	}
}

// get raft size, called by replicated state machine
func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// called by replicated state machine
func (rf *Raft) SaveStateAndSnapshot(requestSnapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if requestSnapshotIndex <= rf.lastSnapshotIndex {	// for linearizability
		return
	}
	arryIdx := rf.ArrayIdx(requestSnapshotIndex)
	if arryIdx >= len(rf.logs) {
		return
	}

	rf.logs = append(make([]LogEntry, 0), rf.logs[arryIdx: ]...)	// remain logs[0] as lastSnapshotIndex
	rf.lastSnapshotIndex = requestSnapshotIndex
	state := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

}

// get snapshot, called by replicated state machine
func (rf *Raft) ReadSnapshot() []byte {
	return rf.persister.ReadSnapshot()
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

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		newEntry := LogEntry{
			Index:   rf.LogIndex(len(rf.logs)),
			Term:    rf.currentTerm,
			Command: command,
		}
		index = newEntry.Index
		term = newEntry.Term
		rf.logs = append(rf.logs, newEntry)
		rf.persist()
		DPrintf("[%d-%s-%d] get new command %v of index %d", rf.me, rf.state, rf.currentTerm, command, index)
		rf.sendAppendEntriesToFollower()	// to improve speed
	} else {
		isLeader = false
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

	// fake log entry, because the first entry's index should be 1
	rf.logs = make([]LogEntry, 1)
	rf.logs[0].Term = 0
	rf.logs[0].Index = 0

	rf.state = FOLLOWER
	rf.currentTerm = 0
	rf.votedFor = NOTVOTED
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	rf.applyCh = applyCh
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeoutTimer()
	DPrintf("[%d] is initialized\n", rf.me)

	return rf
}
