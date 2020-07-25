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
	"sync"
	"sync/atomic"
	"time"
)


// import "bytes"
// import "../labgob"


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
	lastLogIndex := rf.LogIndex(len(rf.logs) - 1) // rf.getLastLogEntry().Index
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

func (rf *Raft) LogIndex(index int) int {
	return index + rf.lastSnapshotIndex
}

func (rf *Raft) ArrayIdx(index int) int {
	return index - rf.lastSnapshotIndex
}


// get the last log entry
//func (rf *Raft) getLastLogEntry() LogEntry {
//	entry := rf.logs[len(rf.logs) - 1]
//	return entry
//}

//get the index of the last entry in a specified term
func (rf *Raft) getLastIndexOfTerm(prevLogIndex int, conflictTerm int) int {
	for idx := prevLogIndex; idx >= rf.lastSnapshotIndex + 1; idx -= 1 {
		if rf.logs[rf.ArrayIdx(idx)].Term == conflictTerm {
			return idx
		}
	}
	return -1
	//for idx := rf.getLastLogEntry().Index; idx > 0; idx -= 1 {
	//	if rf.logs[idx].Term == term {
	//		return rf.logs[idx].Index
	//	}
	//	if rf.logs[idx].Term < term {
	//		break
	//	}
	//}
	//return -1
}

//get the index of the first entry in a specified term
func (rf *Raft) getFirstIndexOfTerm(prevLogIndex int, conflictTerm int) int {
	conflictIndex := prevLogIndex
	for rf.logs[rf.ArrayIdx(conflictIndex - 1)].Term == conflictTerm {
		conflictIndex -= 1
		if conflictIndex == rf.lastSnapshotIndex + 1 {
			break
		}
	}
	return conflictIndex
	//for idx := 1; idx <= rf.LogIndex(len(rf.logs) - 1); idx += 1 {
	//	if rf.logs[idx].Term == term {
	//		return rf.logs[idx].Index
	//	}
	//	if rf.logs[idx].Term > term {
	//		break
	//	}
	//}
	//return -1
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
	for getVoteNum <= peersNum / 2 && finishedVoteNum != peersNum /*&& rf.state == CANDIDATE*/ {	// conditions of waiting
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
	// Your code here (2A, 2B).
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
	lastLogEntry := rf.logs[len(rf.logs) - 1] //rf.getLastLogEntry()
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
		//prevLogIndex := rf.nextIndex[svr] - 1
		if rf.nextIndex[svr] <= rf.lastSnapshotIndex {
			rf.sendInstallSnapToSvr(svr)
			continue
		}

		//me := rf.me
		//state := rf.state
		//currentTerm := rf.currentTerm
		appendEntriesArgs := AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			PrevLogIndex: rf.nextIndex[svr] - 1,
			PrevLogTerm:  rf.logs[rf.ArrayIdx(rf.nextIndex[svr] - 1)].Term,
		}
		if rf.LogIndex(len(rf.logs)- 1) >= rf.nextIndex[svr] {		// get log entry
			appendEntriesArgs.Entries = rf.logs[rf.ArrayIdx(rf.nextIndex[svr]): ]
		}

		go func(svr int, appendEntriesArgs AppendEntriesArgs) {
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
			// handle response of RPC request
			hasNewCommit := false 	// whether there is new commit
			if appendEntriesReply.Success {
				//DPrintf("[%d-%s-%d] append entry to %d success", rf.me, rf.state, rf.currentTerm, svr)
				rf.matchIndex[svr] = appendEntriesArgs.PrevLogIndex + len(appendEntriesArgs.Entries)	// update matchIndex
				rf.nextIndex[svr] =  rf.matchIndex[svr] + 1												// update nextIndex
				hasNewCommit = rf.commitLogEntry()
			} else {
				//DPrintf("[%d-%s-%d] append entry to %d fails", rf.me, rf.state, rf.currentTerm, svr)
				if appendEntriesReply.IsConflict {		// if there is a confliction
					lastIndexOfTerm := rf.getLastIndexOfTerm(appendEntriesArgs.PrevLogIndex, appendEntriesReply.ConflictTerm)
					if lastIndexOfTerm == -1 {	// leader doesn't has the log in conflict term
						rf.nextIndex[svr] = appendEntriesReply.ConflictIndex
					} else {
						rf.nextIndex[svr] = lastIndexOfTerm + 1
					}
				} else {	// log missing
					rf.nextIndex[svr] = appendEntriesReply.LastIndex + 1
				}
			}
			//rf.mu.Unlock()
			if hasNewCommit {	// if there is a new commit, trigger apply
				//rf.triggerApply <- true
				rf.applyCommitEntry()
			}
			rf.mu.Unlock()
		}(svr, appendEntriesArgs)
	}

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
		}
	}
	//for N := rf.commitIndex + 1; N <= rf.LogIndex(len(rf.logs) - 1); N += 1 {
	//	repliCnt := 1
	//	for svr := 0; svr < len(rf.peers); svr += 1 {
	//		if svr == rf.me {
	//			continue
	//		}
	//		if rf.matchIndex[svr] >= N {
	//			repliCnt += 1
	//		}
	//	}
	//	if repliCnt > len(rf.peers) / 2 && rf.logs[rf.ArrayIdx(N)].Term == rf.currentTerm {
	//		rf.commitIndex = N
	//	}
	//}
	if rf.commitIndex > oldCommitIndex {
		DPrintf("[%d-%s-%d] log entry is %v", rf.me, rf.state, rf.currentTerm, rf.logs)
		DPrintf("[%d-%s-%d] update commitIndex to %d", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
		return true
	} else {
		return false
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

	if args.PrevLogIndex <= rf.lastSnapshotIndex {
		reply.Success = true
		if args.PrevLogIndex + len(args.Entries) > rf.lastSnapshotIndex {
			appendIndex := rf.lastSnapshotIndex - args.PrevLogIndex
			rf.logs = rf.logs[: 1]
			rf.logs = append(rf.logs, args.Entries[appendIndex: ]...)
			rf.persist()
		}
		//defer rf.mu.Unlock()
		// return
	} else if rf.LogIndex(len(rf.logs) - 1) < args.PrevLogIndex {	// log missing
		reply.Success = false
		reply.IsConflict = false
		reply.LastIndex = rf.LogIndex(len(rf.logs) - 1)
		defer rf.mu.Unlock()
		return
	} else if rf.logs[rf.ArrayIdx(args.PrevLogIndex)].Term != args.PrevLogTerm {	// log conflict
		reply.Success = false
		reply.IsConflict = true
		reply.ConflictTerm = rf.logs[rf.ArrayIdx(args.PrevLogIndex)].Term
		reply.ConflictIndex = rf.getFirstIndexOfTerm(args.PrevLogIndex, reply.ConflictTerm)
		defer rf.mu.Unlock()
		return
	} else {	// start to append log
		reply.Success = true

		hasConfict := false
		thisIdx := args.PrevLogIndex + 1
		entriesIdx := 0
		for ; entriesIdx < len(args.Entries); entriesIdx += 1 {
			if rf.ArrayIdx(thisIdx) >= len(rf.logs) || args.Entries[entriesIdx].Term != rf.logs[rf.ArrayIdx(thisIdx)].Term {
				hasConfict = true
				break
			}
			thisIdx += 1
		}
		if hasConfict {
			newLogs := make([]LogEntry, rf.ArrayIdx(thisIdx))
			copy(newLogs, rf.logs)	// if not use copy(), there would be a race warning (-race)
			rf.logs = newLogs
			rf.logs = append(rf.logs, args.Entries[entriesIdx: ]...)
			//for ; entriesIdx < len(args.Entries); entriesIdx += 1 {
			//	rf.logs = append(rf.logs, args.Entries[entriesIdx])
			//}
			rf.persist()
		}

		//DPrintf("[%d-%s-%d] append log entries success", rf.me, rf.state, rf.currentTerm)
	}
	// check new commit
	hasNewCommit := false
	if args.LeaderCommit > rf.commitIndex {
		hasNewCommit = true
		lastNewIndex := rf.LogIndex(len(rf.logs) - 1)//rf.getLastLogEntry().Index
		if args.LeaderCommit < lastNewIndex {	// figure 2 AppendEntries RPC - Receiver implementation 5
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastNewIndex
		}

		DPrintf("[%d-%s-%d] log entry is %v", rf.me, rf.state, rf.currentTerm, rf.logs)
		DPrintf("[%d-%s-%d] update commitIndex to %d", rf.me, rf.state, rf.currentTerm, rf.commitIndex)
	}
	//rf.mu.Unlock()

	// trigger apply log entry
	if hasNewCommit {
		//rf.triggerApply <- true
		rf.applyCommitEntry()
	}
	rf.mu.Unlock()
}


// InstallSnapshot
func (rf *Raft) sendInstallSnap(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapToSvr(svr int) {
	DPrintf("[%v-%v-%v] send InstallSnapshot to %v", rf.me, rf.state, rf.currentTerm, svr)
	installSnapshotArgs := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastSnapshotIndex,
		LastIncludedTerm:  rf.logs[0].Term,
		Data:              rf.persister.ReadSnapshot(),
	}
	go func(svr int, installSnapshotArgs InstallSnapshotArgs) {
		installSnapshotReply := InstallSnapshotReply{}
		if !rf.sendInstallSnap(svr, &installSnapshotArgs, &installSnapshotReply) {
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
		if rf.matchIndex[svr] < installSnapshotArgs.LastIncludedIndex {	// ???
			rf.matchIndex[svr] = installSnapshotArgs.LastIncludedIndex
		}
		rf.nextIndex[svr] = rf.matchIndex[svr] + 1
		rf.mu.Unlock()
	}(svr, installSnapshotArgs)
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("[%v-%v-%v] get InstallSnapshot from %v", rf.me, rf.state, rf.currentTerm, args.LeaderId)
	if rf.isTermOutOfDate(args.Term) {
		rf.convertToFollower(args.Term)
	}
	reply.Term = rf.currentTerm
	rf.resetElectionTimeout()

	if args.Term != rf.currentTerm || args.LastIncludedIndex <= rf.lastSnapshotIndex {
		return
	}

	lastIncludedArrayIdx := rf.ArrayIdx(args.LastIncludedIndex)
	// if exsiting log entry has same index and term as snapshot's last included entry, retain log entries following it
	if len(rf.logs) > lastIncludedArrayIdx /*&& rf.logs[lastIncludedArrayIdx].Term == args.LastIncludedTerm*/ {
		rf.logs = append(make([]LogEntry, 0), rf.logs[lastIncludedArrayIdx: ]...)
	} else {	// else, discard the entire log
		rf.logs = make([]LogEntry, 1)
		rf.logs[0].Term = args.LastIncludedTerm
		rf.logs[0].Index = args.LastIncludedIndex
	}
	rf.lastSnapshotIndex = args.LastIncludedIndex
	if rf.commitIndex < rf.lastSnapshotIndex {
		rf.commitIndex = rf.lastSnapshotIndex
	}
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex
	}
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), args.Data)

	if rf.lastApplied > rf.lastSnapshotIndex { 	// for linearizability
		return
	}
	snapshotMsg := ApplyMsg{
		CommandValid: false,
		Command:      nil,
		CommandIndex: rf.lastSnapshotIndex,
		Snapshot:     rf.persister.ReadSnapshot(),
	}
	go func(snapshotMsg ApplyMsg) {
		rf.applyCh <- snapshotMsg
	}(snapshotMsg)
}

// goroutine for applying committed log entry
func (rf *Raft) applyCommitEntry() {
	//rf.mu.Lock()
	if rf.lastApplied < rf.lastSnapshotIndex {
		rf.lastApplied = rf.lastSnapshotIndex
	}
	if rf.commitIndex < rf.lastSnapshotIndex {
		rf.commitIndex = rf.lastSnapshotIndex
	}

	if rf.commitIndex <= rf.lastApplied {
		//rf.mu.Unlock()
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
	//rf.mu.Unlock()

}

//func (rf *Raft) applyCommitEntry() {	// ??? diff
//	for {
//		<- rf.triggerApply
//
//		rf.mu.Lock()
//		if rf.commitIndex <= rf.lastApplied {
//			rf.mu.Unlock()
//			continue
//		}
//		applyEntires := append([]LogEntry{},
//			rf.logs[rf.ArrayIdx(rf.lastApplied + 1): rf.ArrayIdx(rf.commitIndex + 1)]...)
//
//		go func(startIdx int, applyEntries []LogEntry) {
//			for idx, entry := range applyEntires {
//				applyMsg := ApplyMsg{
//					CommandValid: true,
//					Command:      entry.Command,
//					CommandIndex: startIdx + idx,
//					Snapshot:     nil,
//				}
//				rf.applyCh <- applyMsg
//				rf.mu.Lock()
//				if rf.lastApplied < applyMsg.CommandIndex {
//					rf.lastApplied = applyMsg.CommandIndex
//				}
//				rf.mu.Unlock()
//			}
//		}(rf.lastApplied + 1, applyEntires)
//		rf.mu.Unlock()
//	}
//}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.encodeState())
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}
	// Your code here (2C).
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


func (rf *Raft) RaftStateSize() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// called by replicated state machine
func (rf *Raft) SaveStateAndSnapshot(requestSnapshotIndex int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if requestSnapshotIndex <= rf.lastSnapshotIndex {
		return
	}
	arryIdx := rf.ArrayIdx(requestSnapshotIndex)
	if arryIdx >= len(rf.logs) {
		return
	}

	rf.logs = append(make([]LogEntry, 0), rf.logs[arryIdx: ]...)
	// rf.logs = rf.logs[arryIdx: ]	// remain logs[0] as lastSnapshotIndex
	rf.lastSnapshotIndex = requestSnapshotIndex
	state := rf.encodeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)

	//update for other nodes ???

}

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

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == LEADER {
		newEntry := LogEntry{
			Index:   rf.LogIndex(len(rf.logs)), // rf.getLastLogEntry().Index + 1,
			Term:    rf.currentTerm,
			Command: command,
		}
		index = newEntry.Index
		term = newEntry.Term
		rf.logs = append(rf.logs, newEntry)
		rf.persist()
		DPrintf("[%d-%s-%d] get new command %v of index %d", rf.me, rf.state, rf.currentTerm, command, index)
		rf.sendAppendEntriesToFollower() // ???
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
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.lastSnapshotIndex = 0
	rf.applyCh = applyCh
	//rf.triggerApply = make(chan bool)
	rf.resetElectionTimeout()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.electionTimeoutTimer()
	//go rf.applyCommitEntry()
	DPrintf("[%d] is initialized\n", rf.me)

	return rf
}
