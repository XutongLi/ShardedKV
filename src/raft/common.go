package raft

import (
	"sync"
	"time"
	"../labrpc"
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
	CommandValid 	bool
	Command      	interface{}
	CommandIndex 	int
	Snapshot		[]byte
}

const (
	NOTVOTED				int = -1
	LEADER 					string = "leader"
	FOLLOWER 				string = "follower"
	CANDIDATE 				string = "candidate"
	ELECTION_TIMEOUT_MIN 	int = 300
	ELECTION_TIMEOUT_MAX 	int = 500
	HEARTBEAT_INTERVAL 		int = 100
	ELECTION_INTERVAL		int = 10
)

// data structure of log entry
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
	currentTerm			int					// (persistent) latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor			int					// (persistent) candidateId that revceived vote in current term (or null if none)
	commitIndex			int					// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied			int					// index of highest log entry appelied to state machine (initialized to 0, increases monotonically)

	logs				[]LogEntry			// (persistent) array of log entries

	// on leader
	nextIndex			[]int				// for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex			[]int				// for each server, index of highest log entry known to replicated on server (initialized to 0, increases monotonically)

	electionTimeout		time.Duration		// election timeout
	lastHeartBeatTime	time.Time			// time of receiving last heartbeat

	applyCh 			chan ApplyMsg		// message applied to state machine
	triggerApply		chan bool 			// trigger apply committed log entry
}

//
// RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term 			int 	// candidate's term
	CandidateId		int 	// candidate requesting vote
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
	IsConflict		bool 		// true if there is conflict, false if there is log missing
	ConflictTerm	int 		// term of the conflict log entry
	ConflictIndex 	int 		// index of first log entry in conflict term
	LastIndex		int 		// the last log index of follower logs
}
