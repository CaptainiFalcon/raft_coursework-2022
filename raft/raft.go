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

import "sync"
import "labrpc"

import "bytes"
import "encoding/gob"
import "time"
import "math/rand"
import "fmt"
import "strconv"

var debug bool = true
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}
const (
	LEADER = 0
	CANDIDATE = 1
	FOLLOWER = 2

	HeartbeatTime = 50
	ElectionMinTime = 170
	ElectionMaxTime = 300
)

type LogEntry struct {
	Term 	int
	Command interface{} 	// see "type ApplyMsg struct"
}
//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// according to Fig.2 in the paper
	// Persistent data
	currentTerm	int		// latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor 	int		// candidateId that received vote in current	term (or null if none)
	logs		[]LogEntry //each entry contains command for state machine, and term when entry	was received by leader 

	// Volatile data
	state		int		// 0 leader  1 candidate  2 follower
	votesCount	int		// how many votes I got in a election
	timer		*time.Timer		// for sending heartbeat or starting a election when followers does not receive a heartbeat or appendenties from leader
	applyCh		chan ApplyMsg

	commitIndex	int		// index of highest log entry known to be committed (initialized to 0, increases monotonically)
	lastApplied	int		// index of highest log entry applied to state machine (initialized to 0, increases	monotonically)

	// the follow data are use by leader only, and need to reinitialize after election
	nextIndex	[]int	// for each server, index of the next log entry	to send to that server (initialized to leader last log index + 1)
	matchIndex	[]int	// for each server, index of highest log entry known to be replicated on server	(initialized to 0, increases monotonically)
						// matchIndex is exist for leader to see a index whether already copy to other sever surpass majority 
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
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
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logs)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data != nil {
		r := bytes.NewBuffer(data)
		d := gob.NewDecoder(r)
		d.Decode(&rf.currentTerm)
		d.Decode(&rf.votedFor)
		d.Decode(&rf.logs)
	}
}




//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	// see Fig.2
	//Rpc-related structure fields should start with an uppercase letter because of the syntax of the Go language
	Term			int			// candidate’s term
	CandidateId		int			// who requesting vote
	LastLogIndex	int			// index of candidate’s last log entry (§5.4)
	LastLogTerm		int			// term of candidate’s last log entry (§5.4)
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	// see Fig.2
	// Rpc-related structure fields should start with an uppercase letter because of the syntax of the Go language
	Term			int			// currentTerm, for candidate to update itself
	VoteGranted		bool		// true means candidate received vote
}

type AppendEntryArgs struct {
	// see Fig.2
	// Rpc-related structure fields should start with an uppercase letter because of the syntax of the Go language
	Term			int			// leader’s term
	LeaderId		int
	PrevLogIndex	int			// index of log entry immediately preceding	new ones
	PrevLogTerm		int			// term of prevLogIndex entry
	Entries 		[]LogEntry	// log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit	int			// leader’s commitIndex
}

type AppendEntryReply struct {
	Term 			int
	Success 		bool
	EntriesCount	int			// how many entries leader sent to me
}

//
// example RequestVote RPC handler.
//
func int_max(a int, b int)(int) {
	if a > b {
		return a
	}
	return b
}

func int_min(a int, b int)(int) {
	if a < b {
		return a
	}
	return b
}

func (rf *Raft) SendRequestVoteToAll(args RequestVoteArgs) {
	for  peer:= 0; peer < len(rf.peers); peer++ {
		if peer == rf.me {
			continue
		}
		go func(peer int, args RequestVoteArgs) {   // RPC, ask others to give me their vote
			var reply RequestVoteReply				// store the result of RPC
			ret := rf.peers[peer].Call("Raft.RequestVote", args, &reply)
			if ret {	// if the remote procudure call successful, then go to analyse the result
				rf.getVoteResult(reply)
			}
		}(peer, args)
	}
}

func (rf *Raft) TimeOut() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == LEADER { // if it is leader now, just to send a heartbeat
		rf.SendAppendEntriesToAll()
	} else {			// if it is not leader, it means to start a election
		rf.state = CANDIDATE
		rf.votedFor = rf.me
		rf.currentTerm += 1
		rf.persist()
		rf.votesCount = 1
		var args RequestVoteArgs
		args.Term = rf.currentTerm
		args.CandidateId = rf.me
		args.LastLogIndex = len(rf.logs) - 1
		if args.LastLogIndex >= 0 {
			args.LastLogTerm = rf.logs[args.LastLogIndex].Term
		}
		rf.SendRequestVoteToAll(args);
	}
	// leader and follower both need to reset tiemr
	rf.ResetTimer()
}

// when follower receive a "AppendEnties", he reset his timer for starting a election
// leader reset his timer for heartbeat
func (rf *Raft) ResetTimer() {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()

	CSMA_time := time.Duration(HeartbeatTime) * time.Millisecond   // it is similar with wireless communication
	if rf.state != LEADER {
		CSMA_time = time.Duration(ElectionMinTime + rand.Int63n(ElectionMaxTime - ElectionMinTime)) * time.Millisecond
	}
	if rf.timer == nil {
		rf.timer = time.NewTimer(CSMA_time)
		go func() {
			for {
				<-rf.timer.C
				rf.TimeOut()
			}
		}()
	}
	rf.timer.Reset(CSMA_time)
}

// call by leader to inform me that he is leader 
// call by leader to send his entry to me
func (rf *Raft) AppendEntries(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {		// he is not a leader anymore
		reply.Success = false
		reply.Term = rf.currentTerm
	// } else if args.Term == rf.currentTerm {
		// fmt.Printf("\nargs.Term == rf.currentTerm in appendentries  " + strconv.Itoa(len(args.Entries)))
	} else {
		// his Term larger than me, so I'am follower
		rf.state = FOLLOWER
		rf.currentTerm = args.Term			// change persistent data
		rf.votedFor = -1
		reply.Term = args.Term
		
		// if args.PrevLogIndex >= 0 && (len(rf.logs) - 1 < args.PrevLogIndex || args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term) {
		if args.PrevLogIndex >= 0 && (len(rf.logs) - 1 < args.PrevLogIndex) {
			reply.Success = false
			reply.EntriesCount = 0
		} else if args.PrevLogIndex >= 0 && (args.PrevLogIndex < len(rf.logs)) && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {  
		//If an existing entry conflicts with a new one (same index	but different terms), delete the existing entry and all that follow it (§5.3)
			rf.logs = rf.logs[ : args.PrevLogTerm]
			reply.Success = false
			fmt.Printf("\ndelete me and the after   " + strconv.Itoa(args.PrevLogIndex))

		} else if args.Entries == nil { // the leader inform me that he is leader or heartbeat
			// if args.PrevLogIndex + 1 >= 0 {
			rf.logs = rf.logs[ : args.PrevLogIndex + 1]
			// }
			reply.Success = true
			reply.EntriesCount = 0
		} else {
			rf.logs = rf.logs[ : args.PrevLogIndex + 1]
			rf.logs = append(rf.logs, args.Entries...)
			reply.Success = true
			reply.EntriesCount = len(args.Entries)
		}
		rf.persist()
		// if rf.me == 2 && reply.Success == true {
			// fmt.Printf("\nsuccess to append to 2   " + strconv.Itoa(len(args.Entries)))
		// }
		// if args.LeaderCommit > rf.commitIndex {  // why stupiy
		// if reply.Success == true && args.LeaderCommit >= rf.commitIndex {
		// 	rf.commitIndex = int_min(args.LeaderCommit, len(rf.logs) - 1)
		// 	go rf.Commit()
		// }
		if reply.Success == true && len(rf.logs) - 1 >= args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
			go rf.Commit()
		}
		rf.ResetTimer()
	}
	// rf.ResetTimer()   // if he is not leader, you can not reset timer
}

// call by leader to inform followers that leader is alive
// call by leader to send his enties to followers
func (rf *Raft) SendAppendEntriesToAll() {
	for peer := 0; peer < len(rf.peers); peer++ {
		// if peer == rf.me || rf.nextIndex[peer] == len(rf.logs) {  // do not be like that, because heartbeat also through here
		if peer == rf.me {
			continue
		}
		var args AppendEntryArgs
		args.Term = rf.currentTerm
		args.LeaderId = rf.me

		args.PrevLogIndex = rf.nextIndex[peer] - 1
		if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.logs) {
			args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
		}
		if rf.nextIndex[peer] >= 0 && rf.nextIndex[peer] < len(rf.logs) {
			args.Entries = rf.logs[rf.nextIndex[peer] : ]
		}
		args.LeaderCommit = rf.commitIndex
		if peer == 1 && debug {
			// fmt.Printf("\nSendAppend     " + strconv.Itoa(len(args.Entries)) + "      " + strconv.Itoa(rf.me))
		}
		go func(peer int, args AppendEntryArgs) {
			var reply AppendEntryReply
			ret := rf.peers[peer].Call("Raft.AppendEntries", args, &reply)
			if ret {
				rf.AfterSendAppendEntries(peer, reply)
			}
		}(peer, args);
	}
}

// for leader to get the result of appending entries to followers
func (rf *Raft) AfterSendAppendEntries(peer int, reply AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != LEADER {   // when network is slow, this may happen
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term    // if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.ResetTimer()
		return
	}

	if reply.Success == true {
		rf.nextIndex[peer] += reply.EntriesCount
		rf.matchIndex[peer] = int_max(rf.nextIndex[peer] - 1, rf.matchIndex[peer])
		/* If there exists an N such that N > commitIndex, a majority
			of matchIndex[i] ≥ N, and log[N].term == currentTerm:
			set commitIndex = N (§5.3, §5.4).
			here, rf.matchIndex[peer] is the N
		*/
		N := rf.matchIndex[peer]
		cnt := 1
		for peer_1 := 0; peer_1 < len(rf.peers); peer_1++ {
			if peer_1 != rf.me && rf.matchIndex[peer_1] >= N { 
				// matchIndex is exist for leader to see a index whether already copy to other sever surpass majority 
				cnt += 1
			}
		}
		if cnt > len(rf.peers) / 2 {
			// if rf.commitIndex < N && rf.logs[N].Term == rf.currentTerm {  // that's wrong
			if rf.commitIndex < N && N < len(rf.logs) && rf.logs[N].Term == rf.currentTerm {
				rf.commitIndex = N
				go rf.Commit()
			}
		}

	} else { //I'm leader
		rf.nextIndex[peer] -= 1
		// rf.nextIndex[peer] = 0
		rf.nextIndex[peer] = int_max(0, rf.nextIndex[peer])
		rf.SendAppendEntriesToAll()
	}

}

func (rf *Raft) Commit() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		var args ApplyMsg
		args.Index = i + 1
		if i == len(rf.logs) {
			fmt.Printf("\nwhat the fuck " + strconv.Itoa(rf.commitIndex))
		}
		args.Command = rf.logs[i].Command
		rf.applyCh <- args
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()			// unlock automatically when function return
	// First, if the logs have last entries with different terms, then the log with the later term is more up-to-date. 
	// Second, if the logs end with the same term, then whichever log is longer is more up-to-date.
	// the voter denies its vote if its own log is more up-to-date than that of the candidate.
	//MUTD: more up-to-date
	candidate_log_MUTD := true		// initially, all processes are up-to-date, so it's true initially
	if len(rf.logs) > 0 {			// after initial
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm || (rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex){
			candidate_log_MUTD = false
		}
	}

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		// If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && candidate_log_MUTD {
			rf.votedFor = args.CandidateId		// is persistent data
			reply.Term = args.Term
			reply.VoteGranted = true
			rf.state = FOLLOWER
			rf.persist()
		}
	} else {
		rf.currentTerm = args.Term  // if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
		rf.state = FOLLOWER			// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state.
		if candidate_log_MUTD == true {
			rf.votedFor = args.CandidateId
		} else {
			rf.votedFor = -1		// meet a larger Term but it's log is too old, so now vote for none
		}
		rf.persist()
		// rf.ResetTimer()			// because of changing state
		reply.Term = args.Term
		reply.VoteGranted = (rf.votedFor == args.CandidateId)
	}
	if reply.VoteGranted == true {
		rf.ResetTimer()
	}
	return
}

func (rf *Raft) getVoteResult(reply RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// his Term must as same as my Term, otherwise it's an old and unvalid vote
	if reply.Term < rf.currentTerm { //when peer broken and reboot later or slow network, this situation maybe heppen
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term		// if one server’s current term is smaller than the other’s, then it updates its current term to the larger value.
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.persist()
		rf.ResetTimer()
		return
	}

	// when everything is ok, and I win the vote of that follower, the follow code should be executed
	if rf.state == CANDIDATE && reply.VoteGranted == true {
		rf.votesCount += 1
		if rf.votesCount > len(rf.peers) / 2 {
			rf.state = LEADER
			for peer := 0; peer < len(rf.peers); peer++ { 
				rf.nextIndex[peer] = len(rf.logs) 	// initialized to leader last log index + 1
				rf.matchIndex[peer] = -1 		  	// how many corrent entries that sever has already， (initialized to 0, increases monotonically)
				// paper's index is start from 1, so it initialize to be 0, here, we should be -1
			}
			rf.SendAppendEntriesToAll()		// inform others immediately
			rf.ResetTimer()					// and reset timer for next heartbeat
		}
	}
	return
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
// func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
// 	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
// 	return ok
// }


//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
// use for the cilients communicating with leader to hand in a command
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here, and you can change the code above
	// if this server isn't the leader, returns false.
	if rf.state != LEADER {
		return index, term, isLeader 
	}

	var log LogEntry
	log.Command = command
	log.Term = rf.currentTerm
	rf.logs = append(rf.logs, log)
	index = len(rf.logs)
	isLeader = true
	term = rf.currentTerm
	rf.persist()
	rf.SendAppendEntriesToAll()
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
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

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	// rf.persist()		// how folish is you
	rf.state = FOLLOWER
	
	rf.applyCh = applyCh
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.nextIndex = make([]int, len(peers))		// automatically initialize to be 0
	rf.matchIndex = make([]int, len(peers))  
	rf.ResetTimer()			// for initing the first election
	
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	return rf
}
