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
	"labrpc"
	"sync"
)

// import "bytes"
// import "encoding/gob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	currentTerm int  // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    *int // candidateId that received vote in current term (or null if none)
	log         []LogEntries
	// log entries; each entry contains command for state machine,
	// and term when entry was received by leader (first index is 1)

	commitIndex int // ponteiro para índice de log???
	// o índice que deve ser usado na próxima entrada do log: LastLogIndex +1
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Não modificar.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Não modificar.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	Term         int // candidateTerm
	CandidateId  int // candidate resquesting vote
	LastLogIndex int // index of candidate’s last log entry
	LastLogTerm  int // term of candidate’s last log entry
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

// example RequestVote RPC handler.
// Receiver implementation:
// 1. Reply false if term < currentTerm
// 2. If votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {

	if args.Term < rf.currentTerm { 
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.votedFor == nil || rf.votedFor == args.candidateId { // falta isso: or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote
		if args.LastLogTerm >= rf.log[commitIndex-1].term && args.LastLogIndex >= (rf.commitIndex)-1 { // candidato.log tá mais avançado do que o do seguidor em 1 term e o último comando recebido é mais recente ou igual
			rf.votedFor = args.candidateId
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true
		} 
	}

	/* Só escrevi a lógica da condição, ainda não sei exatamente como funcionam as funções em Go,
	então não sei como deve ser o retorno, já que esse "reply" me parece um argumento de entrada.
	Como só segui o padrão das linhas que já estavam escritas, podem existir erros sintáticos. */
}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// You'll also need to define a struct to hold information about each log entry.
type LogEntries struct {
	index	int
	term 	int
	command interface{}

	// log entries; each entry contains command for state machine, and term when entry
	// was received by leader (first index is 1)
}

// To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet)
type AppendEntriesArgs struct {
	Term         int          // leader's Term
	LeaderId     int          // so follower can redirect clients
	PrevLogIndex int          // index of log entry immediately preceding new ones
	PrevLogTerm  int          // term of prevLogIndex entry
	Entries      []LogEntries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int          // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

// Write an AppendEntries RPC handler method that resets the election timeout
// so that other servers don't step forward as leaders when one has already been elected.
// Receiver implementation:
// 1. Reply false if term < currentTerm
// 2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
// 3. If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it
// 4. Append any new entries not already in the log
// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < reply.Term {
		reply.Success = false
	}
}

/*
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args.Term < rf.Term || rf.log[PrevLogIndex] == nil || rf.log[PrevLogIndex].term != PrevLogTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
	} else 
	
	// resetar election timeout
	// definir mudança em rf.currentTerm

	reply.Term = rf.currentTerm
	reply.Success = true

	// Não estou muito convicto desse código, então escrevi mas deixei comentado e com a versão base
}
*/

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Método é do tipo Raft, recebe uma interface como parâmetro e retorna 3 valores: int, int e boolean

	return index, term, isLeader
}

// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}            // pega o endereço do Raft, é o objeto Raft
	rf.peers = peers         // endpoint do RPC dos peers, é um vetor de ponteiros de ClientEnd
	rf.persister = persister // guarda o estado persistido, é um ponteiro de Persister
	rf.me = me               // índice do peer desse servidor, é um int

	// Your initialization code here.

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf

	/*
	 Modify Make() to create a background goroutine that will kick off leader election periodically
	 by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	 This way a peer will learn who is the leader, if there is already leader, or become itself the leader.
	 Implement the RequestVote() RPC handler so that servers will vote for one another.
	*/
}

// Anotações

/*
	A service calls Make(peers,me,…) to create a Raft peer.
	The peers argument is an array of established RPC connections, one to each Raft peer (including this one).
	The me argument is the index of this peer in the peers array.
	Start(command) asks Raft to start the processing to append the command to the replicated log.
	Start() should return immediately, without waiting for for this process to complete.
	The service expects your implementation to send an ApplyMsg for each new committed log entry to the applyCh argument to Make().

	Your Raft peers should exchange RPCs using the labrpc Go package that we provide to you.
	It is modeled after Go's rpc library, but internally uses Go channels rather than sockets.
	raft.go contains some example code that sends an RPC (sendRequestVote()) and that handles an incoming RPC (RequestVote()).
	The reason you must use labrpc instead of Go's RPC package is that the tester tells labrpc to delay RPCs,
	re-order them, and delete them to simulate challenging networks conditions under which your code should work correctly.
	Don't modify labrpc because we will test your code with the labrpc as handed out.
*/

/*
	Goroutines são funções ou métodos executados em concorrência.
	Podemos pensar nelas como uma especie de lightweight thread que são gerenciadas pelo runtime do Go.

	channel, a concurrency-safe communication object

*/

// TODO:

/*
	Implement leader election and heartbeats (AppendEntries RPCs with no log entries).
	The goal for Part 2A is for a single leader to be elected, for the leader to remain the leader if there are no failures,
	and for a new leader to take over if the old leader fails or if packets to/from the old leader are lost.
	Run go test -run 2A to test your 2A code
*/

// Hints:

/*
	1- Add any state you need to the Raft struct in raft.go. You'll also need to define a struct to hold information about each log entry.
	Your code should follow Figure 2 in the paper as closely as possible.

	2- Go marshals only the public fields in any structure passed over RPC. Public fields are the ones whose names start with capital letters.
	Forgetting to make fields public by naming them with capital letters is the single most frequent source of bugs in these labs.

	3- Fill in the RequestVoteArgs and RequestVoteReply structs. Modify Make() to create a background goroutine
	that will kick off leader election periodically by sending out RequestVote RPCs when it hasn't heard from another peer for a while.
	This way a peer will learn who is the leader, if there is already leader, or become itself the leader.
	Implement the RequestVote() RPC handler so that servers will vote for one another.

	4- To implement heartbeats, define an AppendEntries RPC struct (though you may not need all the arguments yet),
	and have the leader send them out periodically. Write an AppendEntries RPC handler method
	that resets the election timeout so that other servers don't step forward as leaders when one has already been elected.

	5- Make sure the election timeouts in different peers don't always fire at the same time,
	or else all peers will vote for themselves and no one will become leader.

	6- The tester requires that the leader send heartbeat RPCs no more than ten times per second.

	7- The tester requires your Raft to elect a new leader within five seconds of the failure of the old leader
	(if a majority of peers can still communicate). Remember, however, that leader election
	may require multiple rounds in case of a split vote (which can happen if packets are lost or
	if candidates unluckily choose the same random backoff times). You must pick election timeouts (and thus heartbeat intervals)
	that are short enough that it's very likely that an election will complete in less than five seconds even if it requires multiple rounds.

	8- The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
	Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds.
	Because the tester limits you to 10 heartbeats per second, you will have to use an election timeout larger
	than the paper's 150 to 300 milliseconds, but not too large, because then you may fail to elect a leader within five seconds.

	9- You may find Go's time and rand packages useful.

	10- If your code has trouble passing the tests, read the paper's Figure 2 again;
	the full logic for leader election is spread over multiple parts of the Figure.

	11- A good way to debug your code is to insert print statements when a peer sends or receives a message,
	and collect the output in a file with go test -run 2A > out. Then, by studying the trace of messages in the out file,
	you can identify where your implementation deviates from the desired protocol. You might find DPrintf in util.go
	useful to turn printing on and off as you debug different problems.

	12- You should check your code with go test -race, and fix any races it report
*/