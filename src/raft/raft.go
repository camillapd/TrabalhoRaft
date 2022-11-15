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
	"math/rand"
	"sync"
	"time"
)

// variáveis globals da state machine
// a qualquer momento cada servidor pode ter um dos três estados
const (
	LEADER    = 1
	FOLLOWER  = 2
	CANDIDATE = 3
)

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

	// persistent on all servers
	currentTerm int          // latest term server has seen (initialized to 0 on first boot, increases monotonically)
	votedFor    int          // candidateId that received vote in current term (or null if none)
	log         []LogEntries // log entries

	// volatile on all servers
	commitIndex int // ponteiro para índice de log???
	// o índice que deve ser usado na próxima entrada do log: LastLogIndex +1
	lastApplied int // index of highest log entry applied to state machine (initialized to 0, increases monotonically)

	// volatile on leaders - reinitialized after election
	nextIndex  []int // for each server, index of the next log entry to send to that server (initialized to leaderlast log index + 1)
	matchIndex []int // for each server, index of highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	state           int // o estado do servidor raft
	votes           int
	electionTimeout *time.Timer
	electionTimer   time.Duration
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int = rf.currentTerm
	var isleader bool = false

	if rf.state == LEADER {
		isleader = true
	}

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
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm

	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		if args.Term < rf.currentTerm { // como no paper: Reply false if term < currentTerm
			reply.VoteGranted = false
		} else if args.Term > rf.currentTerm {
			// como no paper: If RPC request or response contains term T > currentTerm: set currentTerm = T
			rf.currentTerm = args.Term
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		}
	} else {
		reply.VoteGranted = false
	}

	rf.mu.Unlock()
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
// log entries; each entry contains command for state machine, and term when entry
// was received by leader (first index is 1)
type LogEntries struct {
	Index   int
	Term    int
	Command interface{}
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
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()

	reply.Term = rf.currentTerm // o valor do AppendEntriesReply vai receber sempre o term atual do raft
	reply.Success = true        // é verdadeiro a menos que o if aconteça
	rf.ResetElectionTimeout()

	if args.Term < rf.currentTerm { // como no paper: Reply false if term < currentTerm
		reply.Success = false
	} else if args.Term > rf.currentTerm { // como no paper: If RPC request or response contains term T > currentTerm: set currentTerm = T
		rf.currentTerm = args.Term
	}

	rf.mu.Unlock()
}

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

func (rf*Raft) ResetElectionTimeout(){
	rand.Seed(time.Now().UnixNano())
	rf.electionTimer = time.Duration(rand.Intn(100)) + 400 // et espera por, no max, meio segundo
	rf.electionTimeout.Reset(rf.electionTimer)
}

// a go routine que verifica electionTimeout e lança Heartbeats
func (rf *Raft) Loop(){

	for true {
		for rf.state == LEADER{
			go rf.Heartbeat() // ou rf.Heartbeat    qual a diferença?
		}

		for rf.state == FOLLOWER{
			
			<-ref.electionTimeout.C //se o et estourar:
			go rf.LeaderElection()

			// isso faz sentido? essa parte de channels me confundiu um bocado

		}
	}

}

func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := rf.currentTerm
	isLeader := false

	if rf.state == LEADER {
		isLeader = true
	}

	// Não modificar

	return index, term, isLeader
}

// a go routine que implementa a eleição de líderes
func (rf *Raft) LeaderElection() {

	go func() {
		for true {
			rf.mu.Lock()
			if rf.state == FOLLOWER {
				rf.state = CANDIDATE
				continue
			}

			if rf.state == CANDIDATE {
				rf.currentTerm = rf.currentTerm + 1
				rf.votes = 1

				rf.ResetElectionTimeout()

				requestVoteReply := &RequestVoteReply{}

				// manda a mensagem para votar
				for i := 0; i < len(rf.peers); i++ {
					if i != rf.me {
						go func(i int) {
							rf.mu.Lock()
							requestVoteArgs := &RequestVoteArgs{}
							requestVoteArgs.Term = rf.currentTerm
							requestVoteArgs.CandidateId = rf.me
							rf.mu.Unlock()

							rf.RequestVote(requestVoteArgs, requestVoteReply)

							if rf.sendRequestVote(i, requestVoteArgs, requestVoteReply) {
								rf.mu.Lock()

								if requestVoteReply.VoteGranted && rf.state == CANDIDATE {
									rf.votes = rf.votes + 1
									if rf.votes > len(rf.peers)/2 {
										rf.state = LEADER
										rf.currentTerm = rf.currentTerm + 1
										rf.ResetElectionTimeout()
										go rf.Heartbeat()
									}
								} else if requestVoteReply.Term > rf.currentTerm {
									rf.currentTerm = requestVoteReply.Term
									rf.state = FOLLOWER
									rf.votes = 0
									rf.votedFor = -1
									rf.ResetElectionTimeout()
								}
								rf.mu.Unlock()
							}
						}(i)
					}
				}
			}

			if rf.state == LEADER {
				continue
			}

			rf.mu.Unlock()

			/*
				To begin an election, a follower increments its current
				term and transitions to candidate state. It then votes for
				itself and issues RequestVote RPCs in parallel to each of
				the other servers in the cluster. A candidate continues in
				this state until one of three things happens: (a) it wins the
				election, (b) another server establishes itself as leader, or
				(c) a period of time goes by with no winner.
			*/
		}
	}()
}

// a go routine que implementa os heartbeats
func (rf *Raft) Heartbeat() {

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			if rf.state == LEADER {
				go func(i int) {
					appendEntriesArgs := &AppendEntriesArgs{}

					rf.mu.Lock()
					appendEntriesArgs.Term = rf.currentTerm
					appendEntriesArgs.LeaderId = rf.me
					appendEntriesArgs.LeaderCommit = rf.commitIndex
					rf.mu.Unlock()

					appendEntriesReply := &AppendEntriesReply{}
					rf.peers[i].Call("Raft.AppendEntries", appendEntriesArgs, appendEntriesReply)

					rf.mu.Lock()
					if appendEntriesReply.Term > rf.currentTerm || !appendEntriesReply.Success {
						rf.currentTerm = appendEntriesArgs.Term
						rf.state = FOLLOWER
						rf.votes = 0
						rf.votedFor = -1
						rf.ResetElectionTimeout()
					}
					rf.mu.Unlock()
				}(i)
			} else {
				return
			}
		}
	}

	time.Sleep(125 * time.Millisecond) // garantir que serão menos de 10 hb/s conforme regras do fim do codigo

	/* Leaders send periodic
	heartbeats (AppendEntries RPCs that carry no log entries)
	to all followers in order to maintain their authority.
	If a follower receives no communication
	over a period of time called the election timeout,
	then it assumes there is no viable leader
	and begins an election to choose a new leader
	*/

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
	persister *Persister, applyCh chan ApplyMsg) *Raft { // WIP
	rf := &Raft{}            // pega o endereço do Raft, é o objeto Raft
	rf.peers = peers         // endpoint do RPC dos peers, é um vetor de ponteiros de ClientEnd
	rf.persister = persister // guarda o estado persistido, é um ponteiro de Persister
	rf.me = me               // índice do peer desse servidor, é um int

	// inicializações dos estados do raft
	// os vetores não precisam inicializar porque no Go começam com 0 por default
	rf.currentTerm = 0
	rf.votedFor = -1

	rf.commitIndex = 0
	rf.lastApplied = 0

	rf.state = FOLLOWER
	rf.votes = 0
	rand.Seed(time.Now().UnixNano())
	rf.electionTimer = time.Duration(rand.Intn(100)) + 400
	rf.electionTimeout = time.NewTimer(rf.electionTimer * time.Millisecond)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// aqui é a goroutine para começar a eleição
	go rf.LeaderElection()
	
	// goroutine para verificar hb e timeout
	go rf.Loop()

	return rf
}

/* Anotações

-- Sobre GO

	Goroutines são funções ou métodos executados em concorrência.
	Podemos pensar nelas como uma especie de lightweight thread que são gerenciadas pelo runtime do Go.

	channel, a concurrency-safe communication object

*/
