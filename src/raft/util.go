package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

// raft state
const (
	Follower = iota
	Candidate
	Leader
)

type LogEntry struct {
	Term    int
	Command interface{}
}

type InstallSnapshotArgs struct {
	Term             int    // 发送请求方的任期
	LeaderId         int    // 请求方的LeaderId
	LastIncludeIndex int    // 快照最后applied的日志下标
	LastIncludeTerm  int    // 快照最后applied时的当前任期
	Data             []byte // 快照区块的原始字节流数据
	//Done bool
}

type InstallSnapshotReply struct {
	Term int
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int //	需要竞选的人的任期
	CandidateId  int // 需要竞选的人的Id
	LastLogIndex int // 候选人最新的日志条目索引
	LastLogTerm  int // 候选人最新日志条目对应的任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 投票方的term，如果竞选者比自己还低就改为这个
	VoteGranted bool // 是否投票给了该竞选人
}

// AppendEntriesArgs Append Entries RPC structure
type AppendEntriesArgs struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 前一个日志的索引 (用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1)
	PrevLogTerm  int        // 前一个日志所属的任期 (用于匹配日志的任期是否是合适的是，是否有冲突)
	Entries      []LogEntry // 将要存储的日志条目列表 (为空时代表heartbeat, 有时候为了效率会发送超过一条)
	LeaderCommit int        // Leader已提交的日志条目索引 (最后一个被大多数机器都复制的日志Index)
}

type AppendEntriesReply struct {
	Term    int  // leader的term可能是过时的，此时收到的Term用于更新他自己
	Success bool //	如果follower与Args中的PreLogIndex/PreLogTerm都匹配才会接过去新的日志（追加），不匹配直接返回false
}

// 最小值min
func min(num1 int, num2 int) int {
	if num1 > num2 {
		return num2
	} else {
		return num1
	}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	//For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// ---------------------------------------

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

func HeartbeatTimeout() time.Duration {
	return 50 * time.Millisecond
}

func ElectionTimeout() time.Duration {
	return time.Duration(150+rand.Intn(200)) * time.Millisecond
}

// other server has higher term
func (rf *Raft) reset(term int) {
	rf.currentTerm = term
	rf.state = Follower
	rf.votedFor = -1
}

// 在处理别节点发来的RequestVote RPC时，需要做一些检查才能投出赞成票
// 1. 候选人最后一条Log条目的任期号大于本地最后一条Log条目的任期号；
// 2. 或者，候选人最后一条Log条目的任期号等于本地最后一条Log条目的任期号，且候选人的Log记录长度大于等于本地Log记录的长度
func (rf *Raft) isLogUpToDate(index int, term int) bool {
	lastIndex := rf.getLastIndex()
	lastTerm := rf.getLastTerm()
	return term > lastTerm || (term == lastTerm && index >= lastIndex)
}

func (rf *Raft) isMatchPrevLog(index int, term int) bool {
	return index >= len(rf.logs) || rf.logs[index].Term != term
}

// 获取最后的快照日志下标(代表已存储）
func (rf *Raft) getLastIndex() int {
	return len(rf.logs) - 1
}

func (rf *Raft) getPrevLogInfo(server int) (int, int) {
	prevLogIndex := rf.nextIndex[server] - 1
	prevLogTerm := rf.logs[prevLogIndex].Term
	return prevLogIndex, prevLogTerm
}

func (rf *Raft) genAppendEntries(server int) AppendEntriesArgs {
	prevLogIndex, prevLogTerm := rf.getPrevLogInfo(server)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]LogEntry, 0),
	}
	args.Entries = append(args.Entries, rf.logs[prevLogIndex+1:]...)
	return args
}

// 获取最后的任期
func (rf *Raft) getLastTerm() int {
	if len(rf.logs) == 0 {
		return 0
	} else {
		return rf.logs[len(rf.logs)-1].Term
	}
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// return currentTerm and whether this server believes it is the leader.
// 测试代码里面会调用raft.go中的getState方法，判断你当前的任期和是否是领导人
func (rf *Raft) GetState() (int, bool) {

	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.state == Leader
}
