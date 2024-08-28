package raft

type Message struct {
	Term int
	Msg  interface{}
}

type empty struct{}

type requestVoteRequest struct {
	Term         int
	CandidateId  int
	LastLogIndex int // candidate最新的日志条目索引
	LastLogTerm  int
}

type requestVoteReply struct {
	ReqTerm     int
	Term        int  // candidate的term可能是过时的，此时收到的Term用于更新他自己
	VoteGranted bool // 是否投票给了该candidate
}

// 心跳: AppendEntries RPCs that carry no log entries is heartbeat
type appendEntriesRequest struct {
	Term         int        // leader的任期
	LeaderId     int        // leader自身的ID
	PrevLogIndex int        // 前一个日志的索引 (用于匹配日志位置是否是合适的，初始化rf.nextIndex[i] - 1)
	PrevLogTerm  int        // 前一个日志所属的任期 (用于匹配日志的任期是否是合适的是，是否有冲突)
	Entries      []LogEntry // 将要存储的日志条目列表 (为空时代表heartbeat, 有时候为了效率会发送超过一条)
	LeaderCommit int        // Leader已提交的日志条目索引 (最后一个被大多数机器都复制的日志Index)
}

type appendEntriesReply struct {
	Id              int
	ReqTerm         int
	ReqPrevLogIndex int //方便日志排查，请求logIndex也返回
	ReqLogLen       int //appendEntries的长度，方便更新nextIndex、matchIndex
	Term            int
	Success         bool

	// 快速回退
	ConflictTerm     int //follower中与Leader的preLog冲突的Log对应的任期号
	ConflictIndex    int //follower中任期号为conflictTerm的第一条LogIndex
	NextSendLogIndex int //follower中的最后一条日志Index+1
}

type installSnapshotRequest struct {
	Term              int    // 发送请求方的任期
	LeaderId          int    // 请求方的LeaderId
	LastIncludedIndex int    // 快照最后applied的日志下标
	LastIncludedTerm  int    // 快照最后applied时的当前任期
	Snapshot          []byte // 快照区块的原始字节流数据
}

type installSnapshotReply struct {
	ReqTerm              int
	ReqLastIncludedIndex int
	ReqLastIncludedTerm  int
	Id                   int
	Term                 int
	Success              bool
}

// 外部调用, 会进入这里来
func (rf *Raft) RequestVoteReq(args *requestVoteRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) RequestVoteReply(args *requestVoteReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) AppendEntries(args *appendEntriesRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) AppendEntriesReply(args *appendEntriesReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) InstallSnapshot(args *installSnapshotRequest, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) InstallSnapshotReply(args *installSnapshotReply, reply *empty) {
	rf.messagePipeLine <- Message{
		Term: args.Term,
		Msg:  args,
	}
}

func (rf *Raft) sendRequestVoteRequest(server int, args *requestVoteRequest) {
	rf.peers[server].Call("Raft.RequestVoteReq", args, &empty{})
}

func (rf *Raft) sendRequestVoteReply(server int, args *requestVoteReply) {
	rf.peers[server].Call("Raft.RequestVoteReply", args, &empty{})
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *appendEntriesRequest) {
	rf.peers[server].Call("Raft.AppendEntries", args, &empty{})
}

func (rf *Raft) sendAppendEntriesReply(server int, args *appendEntriesReply) {
	rf.peers[server].Call("Raft.AppendEntriesReply", args, &empty{})
}

func (rf *Raft) sendInstallSnapshotRequest(server int, args *installSnapshotRequest) {
	rf.peers[server].Call("Raft.InstallSnapshot", args, &empty{})
}

func (rf *Raft) sendInstallSnapshotReply(server int, args *installSnapshotReply) {
	rf.peers[server].Call("Raft.InstallSnapshotReply", args, &empty{})
}

// 向所有节点发送投票请求,发起投票请求时候比较的应该是LogIndex，而不是log长度
func (rf *Raft) sendVoteRequestToPeers() {
	// 外部会共享这个变量, 为了并发安全拷贝一份
	lastLogEntry := rf.LastLogEntry()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(i, term, LastLogIdx, LastLogTerm int) {
			rf.sendRequestVoteRequest(i, &requestVoteRequest{
				term,
				rf.me,
				LastLogIdx,
				LastLogTerm,
			})
		}(i, rf.status.CurrentTerm, lastLogEntry.LogIndex, lastLogEntry.Term)
	}
}

func (rf *Raft) sendInstallSnapshotReplyToLeader(msg *installSnapshotRequest, term int, success bool) {
	rf.sendInstallSnapshotReply(msg.LeaderId, &installSnapshotReply{
		ReqTerm:              msg.Term,
		Term:                 term,
		Id:                   rf.me,
		ReqLastIncludedIndex: msg.LastIncludedIndex,
		ReqLastIncludedTerm:  msg.LastIncludedTerm,
		Success:              success,
	})
}

func (rf *Raft) sendRequestVoteReplyToCandidate(voteGranted bool, term int, msg *requestVoteRequest) {
	rf.sendRequestVoteReply(msg.CandidateId, &requestVoteReply{
		msg.Term,
		term,
		voteGranted,
	})
}

func (rf *Raft) sendAppendEntriesReplyToLeader(success bool, term int, msg *appendEntriesRequest) {
	rf.sendAppendEntriesReply(msg.LeaderId, &appendEntriesReply{
		Id:              rf.me,
		ReqTerm:         msg.Term,
		ReqPrevLogIndex: msg.PrevLogIndex,
		ReqLogLen:       len(msg.Entries),
		Term:            term,
		Success:         success,
	})
}

func (rf *Raft) sendAppendEntriesRequestToFollower(server, term int, preLog LogEntry, entries []LogEntry, leaderCommit int) {
	rf.sendAppendEntriesRequest(server, &appendEntriesRequest{
		term,
		rf.me,
		preLog.LogIndex,
		preLog.Term,
		entries,
		leaderCommit,
	})
}
