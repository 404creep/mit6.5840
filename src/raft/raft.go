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
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// 所有的servers拥有的变量:
	currentTerm int        // 记录当前的任期
	votedFor    int        // 记录当前的任期把票投给了谁
	logs        []LogEntry //  first index is 1 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号

	// 所有的servers经常修改的:
	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 已知被 提交 的最大日志条目索引  (初始化为0，持续递增）
	lastApplied int // 已被状态机 执行 的最大日志条目索引

	// leader拥有的可见变量，用来管理他的follower(leader经常修改的）
	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标

	state          int         // 该节点是什么角色（状态）
	electionTimer  *time.Timer // 每个节点中的选举计时器
	heartbeatTimer *time.Timer // 每个节点中的心跳计时器

	applyChan chan ApplyMsg // 日志都是存在这里client取（2B）
	applyCond *sync.Cond    //在commit后唤醒 applyChecker goroutine的条件变量
}

// ------------------------------------------leader选举部分-------------------------------------
// example RequestVote RPC handler.
// rf 如何根据接收到的RequestVoteArgs， 返回一个 RequestVoteReply

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm  If votedFor is null or candidateId
	// 过期的请求 或 已经投给别人 返回false
	if args.Term < rf.currentTerm || (args.Term == rf.currentTerm &&
		rf.votedFor != -1 && rf.votedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	// candidate’s log is at least as up-to-date as receiver’s log, grant vote
	// 日志conflict 返回 false
	if !rf.isLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		reply.Term, reply.VoteGranted = rf.currentTerm, false
		return
	}

	//新的任期请求会 更新给rf 任期 和 状态
	if args.Term > rf.currentTerm {
		rf.state = Follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	// 成功选举,返回投票，重置选举时间
	rf.votedFor = args.CandidateId
	rf.electionTimer.Reset(ElectionTimeout())
	reply.Term, reply.VoteGranted = rf.currentTerm, true
}

// currentTerm自增  –> 给自己投票 –> 向其他服务器发起RequestVote RPC -> 成功当选则广播心跳/返回失败重置为follower
func (rf *Raft) StartElection() {

	rf.currentTerm += 1
	rf.state = Candidate

	rf.votedFor = rf.me
	grantedVoteNum := 1

	args := RequestVoteArgs{
		rf.currentTerm,
		rf.me,
		rf.getLastIndex(),
		rf.getLastTerm(),
	}

	// 开启协程对各个节点发起选举
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(server int) {
			reply := RequestVoteReply{}
			if rf.sendRequestVote(server, &args, &reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm == args.Term && rf.state == Candidate {
					if reply.VoteGranted {
						grantedVoteNum += 1
						if grantedVoteNum > len(rf.peers)/2 {
							fmt.Printf("{Node %v}  receives majority votes in term %v\n", rf.me, rf.currentTerm)
							rf.state = Leader
							rf.BroadcastHeartbeat(true)
						}
					} else if reply.Term > rf.currentTerm {
						//返回者的任期大于args（网络分区原因) 重置rf状态
						//fmt.Printf("{Node %v} finds a new leader {Node %v} with term %v "+
						//	"and steps down in term %v\n", rf.me, i, reply.Term, rf.currentTerm)
						rf.reset(reply.Term)
						rf.persist()
					}
				}
			}
		}(i)
	}
}

// ticker 协程会定期收到两个 timer 的到期事件，如果是 election timer 到期，则发起一轮选举
// 如果是 heartbeat timer 到期且节点是 leader，则发起一轮心跳。
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader { //follower 未收到心跳则定时发起选举
				rf.StartElection()
				rf.electionTimer.Reset(ElectionTimeout())
			}
			rf.mu.Unlock()
		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader { // leader 定时广播
				rf.BroadcastHeartbeat(true)
				rf.heartbeatTimer.Reset(HeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
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

//---------------------------------------------日志增量部分--------------------------------------------------

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// AppendEntries RPC Receiver implementation
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < currentTerm (§5.1)
	// rf 收到的args的 日志任期过期， 返回失败
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// other server has higher term （rf过期了）
	if args.Term > rf.currentTerm {
		rf.currentTerm, rf.votedFor = args.Term, -1
		rf.state = Follower
	}

	// 合法的leader AppendEntries(心跳) 重置选举时间
	rf.electionTimer.Reset(ElectionTimeout())
	if args.Entries == nil {
		return
	}

	// Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm
	//1. 如果preLogIndex的大于当前日志的最大的下标说明跟随者缺失日志，拒绝附加日志
	//2. 如果preLog出`的任期和preLogIndex处的任期和preLogTerm不相等，那么说明日志存在conflict,拒绝附加日志
	if !rf.isMatchPrevLog(args.PrevLogIndex, args.PrevLogTerm) {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// delete conflicting entries and append new entries
	// 删除冲突日志，添加新日志
	rf.logs = append(rf.logs[:args.PrevLogIndex+1], args.Entries...)
	reply.Term = rf.currentTerm
	reply.Success = true

	// set commitIndex = min(leaderCommit, index of last **new** entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastIndex())
	}

}

func (rf *Raft) LeaderAppendChecker() {
	// 循环检测server是否需要appendEntries
	for rf.killed() == false && rf.state == Leader {

		for peer := range rf.peers {
			if peer == rf.me {
				continue
			}
			go func(server int) {
				rf.mu.Lock()
				args := rf.genAppendEntries(server) //生成对server的append请求
				lastLogIndex := rf.getLastIndex()
				rf.mu.Unlock()

				if rf.getLastIndex() >= rf.nextIndex[server] {
					fmt.Printf("[term %d]: Raft[%d] send real appendEntries to Raft[%d]", rf.currentTerm, rf.me, server)
					// 根据 rf.nextIndex[server] 生成 requestAppendEntries
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)

					// handle reply
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.reset(reply.Term)
					}
					// If successful: update nextIndex and matchIndex for follower (§5.3)
					if reply.Success {
						rf.nextIndex[server] = lastLogIndex + 1
						rf.matchIndex[server] = lastLogIndex
					} else {
						//if AppendEntries fails because of log inconsistency: decrement nextIndex and retry(§5.3)
						rf.nextIndex[server] = int(math.Max(1.0, float64(rf.nextIndex[server]-1)))
					}
					rf.mu.Unlock()
				}
			}(peer)
		}
		// If there exists an N such that N > commitIndex, a majority of matchIndex[i] ≥ N,
		// and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4)
		// 更新leader的commitIndex,唤醒applyCond
		consensus := 1
		rf.mu.Lock()
		if rf.commitIndex < rf.getLastIndex() {
			for N := rf.getLastIndex(); N > rf.commitIndex; N-- {
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					if rf.matchIndex[i] >= N {
						consensus++
					}
				}
				if consensus > len(rf.peers)/2 && rf.logs[N].Term == rf.currentTerm {
					rf.commitIndex = N
					rf.applyCond.Broadcast()
					break
				}
			}
		}
	}
}

func (rf *Raft) applyChecker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		rf.lastApplied++
		//fmt.Printf("[term %d]:Raft [%d] [state %d] apply log entry %d to the service",
		//	rf.currentTerm, rf.me, rf.state, rf.lastApplied)
		newApplyIndex := rf.lastApplied
		command := rf.logs[newApplyIndex].Command
		rf.mu.Unlock()
		rf.applyChan <- ApplyMsg{
			CommandValid:  true,
			Command:       command,
			CommandIndex:  newApplyIndex,
			SnapshotValid: false,
		}
		//fmt.Printf("[term %d]:Raft [%d] [state %d] apply log entry %d " +
		//	"to the service successfully", rf.currentTerm, rf.me, rf.state, rf.lastApplied)
	}
}

//----------------------------------------------------日志压缩(快照）部分--------------------------------

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
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
	// Your initialization code here (2A, 2B, 2C).
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		dead:        0,
		applyChan:   applyCh,
		state:       Follower,
		currentTerm: 0,
		votedFor:    -1,
		logs:        make([]LogEntry, 1),

		nextIndex:  make([]int, len(peers)),
		matchIndex: make([]int, len(peers)),

		commitIndex: 0,
		lastApplied: 0,

		heartbeatTimer: time.NewTimer(HeartbeatTimeout()),
		electionTimer:  time.NewTimer(ElectionTimeout()),
	}
	rf.applyCond = sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	for i := 0; i < len(peers); i++ {
		rf.matchIndex[i], rf.nextIndex[i] = 0, 1
	}
	// start ticker goroutine to start elections
	go rf.ticker()

	// append entry to local log
	go rf.LeaderAppendChecker()

	// put the committed entry to apply on the status machine
	go rf.applyChecker()
	return rf
}

func (rf *Raft) BroadcastHeartbeat(isHeartBeat bool) {
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: len(rf.logs) - 1,
		PrevLogTerm:  rf.logs[len(rf.logs)-1].Term,
		Entries:      []LogEntry{}, // 空的 Entries 切片
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}
	for peer := range rf.peers {
		if peer == rf.me {
			continue
		}
		if isHeartBeat {
			// Leaders send periodic heartbeats (AppendEntries RPCs that carry no log entries)  (§5.2)
			go rf.sendAppendEntries(peer, &args, &reply)
		}
	}
}

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
// 调用raft中的start函数，对leader节点写入log (然后检测log是否成功其实就是通过applyChan协程一直检测)
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.killed() {
		return index, term, false
	}

	fmt.Printf("[term %d]: Raft [%d] start consensus\n", rf.currentTerm, rf.me)
	index = len(rf.logs)
	term = rf.currentTerm
	rf.logs = append(rf.logs, LogEntry{rf.currentTerm, command})
	return index, term, isLeader
}
