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
	"6.5840/labgob"
	"bytes"
	"log"
	"sort"
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

	status Status           // 服务器的状态
	timer  <-chan time.Time // 用于接收计时器的信号

	// 外部消息, 进入总线
	messagePipeLine chan Message

	// start 传来的command
	commandChan  chan CommandInfo
	snapShotChan chan SnapShotInfo

	applyChan  chan ApplyMsg   // 	日志都是存在这里client取（2B），但是无缓冲
	applyQueue *UnboundedQueue // 防止applyMsg阻塞的缓冲队列
}

type Status struct {
	// 该节点是什么角色（状态）
	state State

	// 正常情况下commitIndex与lastApplied应该是一样的，但是如果有一个新的提交，并且还未应用的话last应该要更小些
	commitIndex int // 已知被 提交 的最大日志条目索引  (初始化为0，持续递增）
	lastApplied int // 已被状态机 执行 的最大日志条目索引

	CandidateInfo
	LeaderInfo
	PersistInfo
}

type PersistInfo struct {
	// 所有的servers拥有的变量:
	CurrentTerm       int        // 记录当前的任期
	VotedFor          int        // 记录当前的任期把票投给了谁
	Logs              []LogEntry //  first index is 1 日志条目数组，包含了状态机要执行的指令集，以及收到领导时的任期号
	LastIncludedIndex int        //该索引以及之前的所有条目都已经被快照覆盖
	LastIncludedTerm  int
}
type CandidateInfo struct {
	receiveVoteNum int // 收到的选票数
}

type LeaderInfo struct {
	// leader拥有的可见变量，用来管理他的follower(leader经常修改的）
	// nextIndex与matchIndex初始化长度应该为len(peers)，Leader对于每个Follower都记录他的nextIndex和matchIndex
	// nextIndex指的是下一个的appendEntries要从哪里开始
	// matchIndex指的是已知的某follower的log与leader的log最大匹配到第几个Index,已经apply
	nextIndex  []int // 对于每一个server，需要发送给他下一个日志条目的索引值（初始化为leader日志index+1,那么范围就对标len）
	matchIndex []int // 对于每一个server，已经复制给该server的最后日志条目下标
}

func (rf *Raft) Step() {
	rf.electionTimer()
	for {
		select {
		case <-rf.timer:
			rf.handleTimeOut()
		default:
			select {
			case <-rf.timer:
				rf.handleTimeOut()
			// start 传来的command
			case commandInfo := <-rf.commandChan:
				if rf.killed() {
					rf.debug("dead")
					go func() {
						for {
							respChan := commandInfo.RespChan
							respChan <- CommandRespInfo{Term: -1, Index: -1, IsLeader: false}
						}
					}()
					return
				}
				switch rf.status.state {
				case Candidate, Follower:
					go func(term int) {
						respChan := commandInfo.RespChan
						respChan <- CommandRespInfo{Term: term, Index: -1, IsLeader: false}
					}(rf.status.CurrentTerm)
				case Leader:
					rf.status.Logs = append(rf.status.Logs, LogEntry{
						rf.status.CurrentTerm,
						rf.getLastLogIndex() + 1, // todo 这里的logIndex应该是递增的
						commandInfo.Command,
					})
					rf.persist(nil)
					go func(term, index int) {
						respChan := commandInfo.RespChan
						respChan <- CommandRespInfo{
							Term:     term,
							Index:    index,
							IsLeader: true,
						}
					}(rf.status.CurrentTerm, rf.getLastLogIndex())
					// 为了尽快同步日志并返回客户端, 需要让定时器尽快过期
					rf.timerTimeOut()
				}
			case intput := <-rf.messagePipeLine:
				if rf.status.CurrentTerm < intput.Term {
					rf.status.CurrentTerm = intput.Term
					rf.status.state = Follower
					rf.status.VotedFor = -1
					rf.persist(nil)
				}
				switch msg := intput.Msg.(type) {
				case *requestVoteRequest:
					// 此处一定rf.term >= msg.term
					rf.debug("received RequestVoteRequest from %v, remote lastLog (index=%v, term=%v), remote term=%v",
						msg.CandidateId, msg.LastLogIndex, msg.LastLogTerm, msg.Term)
					if rf.status.state == Follower && rf.status.VotedFor == -1 &&
						rf.status.CurrentTerm == msg.Term && rf.isLogUpToDate(msg.LastLogTerm, msg.LastLogIndex) {
						rf.status.VotedFor = msg.CandidateId
						rf.persist(nil)
						rf.debug("vote for %v,reqTerm=%v,logs=%v", msg.CandidateId, msg.Term, rf.status.Logs)
						go func(term int) {
							rf.sendRequestVoteReply(msg.CandidateId, &requestVoteReply{
								msg.Term,
								term,
								true,
							})
						}(rf.status.CurrentTerm)
						rf.electionTimer()
						// you should only restart your election timer if
						// a)you get an AppendEntries RPC from the current leader (i.e., if the term in the AppendEntries arguments is outdated, you should not reset your timer);
						// b) you are starting an election; or
						// c) you grant a vote to another peer.
					} else {
						// 如果是rf仍然是candidate or leader, 说明rf.term >= msg.term, 那么拒绝投票
						// 或者是follower 且 (==mag.term(已经投过票了||日志不够新)  || rf.term > msg.term)
						rf.debug("reject vote,reqTerm=%v,logs=%v,voteFor=%v", msg.Term, rf.status.Logs, rf.status.VotedFor)
						go func(term int) {
							rf.sendRequestVoteReply(msg.CandidateId, &requestVoteReply{
								msg.Term,
								term,
								false,
							})
						}(rf.status.CurrentTerm)
					}
				case *requestVoteReply:
					// 1.如果是过去的消息, 直接无视
					// 2.自己已经不是candidate了, 无视
					if msg.ReqTerm != rf.status.CurrentTerm || rf.status.state != Candidate {
						break
					}
					if msg.VoteGranted {
						rf.status.receiveVoteNum++
						if rf.status.receiveVoteNum > len(rf.peers)/2 {
							rf.status.state = Leader
							/*
								(Reinitialized after election)
								nextIndex[]: for each server, index of the next log entry
								to send to that server (initialized to leader last log index + 1)
								Leader尝试发送最新的日志 方便快速同步
								matchIndex[]: for each server, index of highest log entry
								known to be replicated on server(initialized to 0, increases monotonically)
								Leader 认为该 Follower 还没有复制任何日志条目
							*/
							nextIndex := rf.getLastLogIndex() + 1
							for i := 0; i < len(rf.peers); i++ {
								rf.status.nextIndex[i] = nextIndex
								rf.status.matchIndex[i] = 0
							}
							rf.debug("be leader, logs=%v, nextIndex=%v, matchIndex=%v",
								rf.status.Logs, rf.status.nextIndex, rf.status.matchIndex)
							// leader 直接超时，发送心跳
							rf.timerTimeOut()
						}
					}
				case *installSnapshotRequest:
					// 进入case时候，rf.term >= msg.term
					if msg.Term < rf.status.CurrentTerm {
						go func(term int) {
							rf.sendInstallSnapshotReply(msg.LeaderId, &installSnapshotReply{
								ReqTerm: msg.Term,
								Term:    term,
								Id:      rf.me,
							})
						}(rf.status.CurrentTerm)
						break
					}
					// todo 为什么要持久化rf.term
					if rf.status.state == Candidate {
						rf.status.state = Follower
					}
					rf.electionTimer()
					/*
						If existing log entry has same index and term as snapshot’s
						last included entry, retain log entries following it and reply
					*/
					entry, ok := rf.getLogByIndex(msg.LastIncludedIndex)
					// 如果该follower没有包含快照，或者包含了但是term不匹配，删除所有的日志，只保留快照
					if !(ok && entry.Term == msg.LastIncludedTerm) &&
						rf.status.commitIndex < msg.LastIncludedIndex {
						newLogs := []LogEntry{{
							Term:     msg.LastIncludedTerm,
							LogIndex: msg.LastIncludedIndex,
							Command:  nil,
						}}
						rf.status.Logs = newLogs
						rf.status.LastIncludedIndex = msg.LastIncludedIndex
						rf.status.LastIncludedTerm = msg.LastIncludedTerm
						rf.status.commitIndex = msg.LastIncludedIndex
						rf.persist(msg.Snapshot)
						rf.applyChan <- ApplyMsg{
							CommandValid:  false,
							SnapshotValid: true,
							Snapshot:      msg.Snapshot,
							SnapshotTerm:  msg.LastIncludedTerm,
							SnapshotIndex: msg.LastIncludedIndex,
						}
					}
					// 如果已经包含了该快照，直接返回
					go func(term int) {
						rf.sendInstallSnapshotReply(msg.LeaderId, &installSnapshotReply{
							ReqTerm:              msg.Term,
							Term:                 term,
							Id:                   rf.me,
							ReqLastIncludedIndex: msg.LastIncludedIndex,
							ReqLastIncludedTerm:  msg.LastIncludedTerm,
						})
					}(rf.status.CurrentTerm)
				case *installSnapshotReply:
					if rf.status.state != Leader || msg.Term < rf.status.CurrentTerm ||
						msg.ReqTerm != rf.status.CurrentTerm {
						break
					}
					//尝试更新该 Follower 的 NextLogIndex 和 MatchIndex
					rf.status.nextIndex[msg.Id] = max(rf.status.nextIndex[msg.Id], msg.ReqLastIncludedIndex+1)
					rf.status.matchIndex[msg.Id] = rf.status.nextIndex[msg.Id] - 1
					// 更新commitIndex
					rf.updateCommitIndex()
				case *appendEntriesRequest:
					// 进入case时候，rf.term >= msg.term
					// 拒绝过期leader日志
					if msg.Term < rf.status.CurrentTerm {
						go func(term int) {
							rf.sendAppendEntriesReply(msg.LeaderId, &appendEntriesReply{
								ReqTerm: msg.Term,
								Term:    term,
								Success: false,
							})
						}(rf.status.CurrentTerm)
						break
					}
					// rf.term == msg.term
					// fixme 重置并持久化状态
					if rf.status.state == Candidate {
						rf.status.state = Follower
					}
					rf.electionTimer()
					// 快速回退，不断回退到leader发来的preLogIndex的term
					// todo 没有该preLog， conflictIndex = len(rf.Logs),conflictTerm=-1
					if msg.PrevLogIndex > rf.getLastLogIndex() {
						go func(term, conflictIndex int) {
							rf.sendAppendEntriesReply(msg.LeaderId, &appendEntriesReply{
								ReqTerm:       msg.Term,
								Term:          term,
								Success:       false,
								ConflictIndex: conflictIndex,
								ConflictTerm:  -1,
							})
						}(rf.status.CurrentTerm, rf.getLastLogIndex()+1)
						break
					}
					// 有该preLog，但是term不匹配
					if rf.status.Logs[msg.PrevLogIndex].Term != msg.PrevLogTerm {

					}
				case *appendEntriesReply:
					if rf.status.state != Leader || msg.Term < rf.status.CurrentTerm ||
						msg.ReqTerm != rf.status.CurrentTerm {
						break
					}

				}

			}
		}
	}
}

func (rf *Raft) handleTimeOut() {
	switch rf.status.state {
	// 如果是follower超时, 那么进入candidate状态
	case Follower:
		rf.debug("timeout,being candidate,logs=%v", rf.status.Logs)
		rf.status.state = Candidate
		rf.status.receiveVoteNum = 1
		rf.status.VotedFor = rf.me
		rf.status.CurrentTerm++
		rf.persist(nil)

		rf.electionTimer()
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
			}(i, rf.status.CurrentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())

		}
	// 如果是candidate超时, 那么term+1, 区分上一选举周期，重新开始选举
	case Candidate:
		rf.debug("timeout,retrying,logs=%v", rf.status.Logs)
		rf.status.CurrentTerm++
		rf.status.receiveVoteNum = 1
		rf.persist(nil)
		rf.electionTimer()
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
			}(i, rf.status.CurrentTerm, rf.getLastLogIndex(), rf.getLastLogTerm())

		}
	// 如果是leader超时, 那么发送心跳
	case Leader:
		rf.debug("timeout,send heartbeat,logs=%v", rf.status.Logs)
		for i := range rf.peers {
			if i != rf.me {
				rf.leaderSendLogs(i)
			}
		}
		rf.resetLeaderTimer() // leader定期发送心跳
	}
}

//---------------------------------------------日志增量部分--------------------------------------------------

func (rf *Raft) updateCommitIndex() {
	sortedMatchIndex := append([]int(nil), rf.status.matchIndex...) // 简化复制操作
	sortedMatchIndex[rf.me] = len(rf.status.Logs) - 1
	sort.Ints(sortedMatchIndex)

	// 计算出中位数的索引N
	N := sortedMatchIndex[len(sortedMatchIndex)/2]

	//If there exists an N such that N > commitIndex, a majority
	//of matchIndex[i] ≥ N, and log[N].term == CurrentTerm:set commitIndex = N
	// todo why 确保新CommitIndex比当前的大，且日志条目属于当前任期
	if N > rf.status.commitIndex && rf.status.Logs[N].Term == rf.status.CurrentTerm {
		rf.applyCommittedLogs(N)
	}
}

// applyCommittedLogs 负责将从oldCommitIndex到新的CommitIndex之间的日志条目应用到状态机
func (rf *Raft) applyCommittedLogs(newCommitIndex int) {
	oldCommitIndex := rf.status.commitIndex
	rf.status.commitIndex = newCommitIndex

	rf.debug("commit log, matchIdx=%v N=%v oldCommitIndex=%v, now is %v",
		rf.status.matchIndex, newCommitIndex, oldCommitIndex, rf.status.commitIndex)
	// todo 这里的commitIndex是log的还是数组下标的
	for i := oldCommitIndex + 1; i <= newCommitIndex; i++ {
		msg := ApplyMsg{
			CommandValid: true,
			Command:      rf.status.Logs[i].Command,
			CommandIndex: i,
		}
		rf.applyQueue.Enqueue(msg)
	}
}

// todo
func (rf *Raft) leaderSendLogs(server int) {
	if rf.status.nextIndex[server] <= rf.status.LastIncludedIndex {
		// 如果nextIndex小于等于lastIncludedIndex，说明需要发送快照
		snapshot := rf.persister.ReadSnapshot()
		go func(term, leaderId, lastIncludedIndex, lastIncludedTerm int, snapshot []byte) {
			rf.sendInstallSnapshotRequest(server, &installSnapshotRequest{
				Term:              term,
				LeaderId:          leaderId,
				LastIncludedIndex: lastIncludedIndex,
				LastIncludedTerm:  lastIncludedTerm,
				Snapshot:          snapshot,
			})
		}(rf.status.CurrentTerm, rf.me, rf.status.LastIncludedIndex, rf.status.LastIncludedTerm, snapshot)
	} else {
		// 否则发送日志
		// 切片引用防止函数中被并发修改，先复制
		logsCopy := make([]LogEntry, len(rf.status.Logs))
		copy(logsCopy, rf.status.Logs)
		nextIndexCopy := make([]int, len(rf.status.nextIndex))
		copy(nextIndexCopy, rf.status.nextIndex)
		lastLogIndex := rf.getLastLogIndex()
		//fixme log 数组越界
		go func(server, term, commitIndex, lastLogIndex int, logs []LogEntry, nextIndex []int) {
			/*
				If last log index ≥ nextIndex for a follower: send
				AppendEntries RPC with log entries starting at nextIndex
			*/
			if lastLogIndex >= nextIndex[server] {
				rf.debug("sendAppendEntries to %v,lastLogIndex=%v,nextIndex=%v,lenOfLog=%v",
					server, lastLogIndex, nextIndex[server], len(logs))
				rf.sendAppendEntriesRequest(server, &appendEntriesRequest{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex[server] - 1,
					PrevLogTerm:  logs[nextIndex[server]-1].Term,
					Entries:      logs[nextIndex[server]:],
					LeaderCommit: commitIndex,
				})
			} else {
				// If there are no new entries to send: send
				// AppendEntries RPC with log entries starting at nextIndex - 1
				// 发送心跳
				rf.debug("send heartbeat to %v,lastLogIndex=%v,nextIndex=%v,lenOfLog=%v",
					server, lastLogIndex, nextIndex[server], len(logs))
				rf.sendAppendEntriesRequest(server, &appendEntriesRequest{
					Term:         term,
					LeaderId:     rf.me,
					PrevLogIndex: nextIndex[server] - 1,
					PrevLogTerm:  logs[nextIndex[server]-1].Term,
					Entries:      nil,
					LeaderCommit: commitIndex,
				})
			}
		}(server, rf.status.CurrentTerm, rf.status.commitIndex, lastLogIndex, logsCopy, nextIndexCopy)
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
func (rf *Raft) persist(snapShot []byte) {
	// Your code here (2C).
	// Example:
	w := new(bytes.Buffer)
	//创建一个 Gob 编码器，将编码结果写入 w
	e := labgob.NewEncoder(w)
	err := e.Encode(&rf.status.PersistInfo)
	if err != nil {
		panic(err)
	}
	raftstate := w.Bytes()
	if snapShot == nil {
		snapShot = rf.persister.ReadSnapshot()
	}
	rf.persister.Save(raftstate, snapShot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		rf.status.PersistInfo.Logs = make([]LogEntry, 1)
		rf.status.PersistInfo.CurrentTerm = 0
		rf.status.PersistInfo.VotedFor = -1
		return
	}
	//创建读取器r 和 解码器d
	r := bytes.NewReader(data)
	d := labgob.NewDecoder(r)
	if err := d.Decode(&rf.status.PersistInfo); err != nil {
		panic(err)
	}
}

func (rf *Raft) ProcessApplyQueue() {
	for {
		all := rf.applyQueue.DequeueAll()
		for _, v := range all {
			if Debug {
				log.Printf("[async thread] %v send to applyCh %v\n", rf.me, v)
			}
			msg := v.(ApplyMsg)
			if msg.SnapshotValid {
				if Debug {
					log.Printf("applyCh got snapshot: %s\n", string(msg.Snapshot))
				}
			}
			rf.applyChan <- msg
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	respChan := make(chan struct{})
	rf.snapShotChan <- SnapShotInfo{
		Index:    index,
		SnapShot: snapshot,
		RespChan: respChan,
	}
	<-respChan
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
	rf := &Raft{
		peers:      peers,
		persister:  persister,
		me:         me,
		dead:       0,
		applyChan:  applyCh,
		applyQueue: NewUnboundedQueue(),
		status: Status{
			state:       Follower,
			commitIndex: 0,
			lastApplied: 0,
			LeaderInfo: LeaderInfo{
				nextIndex:  make([]int, len(peers)),
				matchIndex: make([]int, len(peers)),
			},
			CandidateInfo: CandidateInfo{
				receiveVoteNum: 0,
			},
			PersistInfo: PersistInfo{
				CurrentTerm:       0,
				VotedFor:          -1,
				Logs:              []LogEntry{{0, 0, nil}},
				LastIncludedIndex: 0,
				LastIncludedTerm:  0,
			},
		},
		messagePipeLine: make(chan Message),
		commandChan:     make(chan CommandInfo),
	}
	// (first index is 1)
	for i := range rf.status.nextIndex {
		rf.status.nextIndex[i] = 1
	}
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.Step()
	// 处理 applyQueue 中的所有 ApplyMsg 并发送到 applyCh
	go rf.ProcessApplyQueue()
	return rf
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
	respChan := make(chan CommandRespInfo)
	rf.commandChan <- CommandInfo{
		Command:  command,
		RespChan: respChan,
	}
	resp := <-respChan
	return resp.Index, resp.Term, resp.IsLeader
}
