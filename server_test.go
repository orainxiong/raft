package raft

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strconv"
	"sync"
	"testing"
	"time"
)

//--------------------------------------
// Request Vote
//--------------------------------------

// Ensure that we can request a vote from a server that has not voted.
func TestServerRequestVote(t *testing.T) {

	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)

	server := newTestServer("1", &testTransporter{})

	server.Start()
	logger.Printf("server state %#v", server)

	if _, err := server.Do(&DefaultJoinCommand{Name: server.Name()}); err != nil {
		t.Fatalf("Server %s unable to join: %v", server.Name(), err)
	}
	logger.Printf("server state %s", server.GetState())

	defer server.Stop()

	var (
		term          uint64 = 1
		candidateName string = "foo"
		lastLogIndex  uint64 = 1
		lastLogTerm   uint64 = 0
	)
	resp := server.RequestVote(newRequestVoteRequest(term, candidateName, lastLogIndex, lastLogTerm))
	if resp.Term != 1 || !resp.VoteGranted {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
}

// // Ensure that a vote request is denied if it comes from an old term.
func TestServerRequestVoteDeniedForStaleTerm(t *testing.T) {
	s := newTestServer("1", &testTransporter{})

	s.Start()
	if _, err := s.Do(&DefaultJoinCommand{Name: s.Name()}); err != nil {
		t.Fatalf("Server %s unable to join: %v", s.Name(), err)
	}

	s.(*server).mutex.Lock()
	s.(*server).currentTerm = 2
	s.(*server).mutex.Unlock()

	defer s.Stop()
	resp := s.RequestVote(newRequestVoteRequest(1, "foo", 1, 0))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Invalid request vote response: %v/%v", resp.Term, resp.VoteGranted)
	}
	if s.Term() != 2 && s.State() != Follower {
		t.Fatalf("Server did not update term and demote: %v / %v", s.Term(), s.State())
	}
}

// Ensure that a vote request is denied if we've already voted for a different candidate.
func TestServerRequestVoteDeniedIfAlreadyVoted(t *testing.T) {
	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	s := newTestServer("1", &testTransporter{})

	s.Start()
	if _, err := s.Do(&DefaultJoinCommand{Name: s.Name()}); err != nil {
		t.Fatalf("Server %s unable to join: %v", s.Name(), err)
	}

	s.(*server).mutex.Lock()
	s.(*server).currentTerm = 2
	s.(*server).mutex.Unlock()
	defer s.Stop()
	var (
		termA          uint64 = 2
		candidateNameA string = "foo"
		lastLogIndexA  uint64 = 2
		lastLogTermA   uint64 = 0

		termB          uint64 = 2
		candidateNameB string = "bar"
		lastLogIndexB  uint64 = 2
		lastLogTermB   uint64 = 0
	)
	resp := s.RequestVote(newRequestVoteRequest(termA, candidateNameA, lastLogIndexA, lastLogTermA))
	if resp.Term != 2 || !resp.VoteGranted {
		t.Fatalf("First vote should not have been denied")
	}
	resp = s.RequestVote(newRequestVoteRequest(termB, candidateNameB, lastLogIndexB, lastLogTermB))
	if resp.Term != 2 || resp.VoteGranted {
		t.Fatalf("Second vote should have been denied")
	}
}

// Ensure that a vote request is approved if vote occurs in a new term.
func TestServerRequestVoteApprovedIfAlreadyVotedInOlderTerm(t *testing.T) {
	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	s := newTestServer("1", &testTransporter{})

	s.Start()
	if _, err := s.Do(&DefaultJoinCommand{Name: s.Name()}); err != nil {
		t.Fatalf("Server %s unable to join: %v", s.Name(), err)
	}

	time.Sleep(time.Millisecond * 100)

	s.(*server).mutex.Lock()
	s.(*server).currentTerm = 2
	s.(*server).mutex.Unlock()
	defer s.Stop()
	resp := s.RequestVote(newRequestVoteRequest(2, "foo", 2, 1))
	if resp.Term != 2 || !resp.VoteGranted || s.VotedFor() != "foo" {
		t.Fatalf("First vote should not have been denied")
	}
	resp = s.RequestVote(newRequestVoteRequest(3, "bar", 2, 1))

	if resp.Term != 3 || !resp.VoteGranted || s.VotedFor() != "bar" {
		t.Fatalf("Second vote should have been approved")
	}
}

// Ensure that a vote request is denied if the log is out of date.
func TestServerRequestVoteDenyIfCandidateLogIsBehind(t *testing.T) {
	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	tmpLog := newLog()
	/*
		newLogEntry(log *Log, event *ev, index uint64, term uint64, command Command)
	*/
	e0, _ := newLogEntry(tmpLog, nil, 1, 1, &testCommand1{Val: "foo", I: 20})
	e1, _ := newLogEntry(tmpLog, nil, 2, 1, &testCommand2{X: 100})
	e2, _ := newLogEntry(tmpLog, nil, 3, 2, &testCommand1{Val: "bar", I: 0})
	s := newTestServerWithLog("1", &testTransporter{}, []*LogEntry{e0, e1, e2})

	// start as a follower with term 2 and index 3
	s.Start()
	defer s.Stop()

	logger.Println("---------------------------------------------------")
	logger.Printf("server s %s", s.GetState())
	// request vote from term 3 with last log entry 2, 2
	// (term uint64, candidateName string, lastLogIndex uint64, lastLogTerm uint64)
	// req term > server.term and will update server.term
	resp := s.RequestVote(newRequestVoteRequest(3, "foo", 2, 2))
	if resp.Term != 3 || resp.VoteGranted {
		t.Fatalf("Stale index vote should have been denied [%v/%v]", resp.Term, resp.VoteGranted)
	}
	logger.Printf("server s %s", s.GetState())
	logger.Println("---------------------------------------------------")
	logger.Printf("server s %s", s.GetState())
	// request vote from term 2 with last log entry 2, 3
	resp = s.RequestVote(newRequestVoteRequest(2, "foo", 3, 2))
	if resp.Term != 3 || resp.VoteGranted {
		t.Fatalf("Stale term vote should have been denied [%v/%v]", resp.Term, resp.VoteGranted)
	}
	logger.Printf("server s %s", s.GetState())
	logger.Println("---------------------------------------------------")
	logger.Printf("server s %s", s.GetState())
	// request vote from term 3 with last log entry 2, 3
	resp = s.RequestVote(newRequestVoteRequest(3, "foo", 3, 2))
	if resp.Term != 3 || !resp.VoteGranted {
		t.Fatalf("Matching log vote should have been granted")
	}
	logger.Printf("server s %s", s.GetState())
	logger.Println("---------------------------------------------------")
	logger.Printf("server s %s", s.GetState())
	// request vote from term 3 with last log entry 2, 4
	resp = s.RequestVote(newRequestVoteRequest(3, "foo", 4, 2))
	if resp.Term != 3 || !resp.VoteGranted {
		t.Fatalf("Ahead-of-log vote should have been granted")
	}
	logger.Printf("server s %s", s.GetState())
}

func TestProcessVoteResponse(t *testing.T) {
	// server Term: 0, status: Leader
	// response Term : 1, granted
	// Expectation: not success
	// Server Term 1 status:Leader
	server := &server{}
	server.eventDispatcher = newEventDispatcher(server)
	server.currentTerm = 0
	server.state = Leader
	response := &RequestVoteResponse{
		VoteGranted: true,
		Term:        1,
	}
	if success := server.processVoteResponse(response); success {
		t.Fatal("Process should fail if the resp's term is larger than server's")
	}
	if server.state != Follower {
		t.Fatal("Server should stepdown")
	}

	// server Term: 1, status: Follower
	// response Term: 2, granted
	// Expectation: not success
	response.Term = 2
	if success := server.processVoteResponse(response); success {
		t.Fatal("Process should fail if the resp's term is larger than server's")
	}
	if server.state != Follower {
		t.Fatal("Server should still be Follower")
	}

	server.currentTerm = 2
	// server Term: 2, status: Follower
	// response Term: 2
	// Expectation: success
	if success := server.processVoteResponse(response); !success {
		t.Fatal("Process should success if the server's term is larger than resp's")
	}

}

// //--------------------------------------
// // Promotion
// //--------------------------------------

// // Ensure that we can self-promote a server to candidate, obtain votes and become a fearless leader.
func TestServerPromoteSelf(t *testing.T) {
	e0, _ := newLogEntry(newLog(), nil, 1, 1, &testCommand1{Val: "foo", I: 20})
	s := newTestServerWithLog("1", &testTransporter{}, []*LogEntry{e0})

	// start as a follower
	s.Start()
	defer s.Stop()

	time.Sleep(2 * testElectionTimeout)

	if s.State() != Leader {
		t.Fatalf("Server self-promotion failed: %v", s.State())
	}
}

//Ensure that we can promote a server within a cluster to a leader.
func TestServerPromote(t *testing.T) {
	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	lookup := map[string]Server{}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return lookup[peer.Name].RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return lookup[peer.Name].AppendEntries(req)
	}
	servers := newTestCluster([]string{"1", "2", "3"}, transporter, lookup)

	time.Sleep(2 * testElectionTimeout)

	if servers[0].State() != Leader && servers[1].State() != Leader && servers[2].State() != Leader {
		t.Fatalf("No leader elected: (%s, %s, %s)", servers[0].State(), servers[1].State(), servers[2].State())
	}
	for _, s := range servers {
		s.Stop()
	}
}

func FindStrInStrArray(server_name string, servers []Server) bool {
	for _, server := range servers {
		if server_name == server.Name() {
			return true
		}
	}
	return false
}

func FindLeader(servers []Server) Server {
	var getLeader bool = false
	var index int
	getLeader = false
	index = -1
	for i, server := range servers {
		if server.State() == Leader {
			getLeader = true
			index = i
			break
		}
	}

	if !getLeader {
		logger.Printf("------------- No leader elected: %#v", servers)
		return nil
	} else {
		logger.Printf("------------- leader elected is %s %s", servers[index].Name(), servers[index].State())
		return servers[index]
	}
}

func ShowLeaderInfo(leader Server){
	logger.Println("show leader info ",leader.GetState())
	for _ , entry := range leader.LogEntries() {
		logger.Println(entry.pb.GetTerm(),entry.pb.GetIndex(),entry.pb.GetCommandName(),string(entry.pb.GetCommand()))
	}
}

// Test network partition
// 集群A(Leader),B,C,D,E
// 分裂成 A,B / C,D,E

// 场景 1 : C,D,E 集群选出新 leader, 同时接受新的 command
func TestNetworkPartition(t *testing.T) {
	SetLogLevel(1)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	lookup := map[string]Server{}
	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return lookup[peer.Name].RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return lookup[peer.Name].AppendEntries(req)
	}
	servers := newTestCluster([]string{"AA", "BB", "CC", "DD", "EE"}, transporter, lookup)

	time.Sleep(2 * testElectionTimeout)

	FindLeader(servers)

	logger.Println("------------------------- start network partition -----------------------------------")
	// 通过 transport 模拟 network partition
	var GroupA []Server = []Server{servers[0], servers[1]}
	var GroupB []Server = []Server{servers[2], servers[3], servers[4]}

	logger.Println("------------------------- simulate network partition -----------------------------------")

	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		if FindStrInStrArray(s.Name(), GroupA) && FindStrInStrArray(peer.Name, GroupA) {
			return lookup[peer.Name].RequestVote(req)
		} else if FindStrInStrArray(s.Name(), GroupB) && FindStrInStrArray(peer.Name, GroupB) {
			return lookup[peer.Name].RequestVote(req)
		} else {
			return nil
		}

	}

	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		if FindStrInStrArray(s.Name(), GroupA) && FindStrInStrArray(peer.Name, GroupA) {
			return lookup[peer.Name].AppendEntries(req)
		} else if FindStrInStrArray(s.Name(), GroupB) && FindStrInStrArray(peer.Name, GroupB) {
			return lookup[peer.Name].AppendEntries(req)
		} else {
			return nil
		}
	}

	logger.Println("------------------------- new term -----------------------------------")
	// 留出时间选主
	time.Sleep(2 * testElectionTimeout)

	var A_Leader, B_Leader Server
	// 老 leader 如果在集群A 中,那么 command 肯定是无法 commit 的,所以不能返回,通过 goroutine 运行
	if A_Leader = FindLeader(GroupA); A_Leader != nil {
		go A_Leader.Do(&testCommand1{Val: "foo", I: 30})
		go A_Leader.Do(&testCommand1{Val: "foo", I: 40})
	}

	// 新集群中一定会选出新leader
	if B_Leader = FindLeader(GroupB); B_Leader != nil {
		B_Leader.Do(&testCommand1{Val: "foo", I: 80})
		B_Leader.Do(&testCommand1{Val: "foo", I: 90})
	}

	time.Sleep(5 * testElectionTimeout)

	if A_Leader != nil {
		ShowLeaderInfo(A_Leader)
	}
	if B_Leader != nil {
		ShowLeaderInfo(B_Leader)
	}

	logger.Println("------------------------- recover network failure -----------------------------------")
	// 恢复网络
	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return lookup[peer.Name].RequestVote(req)
	}
	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return lookup[peer.Name].AppendEntries(req)
	}
	// 恢复后的集群选出新leader
	time.Sleep(2 * testElectionTimeout)

	// 显示集群信息,可以看到,新的 leader 一定在 C,D,E ,因为 commited log index 大一些.
	leader := FindLeader(servers)
	ShowLeaderInfo(leader)

	for _, s := range servers {
		s.Stop()
	}
}

//--------------------------------------
// Append Entries
//--------------------------------------

// Ensure we can append entries to a server.
func TestServerAppendEntries(t *testing.T) {
	s := newTestServer("1", &testTransporter{})

	s.SetHeartbeatInterval(time.Second * 10)
	s.Start()
	defer s.Stop()

	// Append single entry.
	e, _ := newLogEntry(nil, nil, 1, 1, &testCommand1{Val: "foo", I: 10})
	entries := []*LogEntry{e}
	resp := s.AppendEntries(newAppendEntriesRequest(1, 0, 0, 0, "ldr", entries))
	if resp.Term() != 1 || !resp.Success() {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := s.(*server).log.commitInfo(); index != 0 || term != 0 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Append multiple entries + commit the last one.
	e1, _ := newLogEntry(nil, nil, 2, 1, &testCommand1{Val: "bar", I: 20})
	e2, _ := newLogEntry(nil, nil, 3, 1, &testCommand1{Val: "baz", I: 30})
	entries = []*LogEntry{e1, e2}
	resp = s.AppendEntries(newAppendEntriesRequest(1, 1, 1, 1, "ldr", entries))
	if resp.Term() != 1 || !resp.Success() {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := s.(*server).log.commitInfo(); index != 1 || term != 1 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}

	// Send zero entries and commit everything.
	resp = s.AppendEntries(newAppendEntriesRequest(2, 3, 1, 3, "ldr", []*LogEntry{}))
	if resp.Term() != 2 || !resp.Success() {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := s.(*server).log.commitInfo(); index != 3 || term != 1 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}
}

//Ensure that entries with stale terms are rejected.
func TestServerAppendEntriesWithStaleTermsAreRejected(t *testing.T) {
	s := newTestServer("1", &testTransporter{})

	s.Start()

	defer s.Stop()
	s.(*server).mutex.Lock()
	s.(*server).currentTerm = 2
	s.(*server).mutex.Unlock()

	// Append single entry.
	e, _ := newLogEntry(nil, nil, 1, 1, &testCommand1{Val: "foo", I: 10})
	entries := []*LogEntry{e}
	resp := s.AppendEntries(newAppendEntriesRequest(1, 0, 0, 0, "ldr", entries))
	if resp.Term() != 2 || resp.Success() {
		t.Fatalf("AppendEntries should have failed: %v/%v", resp.Term, resp.Success)
	}
	if index, term := s.(*server).log.commitInfo(); index != 0 || term != 0 {
		t.Fatalf("Invalid commit info [IDX=%v, TERM=%v]", index, term)
	}
}

// Ensure that we reject entries if the commit log is different.
func TestServerAppendEntriesRejectedIfAlreadyCommitted(t *testing.T) {

	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	s := newTestServer("1", &testTransporter{})
	s.Start()
	defer s.Stop()

	// Append single entry + commit.
	e1, _ := newLogEntry(nil, nil, 1, 1, &testCommand1{Val: "foo", I: 10})
	e2, _ := newLogEntry(nil, nil, 2, 1, &testCommand1{Val: "foo", I: 15})
	entries := []*LogEntry{e1, e2}
	resp := s.AppendEntries(newAppendEntriesRequest(1, 0, 0, 2, "ldr", entries))
	if resp.Term() != 1 || !resp.Success() {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}

	// Append entry again (post-commit).
	e, _ := newLogEntry(nil, nil, 2, 1, &testCommand1{Val: "bar", I: 20})
	entries = []*LogEntry{e}
	resp = s.AppendEntries(newAppendEntriesRequest(1, 2, 1, 1, "ldr", entries))
	if resp.Term() != 1 || resp.Success() {
		t.Fatalf("AppendEntries should have failed: %v/%v", resp.Term, resp.Success)
	}
}

// Ensure that we uncommitted entries are rolled back if new entries overwrite them.
func TestServerAppendEntriesOverwritesUncommittedEntries(t *testing.T) {
	SetLogLevel(3)
	SetLogFlag(log.Ldate | log.Ltime | log.Lshortfile)
	s := newTestServer("1", &testTransporter{})
	s.Start()
	defer s.Stop()

	entry1, _ := newLogEntry(s.(*server).log, nil, 1, 1, &testCommand1{Val: "foo", I: 10})
	entry2, _ := newLogEntry(s.(*server).log, nil, 2, 1, &testCommand1{Val: "foo", I: 15})
	entry3, _ := newLogEntry(s.(*server).log, nil, 2, 2, &testCommand1{Val: "bar", I: 20})

	// Append single entry + commit.
	entries := []*LogEntry{entry1, entry2}
	resp := s.AppendEntries(newAppendEntriesRequest(1, 0, 0, 1, "ldr", entries))
	if resp.Term() != 1 || !resp.Success() || s.(*server).log.commitIndex != 1 {
		t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
	}

	for i, entry := range s.(*server).log.entries {
		if entry.Term() != entries[i].Term() || entry.Index() != entries[i].Index() || !bytes.Equal(entry.Command(), entries[i].Command()) {
			t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
		}
	}

	logger.Println("---------------------------------------")

	for index, entry := range s.LogEntries() {
		logger.Printf("index  %d , term %d, index %d,command %s, command_name %s \n", index, entry.pb.GetTerm(), entry.pb.GetIndex(), entry.pb.GetCommand(), entry.pb.GetCommandName())
	}

	// Append entry that overwrites the second (uncommitted) entry.
	entries = []*LogEntry{entry3}
	resp = s.AppendEntries(newAppendEntriesRequest(2, 1, 1, 2, "ldr", entries))
	if resp.Term() != 2 || !resp.Success() || s.(*server).log.commitIndex != 2 {
		t.Fatalf("AppendEntries should have succeeded: %v/%v", resp.Term, resp.Success)
	}
	logger.Println("---------------------------------------")
	entries = []*LogEntry{entry1, entry3}
	for i, entry := range s.(*server).log.entries {
		if entry.Term() != entries[i].Term() || entry.Index() != entries[i].Index() || !bytes.Equal(entry.Command(), entries[i].Command()) {
			t.Fatalf("AppendEntries failed: %v/%v", resp.Term, resp.Success)
		}
	}
}

//--------------------------------------
// Command Execution
//--------------------------------------

// Ensure that a follower cannot execute a command.
func TestServerDenyCommandExecutionWhenFollower(t *testing.T) {
	s := newTestServer("1", &testTransporter{})
	s.Start()
	defer s.Stop()
	var err error
	if _, err = s.Do(&testCommand1{Val: "foo", I: 10}); err != NotLeaderError {
		t.Fatalf("Expected error: %v, got: %v", NotLeaderError, err)
	}
}

//--------------------------------------
// Recovery
//--------------------------------------

// Ensure that a follower cannot execute a command.
func TestServerRecoverFromPreviousLogAndConf(t *testing.T) {
	// Initialize the servers.
	var mutex sync.RWMutex
	servers := map[string]Server{}

	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		mutex.RLock()
		target := servers[peer.Name]
		mutex.RUnlock()

		b, _ := json.Marshal(req)
		clonedReq := &RequestVoteRequest{}
		json.Unmarshal(b, clonedReq)

		return target.RequestVote(clonedReq)
	}
	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		mutex.RLock()
		target := servers[peer.Name]
		mutex.RUnlock()

		b, _ := json.Marshal(req)
		clonedReq := &AppendEntriesRequest{}
		json.Unmarshal(b, clonedReq)

		return target.AppendEntries(clonedReq)
	}

	disTransporter := &testTransporter{}
	disTransporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return nil
	}
	disTransporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return nil
	}

	var names []string
	var paths = make(map[string]string)

	n := 5

	// add n servers
	for i := 1; i <= n; i++ {
		names = append(names, strconv.Itoa(i))
	}

	var leader Server
	for _, name := range names {
		s := newTestServer(name, transporter)

		mutex.Lock()
		servers[name] = s
		mutex.Unlock()
		paths[name] = s.Path()

		if name == "1" {
			leader = s
			s.SetHeartbeatInterval(testHeartbeatInterval)
			s.Start()
			time.Sleep(testHeartbeatInterval)
		} else {
			s.SetElectionTimeout(testElectionTimeout)
			s.SetHeartbeatInterval(testHeartbeatInterval)
			s.Start()
			time.Sleep(testHeartbeatInterval)
		}
		if _, err := leader.Do(&DefaultJoinCommand{Name: name}); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}

	}

	// commit some commands
	for i := 0; i < 10; i++ {
		if _, err := leader.Do(&testCommand2{X: 1}); err != nil {
			t.Fatalf("cannot commit command: %s", err.Error())
		}
	}

	time.Sleep(2 * testHeartbeatInterval)

	for _, name := range names {
		s := servers[name]
		if s.CommitIndex() != 16 {
			t.Fatalf("%s commitIndex is invalid [%d/%d]", name, s.CommitIndex(), 16)
		}
		s.Stop()
	}

	for _, name := range names {
		// with old path and disable transportation
		s := newTestServerWithPath(name, disTransporter, paths[name])
		servers[name] = s

		s.Start()

		// should only commit to the last join command
		if s.CommitIndex() != 6 {
			t.Fatalf("%s recover phase 1 commitIndex is invalid [%d/%d]", name, s.CommitIndex(), 6)
		}

		// peer conf should be recovered
		if len(s.Peers()) != 4 {
			t.Fatalf("%s recover phase 1 peer failed! [%d/%d]", name, len(s.Peers()), 4)
		}
	}

	// let nodes talk to each other
	for _, name := range names {
		servers[name].SetTransporter(transporter)
	}

	time.Sleep(2 * testElectionTimeout)

	// should commit to the previous index + 1(nop command when new leader elected)
	for _, name := range names {
		s := servers[name]
		if s.CommitIndex() != 17 {
			t.Fatalf("%s commitIndex is invalid [%d/%d]", name, s.CommitIndex(), 17)
		}
		s.Stop()
	}
}

//--------------------------------------
// Membership
//--------------------------------------

// Ensure that we can start a single server and append to its log.
func TestServerSingleNode(t *testing.T) {
	s := newTestServer("1", &testTransporter{})
	if s.State() != Stopped {
		t.Fatalf("Unexpected server state: %v", s.State())
	}

	s.Start()

	time.Sleep(testHeartbeatInterval)

	// Join the server to itself.
	if _, err := s.Do(&DefaultJoinCommand{Name: "1"}); err != nil {
		t.Fatalf("Unable to join: %v", err)
	}
	debugln("finish command")

	if s.State() != Leader {
		t.Fatalf("Unexpected server state: %v", s.State())
	}

	s.Stop()

	if s.State() != Stopped {
		t.Fatalf("Unexpected server state: %v", s.State())
	}
}

// Ensure that we can start multiple servers and determine a leader.
func TestServerMultiNode(t *testing.T) {
	// Initialize the servers.
	var mutex sync.RWMutex
	servers := map[string]Server{}

	transporter := &testTransporter{}
	transporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		mutex.RLock()
		target := servers[peer.Name]
		mutex.RUnlock()

		b, _ := json.Marshal(req)
		clonedReq := &RequestVoteRequest{}
		json.Unmarshal(b, clonedReq)

		c := make(chan *RequestVoteResponse)

		go func() {
			c <- target.RequestVote(clonedReq)
		}()

		select {
		case resp := <-c:
			return resp
		case <-time.After(time.Millisecond * 200):
			return nil
		}

	}
	transporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		mutex.RLock()
		target := servers[peer.Name]
		mutex.RUnlock()

		b, _ := json.Marshal(req)
		clonedReq := &AppendEntriesRequest{}
		json.Unmarshal(b, clonedReq)

		c := make(chan *AppendEntriesResponse)

		go func() {
			c <- target.AppendEntries(clonedReq)
		}()

		select {
		case resp := <-c:
			return resp
		case <-time.After(time.Millisecond * 200):
			return nil
		}
	}

	disTransporter := &testTransporter{}
	disTransporter.sendVoteRequestFunc = func(s Server, peer *Peer, req *RequestVoteRequest) *RequestVoteResponse {
		return nil
	}
	disTransporter.sendAppendEntriesRequestFunc = func(s Server, peer *Peer, req *AppendEntriesRequest) *AppendEntriesResponse {
		return nil
	}

	var names []string

	n := 5

	// add n servers
	for i := 1; i <= n; i++ {
		names = append(names, strconv.Itoa(i))
	}

	var leader Server
	for _, name := range names {
		s := newTestServer(name, transporter)
		defer s.Stop()

		mutex.Lock()
		servers[name] = s
		mutex.Unlock()

		if name == "1" {
			leader = s
			s.SetHeartbeatInterval(testHeartbeatInterval)
			s.Start()
			time.Sleep(testHeartbeatInterval)
		} else {
			s.SetElectionTimeout(testElectionTimeout)
			s.SetHeartbeatInterval(testHeartbeatInterval)
			s.Start()
			time.Sleep(testHeartbeatInterval)
		}
		if _, err := leader.Do(&DefaultJoinCommand{Name: name}); err != nil {
			t.Fatalf("Unable to join server[%s]: %v", name, err)
		}

	}
	time.Sleep(2 * testElectionTimeout)

	// Check that two peers exist on leader.
	mutex.RLock()
	if leader.MemberCount() != n {
		t.Fatalf("Expected member count to be %v, got %v", n, leader.MemberCount())
	}
	if servers["2"].State() == Leader || servers["3"].State() == Leader {
		t.Fatalf("Expected leader should be 1: 2=%v, 3=%v\n", servers["2"].State(), servers["3"].State())
	}
	mutex.RUnlock()

	for i := 0; i < 20; i++ {
		retry := 0
		fmt.Println("Round ", i)

		num := strconv.Itoa(i%(len(servers)) + 1)
		num_1 := strconv.Itoa((i+3)%(len(servers)) + 1)
		toStop := servers[num]
		toStop_1 := servers[num_1]

		// Stop the first server and wait for a re-election.
		time.Sleep(2 * testElectionTimeout)
		debugln("Disconnect ", toStop.Name())
		debugln("disconnect ", num, " ", num_1)
		toStop.SetTransporter(disTransporter)
		toStop_1.SetTransporter(disTransporter)
		time.Sleep(2 * testElectionTimeout)
		// Check that either server 2 or 3 is the leader now.
		//mutex.Lock()

		leader := 0

		for key, value := range servers {
			debugln("Play begin")
			if key != num && key != num_1 {
				if value.State() == Leader {
					debugln("Found leader")
					for i := 0; i < 10; i++ {
						debugln("[Test] do ", value.Name())
						if _, err := value.Do(&testCommand2{X: 1}); err != nil {
							break
						}
						debugln("[Test] Done")
					}
					debugln("Leader is ", value.Name(), " Index ", value.(*server).log.commitIndex)
				}
				debugln("Not Found leader")
			}
		}
		for {
			for key, value := range servers {
				if key != num && key != num_1 {
					if value.State() == Leader {
						leader++
					}
					debugln(value.Name(), " ", value.(*server).Term(), " ", value.State())
				}
			}

			if leader > 1 {
				if retry < 300 {
					debugln("retry")
					retry++
					leader = 0
					time.Sleep(2 * testElectionTimeout)
					continue
				}
				t.Fatalf("wrong leader number %v", leader)
			}
			if leader == 0 {
				if retry < 300 {
					retry++
					fmt.Println("retry 0")
					leader = 0
					time.Sleep(2 * testElectionTimeout)
					continue
				}
				t.Fatalf("wrong leader number %v", leader)
			}
			if leader == 1 {
				break
			}
		}

		//mutex.Unlock()

		toStop.SetTransporter(transporter)
		toStop_1.SetTransporter(transporter)
	}

}
