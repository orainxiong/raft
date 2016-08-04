package mytest

import (
	"errors"
	"fmt"
	"log"
	"sync"
	"time"
)

const (
	timeout     = 1
	Running     = "running"
	Stopped     = "stopped"
	Follower    = "follower"
	WorkBacklog = 256
)

type Command struct {
	name string
}

type Event struct {
	Source    interface{}
	ReturnVal string
	c         chan error
}

func NewEvent() *Event {
	return &Event{
		Source: &Command{name: "test"},
		c:      make(chan error),
	}
}

type Peer struct {
	sync.RWMutex
	name         string
	state        string
	stopped      chan bool
	lastActivity time.Time
	server       *Server
}

func NewPeer(name string, server *Server) *Peer {
	return &Peer{
		name:    name,
		stopped: make(chan bool),
		server:  server,
	}
}

func (this *Peer) State() string {
	this.RLock()
	defer this.RUnlock()
	return this.state
}

func (this *Peer) SetState(state string) {
	this.Lock()
	defer this.Unlock()
	this.state = state
}

func (this *Peer) GetState() string {
	this.RLock()
	defer this.RUnlock()
	return fmt.Sprintf("peer %s state %s", this.name, this.state)
}

func (this *Peer) setLastActivity(now time.Time) {
	this.Lock()
	defer this.Unlock()
	this.lastActivity = now
}

func (this *Peer) StartHeartbeat() {

	startup := make(chan bool)
	this.server.RoutineWaitGroup.Add(1)
	go func() {
		defer this.server.RoutineWaitGroup.Done()
		this.heartbeat(startup)
	}()
	<-startup
}

func (this *Peer) StopHeartbeat() {
	close(this.stopped)
	this.SetState(Stopped)
}

func (this *Peer) heartbeat(startup chan bool) {
	ticker := time.Tick(timeout * time.Second)
	startup <- true
	for {
		select {
		case <-ticker:
			log.Printf("peer %s heatbeat", this.name)
			this.setLastActivity(time.Now())
		case <-this.stopped:
			log.Printf("peer %s stop", this.name)
			this.SetState(Stopped)
			return
		}
	}
}

type ParallelBase interface {
	Do(*Event) error
	Send(*Event) error
	Start()
	Stop()
}

type Server struct {
	sync.RWMutex
	state            string
	role             string
	RoutineWaitGroup sync.WaitGroup
	workchan         chan *Event
	stopped          chan bool
	peers            map[string]*Peer
}

func NewServer() *Server {
	return &Server{
		state:    Running,
		role:     Follower,
		workchan: make(chan *Event, WorkBacklog),
		stopped:  make(chan bool),
		peers:    make(map[string]*Peer),
	}
}

func (this *Server) Do(e *Event) error {
	return this.Send(e)
}

func (this *Server) Send(e *Event) error {
	var err error
	select {
	case this.workchan <- e:
	case <-time.After(timeout * time.Second):
		err = errors.New(fmt.Sprintf("request timeout %d", timeout))
	}

	select {
	case err = <-e.c:
	case <-time.After(timeout * time.Second):
		err = errors.New(fmt.Sprintf("response timeout %d", timeout))

	}
	return err
}

func (this *Server) Start() {
	log.Println("server start")
	this.RoutineWaitGroup.Add(1)
	go func() {
		defer this.RoutineWaitGroup.Done()
		this.loop()
	}()

	for _, peer := range this.peers {
		log.Printf("peer %s startheartbeat", peer.name)
		peer.StartHeartbeat()
	}
}

func (this *Server) Stop() {
	if Stopped == this.State() {
		return
	}

	for _, peer := range this.peers {
		log.Printf("peer %s stopheartbeat", peer.name)
		peer.StopHeartbeat()
	}

	close(this.stopped)
	this.RoutineWaitGroup.Wait()
	this.SetState(Stopped)
	log.Print("server end")
}

func (this *Server) State() string {
	this.RLock()
	defer this.RUnlock()
	return this.state
}

func (this *Server) SetState(state string) {
	this.Lock()
	defer this.Unlock()
	this.state = state
}

func (this *Server) Role() string {
	this.RLock()
	defer this.RUnlock()
	return this.role
}

func (this *Server) loop() {
	log.Println("server loop start")
	defer log.Println("server loop end")

	for this.State() != Stopped {
		switch this.Role() {
		case Follower:
			log.Println("server loop as follower")
			this.followerLoop()
		}
	}
}

func (this *Server) followerLoop() {
	for Follower == this.Role() {
		var err error
		select {
		case <-this.stopped:
			this.SetState(Stopped)
			return
		case event := <-this.workchan:
			log.Println("get event")
			switch req := event.Source.(type) {
			case *Command:
				event.ReturnVal = req.name
			default:
				err = errors.New("not support command")
			}
			event.c <- err
		}
	}
}
