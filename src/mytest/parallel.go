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
	OK          = "ok"
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
}

func NewServer() *Server {
	return &Server{
		state:    Running,
		role:     Follower,
		workchan: make(chan *Event, WorkBacklog),
		stopped:  make(chan bool),
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
}

func (this *Server) Stop() {
	if Stopped == this.State() {
		return
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
