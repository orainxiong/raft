package mytest

import (
	"log"
	"testing"
	"time"
)

func TestNewServer(t *testing.T) {
	/*
		测试并发模型
			1. server 基于 waitgroup 记录所有运行的 goroutine. 当退出的时候通过 wait 方式,等待所有的 goroutine 退出
			2. 所有的 goroutine 通过 server 的 stop channel 实时trace server 的状态
			3. event 中存放 channel error
				3.1 通过 channel 传递 error
				3.2 通过 channel 做到同步等待 event 的返回.以确定 event 确实被消费掉
	*/
	log.SetFlags(log.Ldate | log.Ltime | log.Lshortfile)
	server := NewServer()

	peer := NewPeer("xuyuan", server)

	server.peers[peer.name] = peer

	server.Start()
	defer server.Stop()

	var (
		e   *Event
		err error
	)
	e = NewEvent()
	log.Printf("%#v", e)
	err = server.Do(e)
	log.Printf("%#v , err %s", e, err)


	time.Sleep(10 * time.Second)
}
