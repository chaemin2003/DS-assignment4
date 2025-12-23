package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     int32 // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currView             View                 //  keeps track of current view
	pingTimeMap          map[string]time.Time //  keeps track of most recent time VS heard ping from each server
	primaryAckedCurrView bool                 //  keeps track of whether primary has ACKed the current view
	idleServer           string               //  keeps track of any idle servers
}

// server Ping RPC handler.
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	now := time.Now()
	vs.pingTimeMap[args.Me] = now

	// 첫 서버: view 1 생성
	if vs.currView.Viewnum == 0 {
		vs.currView = View{
			Viewnum: 1,
			Primary: args.Me,
			Backup:  "",
		}
		vs.primaryAckedCurrView = false
		reply.View = vs.currView
		return nil
	}

	// primary ACK 처리
	if args.Me == vs.currView.Primary &&
		args.Viewnum == vs.currView.Viewnum {
		vs.primaryAckedCurrView = true
	}

	// reboot 감지 (Ping(0))
	if args.Viewnum == 0 {
		if args.Me == vs.currView.Primary ||
			args.Me == vs.currView.Backup {
			// 죽은 것으로 간주
			vs.pingTimeMap[args.Me] = time.Time{}
		} else {
			vs.idleServer = args.Me
		}
	}

	// 새로운 idle 서버
	if args.Me != vs.currView.Primary &&
		args.Me != vs.currView.Backup {
		vs.idleServer = args.Me
	}

	reply.View = vs.currView
	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// ACK 안 됐으면 아무것도 하지 말 것
	if !vs.primaryAckedCurrView {
		return
	}

	now := time.Now()
	isDead := func(s string) bool {
		if s == "" {
			return false
		}
		last, ok := vs.pingTimeMap[s]
		if !ok {
			return true
		}
		return now.Sub(last) > DeadPings*PingInterval
	}

	primaryDead := isDead(vs.currView.Primary)
	backupDead := isDead(vs.currView.Backup)

	// 1️⃣ primary 죽음 → backup 승격
	if primaryDead {
		vs.currView = View{
			Viewnum: vs.currView.Viewnum + 1,
			Primary: vs.currView.Backup,
			Backup:  "",
		}
		vs.primaryAckedCurrView = false
		return
	}

	// 2️⃣ backup 죽음
	if backupDead {
		vs.currView = View{
			Viewnum: vs.currView.Viewnum + 1,
			Primary: vs.currView.Primary,
			Backup:  "",
		}
		vs.primaryAckedCurrView = false
		return
	}

	// 3️⃣ backup 없고 idle 서버 있으면
	if vs.currView.Backup == "" && vs.idleServer != "" {
		vs.currView = View{
			Viewnum: vs.currView.Viewnum + 1,
			Primary: vs.currView.Primary,
			Backup:  vs.idleServer,
		}
		vs.idleServer = ""
		vs.primaryAckedCurrView = false
		return
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change these two functions.
func (vs *ViewServer) Kill() {
	atomic.StoreInt32(&vs.dead, 1)
	vs.l.Close()
}

// has this server been asked to shut down?
func (vs *ViewServer) isdead() bool {
	return atomic.LoadInt32(&vs.dead) != 0
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.pingTimeMap = make(map[string]time.Time)
	vs.primaryAckedCurrView = false
	vs.idleServer = ""

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.isdead() == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.isdead() == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.isdead() == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.isdead() == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
