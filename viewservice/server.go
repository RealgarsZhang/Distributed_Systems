
package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"

type ViewServer struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  me string

  next_view View
  return_view View
  return_view_ACKed bool
  cur_viewnum uint
  last_ping_time map[string]time.Time
  missed_ping map[string]int
  // Your declarations here.
}
/*
func (vs *ViewServer) Compare(which int,input string) bool{
  if which == 1{ // compare with Primary
    vs.mu.Lock()
    res := (input == vs.return_view.Primary)
    vs.mu.Unlock()
  }else{ //compare with Backup
    vs.mu.Lock()
    res := (input == vs.return_view.Backup)
    vs.mu.Unlock()
  }
  return res
}
*/

func (vs *ViewServer) AttemptSetView(target View,reply *PingReply){ // Must be called locked!!!!
  if vs.return_view_ACKed{
    vs.return_view = target
    vs.return_view_ACKed = false
    vs.cur_viewnum ++
    //fmt.Printf("The viewnum is %d \n",vs.cur_viewnum)
  }else{
    vs.next_view = target
  }
  reply.View = vs.return_view
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
  vs.mu.Lock()
  name := args.Me
  primary := vs.return_view.Primary
  backup := vs.return_view.Backup
  vs.last_ping_time[name] = time.Now()
  vs.missed_ping[name] = 0
  if name!=primary && name!=backup{
    if primary == "" && backup == ""{
      //fmt.Printf("In Ping:The viewnum is %d \n",vs.cur_viewnum)
      vs.return_view = View{vs.cur_viewnum,name,""}
      vs.return_view_ACKed = false
      vs.cur_viewnum ++
      reply.View = vs.return_view
    }else if backup == ""{
      target := View{vs.cur_viewnum,primary,name}
      vs.AttemptSetView(target,reply)
    }else{
      reply.View = vs.return_view
    }
  }else if name == backup{
    if args.Viewnum == 0{//restarted backup
      target := View{vs.cur_viewnum,primary,name}
      vs.AttemptSetView(target,reply)
    }else{
      reply.View = vs.return_view
    }
  }else if name == primary{
    if args.Viewnum == 0{ // restarted primary
      target := View{vs.cur_viewnum,backup,primary}
      vs.AttemptSetView(target,reply)
    }else{
      if args.Viewnum == vs.return_view.Viewnum{
        vs.return_view_ACKed = true
        if vs.next_view.Viewnum != 0{
          vs.return_view = vs.next_view
          vs.return_view_ACKed = false
          vs.cur_viewnum ++
          vs.next_view.Viewnum = 0
        }
      }
      reply.View = vs.return_view
    }
  }
  vs.mu.Unlock()
  // Your code here.

  return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
  vs.mu.Lock()
  reply.View = vs.return_view
  vs.mu.Unlock()
  // Your code here.

  return nil
}


//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {

  vs.mu.Lock()
  primary := vs.return_view.Primary
  backup := vs.return_view.Backup

  if vs.missed_ping[primary]>DeadPings{
    rubbish := &PingReply{}
    target := View{vs.cur_viewnum,backup,""}
    vs.AttemptSetView(target,rubbish)
    //fmt.Printf("In Tick:The viewnum is %d \n",vs.cur_viewnum)
  }
  if vs.missed_ping[backup]>DeadPings{
    rubbish := &PingReply{}
    target := View{vs.cur_viewnum,primary,""}
    vs.AttemptSetView(target,rubbish)
  }
  for k,_ := range vs.missed_ping{
    vs.missed_ping[k] += 1
  }
  vs.mu.Unlock()
  // Your code here.
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
  vs.dead = true
  vs.l.Close()
}

func StartServer(me string) *ViewServer {
  vs := new(ViewServer)
  vs.me = me
  // Your vs.* initializations here.
  vs.next_view = View{0,"",""}
  vs.return_view = View{0,"",""}
  vs.return_view_ACKed = true
  vs.cur_viewnum = 1
  vs.last_ping_time = make(map[string]time.Time)
  vs.missed_ping = make(map[string]int)
  // tell net/rpc about our RPC server and handlers.
  rpcs := rpc.NewServer()
  rpcs.Register(vs)

  // prepare to receive connections from clients.
  // change "unix" to "tcp" to use over a network.
  os.Remove(vs.me) // only needed for "unix"
  l, e := net.Listen("unix", vs.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  vs.l = l

  // please don't change any of the following code,
  // or do anything to subvert it.

  // create a thread to accept RPC connections from clients.
  go func() {
    for vs.dead == false {
      conn, err := vs.l.Accept()
      if err == nil && vs.dead == false {
        go rpcs.ServeConn(conn)
      } else if err == nil {
        conn.Close()
      }
      if err != nil && vs.dead == false {
        fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
        vs.Kill()
      }
    }
  }()

  // create a thread to call tick() periodically.
  go func() {
    for vs.dead == false {
      vs.tick()
      time.Sleep(PingInterval)
    }
  }()

  return vs
}
