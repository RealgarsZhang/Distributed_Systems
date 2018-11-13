package paxos

//
// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import "net"
import "net/rpc"
import "log"
import "os"
import "syscall"
import "sync"
import "fmt"
import "math/rand"
import "time"
//import "strconv"


type Paxos struct {
  mu sync.Mutex
  l net.Listener
  dead bool
  unreliable bool
  rpcCount int
  peers []string
  me int // index into peers[]


  // Your data here.
  peers_min []int // min value of all peers in my view
  instance_map map[int]instance_state // from seq to instance
  global_min int
}
/************************************************************
RPC and other useful structs
************************************************************/

type instance_state struct{
  seq int
  np float64
  na float64
  va interface{}
  decided bool
  v_final interface{}
}

type PrepareArgs struct{
  Seq int
  Proposed_n float64
  Proposer int
  Proposer_Done int
}

type PrepareReply struct{
  Seq int
  Ok bool
  Acceptor int
  Acceptor_na float64
  Acceptor_np float64
  Acceptor_va interface{}
  Acceptor_Done int
  Decided bool
  V_final interface{}
}

type AcceptArgs struct{
  Seq int
  Proposed_n float64
  Proposed_val interface{}
  Proposer int
  Proposer_Done int
}

type AcceptReply struct{
  Seq int
  Ok bool
  Acceptor int
  Acceptor_np float64
  Acceptor_Done int
  Decided bool
  V_final interface{}
}

type DecideArgs struct{
  Seq int
  Val interface{}
  Proposer int
  Proposer_Done int
}

type DecideReply struct{
  Acceptor_Done int
}

/************************************************************
RPC functions
************************************************************/

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareReply) error{
  px.mu.Lock()
  if args.Seq<px.global_min{
    reply.Ok = false
    px.mu.Unlock()
    return nil
  }
  _,cur_instance_state := px.get_instance_state_lockfree(args.Seq)
  if cur_instance_state.decided == true{
    reply.Ok = true
    reply.Decided = true
    reply.V_final = cur_instance_state.v_final
    px.mu.Unlock()
    return nil
  }
  if args.Proposed_n>=cur_instance_state.np{
    cur_instance_state.np = args.Proposed_n
    px.set_instance_state_lockfree(args.Seq,cur_instance_state)
    reply.Ok = true
  }else{
    reply.Ok = false
  }
  px.mu.Unlock()
  reply.Seq = args.Seq
  reply.Acceptor = px.me
  reply.Acceptor_na = cur_instance_state.na
  reply.Acceptor_np = cur_instance_state.np
  reply.Acceptor_va = cur_instance_state.va
  reply.Acceptor_Done = px.get_Done_val(px.me)
  reply.Decided = false
  reply.V_final = nil
  px.update_peers_min(args.Proposer,args.Proposer_Done)
  return nil
}


func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptReply) error{
  px.mu.Lock()
  if args.Seq<px.global_min{
    reply.Ok = false
    px.mu.Unlock()
    return nil
  }
  _,cur_instance_state := px.get_instance_state_lockfree(args.Seq)
  if cur_instance_state.decided == true{
    reply.Ok = true
    reply.Decided = true
    reply.V_final = cur_instance_state.v_final
    px.mu.Unlock()
    return nil
  }
  if args.Proposed_n>=cur_instance_state.np{
    cur_instance_state.np = args.Proposed_n
    cur_instance_state.na = args.Proposed_n
    cur_instance_state.va = args.Proposed_val
    px.set_instance_state_lockfree(args.Seq,cur_instance_state)
    reply.Ok = true
  }else{
    reply.Ok = false
  }
  px.mu.Unlock()
  reply.Seq = args.Seq
  reply.Acceptor = px.me
  reply.Acceptor_np = cur_instance_state.np
  reply.Acceptor_Done = px.get_Done_val(px.me)
  reply.Decided = false
  reply.V_final = nil
  px.update_peers_min(args.Proposer,args.Proposer_Done)
  return nil
}

func (px *Paxos) Decide(args *DecideArgs, reply *DecideReply)error{
  px.update_peers_min(args.Proposer,args.Proposer_Done)
  reply.Acceptor_Done = px.get_Done_val(px.me)
  px.mu.Lock()
  defer px.mu.Unlock()
  temp := px.instance_map[args.Seq]
  if args.Seq<px.global_min||temp.decided{
    return nil
  }
  px.instance_map[args.Seq] = instance_state{ seq:temp.seq,
                                              np: temp.np,
                                              na: temp.na,
                                              va: temp.va,
                                              decided: true,
                                              v_final: args.Val}
//fmt.Print(px.me)
//fmt.Print(" has decided on ")
//fmt.Println(args.Seq)
  /*
  px.instance_map[args.Seq].decided = true
  px.instance_map[args.Seq].v_final = args.Val
  */
  return nil
}


/************************************************************
Private methods
************************************************************/




func (px *Paxos) update_peers_min(peer int, seq int){// NOTE: the seq here is the done value
  px.mu.Lock()
  defer px.mu.Unlock()
  if px.peers_min[peer]>seq{
    return
  }
  old_min := px.global_min

  px.peers_min[peer] = seq+1
  new_min := px.peers_min[0]
  for _, e := range px.peers_min {
    if e < new_min {
        new_min = e
    }
  }
  px.global_min = new_min
  for i:=old_min;i<new_min;i++ {
    _, ok := px.instance_map[i];
    if ok {
       delete(px.instance_map, i);
    }
  }
  return
}

func (px *Paxos) get_Done_val(peer int) int{
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.peers_min[peer]-1
}

func (px *Paxos) is_in_map(seq int) bool {
  px.mu.Lock()
  defer px.mu.Unlock()
  _,ok := px.instance_map[seq]
  return ok
}

func (px *Paxos) get_instance_state(seq int) (bool,instance_state){ // get, or create
  px.mu.Lock()
  defer px.mu.Unlock()
  state,ok := px.instance_map[seq]
  if ok{
    return true, state
  }else{
    px.instance_map[seq] = instance_state{ seq:seq,
                                           va: nil,
                                           v_final:nil}
    return false, px.instance_map[seq]
  }
}

func (px *Paxos) set_instance_state(seq int, src instance_state){
  px.mu.Lock()
  defer px.mu.Unlock()
  px.instance_map[seq] = src
}

func (px *Paxos) get_instance_state_lockfree(seq int) (bool,instance_state){ // get, or create
  state,ok := px.instance_map[seq]
  if ok{
    return true, state
  }else{
    px.instance_map[seq] = instance_state{ seq:seq,
                                           va: nil,
                                           v_final:nil}

    /*
    px.instance_map[seq].seq = seq
    px.instance_map[seq].va = nil
    px.instance_map[seq].v_final = nil
    */
    return false, px.instance_map[seq]
  }
}

func (px *Paxos) set_instance_state_lockfree(seq int, src instance_state){
  px.instance_map[seq] = src
}


func (px *Paxos) startable(seq int) bool{
  px.mu.Lock()
  defer px.mu.Unlock()
  state,ok := px.instance_map[seq]
  if seq<px.global_min || (ok && state.decided){
    return false
  }else{
    return true
  }
}

func (px *Paxos) propose(seq int, v interface{}){//args,reply are for prepare. args1 reply1 are for accept
  _,cur_instance_state := px.get_instance_state(seq)
  // dead need lock?
  var proposed_n float64
  var success bool
  var cnt int
  var highest_na float64
  var highest_na_val interface{}
  var accept_val interface{}
  var args PrepareArgs
  var reply PrepareReply
  var args1 AcceptArgs
  var reply1 AcceptReply
  var args2 DecideArgs
  //var reply2 DecideReply
  id_driver := int(cur_instance_state.np)
  //breaker := 0
  for px.dead == false && cur_instance_state.decided == false{
    proposed_n = get_id(id_driver + 1, px.me)//note this +1. An ID will never be strictly 0.
//fmt.Print("Proposer: ")
//fmt.Println(px.me)

    //prepare phase
    cnt = 0
    highest_na = 0.0
    highest_na_val = nil
    for i:=0;i<len(px.peers);i++{
      //Args =....,reply = ...
      args = PrepareArgs{}
      args.Seq = seq
      args.Proposed_n = proposed_n
      args.Proposer = px.me
      args.Proposer_Done = px.get_Done_val(px.me)
      reply = PrepareReply{}
      if i==px.me{
        px.Prepare(&args,&reply)
        success = true
      }else{
        success = call(px.peers[i],"Paxos.Prepare",&args,&reply)
      }

//fmt.Print(i)
//fmt.Print(": Success called: ")
//fmt.Println(success)
      //Deal with reply
      if success{
        if reply.Decided{
          args2 = DecideArgs{}
          args2.Seq = seq
          args2.Val = reply.V_final
          args2.Proposer = px.me
          args2.Proposer_Done = px.get_Done_val(px.me)
          px.broadcast_decide(args2)
          cnt = 0
          break
        }
        id_driver = max(id_driver,int(reply.Acceptor_np))
        px.update_peers_min(i,reply.Acceptor_Done)
        if reply.Acceptor_na > highest_na{
          highest_na = reply.Acceptor_na
          highest_na_val = reply.Acceptor_va
        }
        if reply.Ok{
          cnt ++
        }
      }
      /*
      sleep_duration := time.Duration(rand.Intn(20)+1)
      time.Sleep(sleep_duration * time.Millisecond)
      */
    }
    if cnt<=len(px.peers)/2{ //Fail. Backoff and retry
      sleep_duration := time.Duration(rand.Intn(10)+3)
      time.Sleep(sleep_duration * time.Millisecond)
      // May need to consider forgetting here
      _,cur_instance_state = px.get_instance_state(seq)
      continue
    }
    //Choose accept value
    if highest_na_val != nil{
      accept_val = highest_na_val
    }else{
      accept_val = v
    }

    //accept.
    cnt = 0
    for i:=0;i<len(px.peers);i++{
      //Args =....,reply = ...
      args1 = AcceptArgs{}
      args1.Seq = seq
      args1.Proposed_n = proposed_n
      args1.Proposed_val = accept_val
      args1.Proposer = px.me
      args1.Proposer_Done = px.get_Done_val(px.me)
      reply1 = AcceptReply{}
      if i==px.me{
        px.Accept(&args1,&reply1)
        success = true
      }else{
        success = call(px.peers[i],"Paxos.Accept",&args1,&reply1)
      }
      if success{
        if reply1.Decided{
          args2 = DecideArgs{}
          args2.Seq = seq
          args2.Val = reply1.V_final
          args2.Proposer = px.me
          args2.Proposer_Done = px.get_Done_val(px.me)
          px.broadcast_decide(args2)
          cnt = 0
          break
        }
        id_driver = max(id_driver,int(reply1.Acceptor_np))
        px.update_peers_min(i,reply1.Acceptor_Done)
        if reply1.Ok{
          cnt ++
        }
      }
      /*
      sleep_duration := time.Duration(rand.Intn(20)+1)
      time.Sleep(sleep_duration * time.Millisecond)
      */

    }
    if cnt<=len(px.peers)/2{ //Fail. Backoff and retry
      sleep_duration := time.Duration(rand.Intn(10)+3)
      time.Sleep(sleep_duration * time.Millisecond)
      // May need to consider forgetting here
      _,cur_instance_state = px.get_instance_state(seq)
      continue
    }
    //Consensus reached.
    args2 = DecideArgs{}
    args2.Seq = seq
    args2.Val = accept_val
    args2.Proposer = px.me
    args2.Proposer_Done = px.get_Done_val(px.me)
    px.broadcast_decide(args2)
    break
    /*
    for i:=0;i<len(px.peers);i++{
//mt.Print("Proposer: ")
//fmt.Print(px.me)
//fmt.Println(i)
//fmt.Println(accept_val)
      args2 = DecideArgs{}
      args2.Seq = seq
      args2.Val = accept_val
      args2.Proposer = px.me
      args2.Proposer_Done = px.get_Done_val(px.me)
      reply2 = DecideReply{}
      if i==px.me{
        px.Decide(&args2,&reply2)
        success = true
      }else{
        success = call(px.peers[i],"Paxos.Decide",&args2,&reply2)
      }
      if success{
        px.update_peers_min(i,reply2.Acceptor_Done)
      }
    }*/

  }
}
func (px *Paxos) broadcast_decide(args2 DecideArgs){
  reply2 := DecideReply{}
  var success bool
  for i:=0;i<len(px.peers);i++{
    if i==px.me{
      px.Decide(&args2,&reply2)
      success = true
    }else{
      success = call(px.peers[i],"Paxos.Decide",&args2,&reply2)
    }
    if success{
      px.update_peers_min(i,reply2.Acceptor_Done)
    }
    /*
    sleep_duration := time.Duration(rand.Intn(20)+1)
    time.Sleep(sleep_duration * time.Millisecond)
    */

  }
}


func get_id(main_num int, proposer int) float64{
  var res float64
  res = 1.0/float64(proposer+2) + float64(main_num)
	return res
}

func max(num1 int, num2 int) int {
  if num1>num2{
    return num1
  }else{
    return num2
  }
}


//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
//
func call(srv string, name string, args interface{}, reply interface{}) bool {
  c, err := rpc.Dial("unix", srv)
  if err != nil {
    err1 := err.(*net.OpError)
    if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
      fmt.Printf("paxos Dial() failed: %v\n", err1)
    }
    return false
  }
  defer c.Close()

  err = c.Call(name, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

/************************************************************
Interfaces
************************************************************/


//
// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
//
func (px *Paxos) Start(seq int, v interface{}) {
  if !px.startable(seq) {
    return
  }else{
    go px.propose(seq,v)
  }
  // Your code here.
}

//
// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
//
func (px *Paxos) Done(seq int) {
  // Your code here.
  px.update_peers_min(px.me,seq)
}

//
// the application wants to know the
// highest instance sequence known to
// this peer.
//
func (px *Paxos) Max() int {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  //if len(px.instance_map)
  cur_max := -1
  for k := range px.instance_map{
    if k>cur_max{
      cur_max = k
    }
  }
  return cur_max
}



//
// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peers z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peers Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
//
func (px *Paxos) Min() int {
  // You code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  return px.global_min
}

//
// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
//
func (px *Paxos) Status(seq int) (bool, interface{}) {
  // Your code here.
  px.mu.Lock()
  defer px.mu.Unlock()
  state,ok := px.instance_map[seq]
  if ok{
    return state.decided, state.v_final
  }else{
    return false, nil
  }

}


//
// tell the peer to shut itself down.
// for testing.
// please do not change this function.
//
func (px *Paxos) Kill() {
  px.dead = true
  if px.l != nil {
    px.l.Close()
  }
}

//
// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
//
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
  px := &Paxos{}
  px.peers = peers
  px.me = me


  // Your initialization code here.
  px.peers_min = make([]int,len(px.peers))
  px.instance_map = make(map[int]instance_state)
  px.global_min = 0
//fmt.Println("Num of peers:")
//fmt.Println(len(px.peers))

  if rpcs != nil {
    // caller will create socket &c
    rpcs.Register(px)
  } else {
    rpcs = rpc.NewServer()
    rpcs.Register(px)

    // prepare to receive connections from clients.
    // change "unix" to "tcp" to use over a network.
    os.Remove(peers[me]) // only needed for "unix"
    l, e := net.Listen("unix", peers[me]);
    if e != nil {
      log.Fatal("listen error: ", e);
    }
    px.l = l

    // please do not change any of the following code,
    // or do anything to subvert it.

    // create a thread to accept RPC connections
    go func() {
      for px.dead == false {
        conn, err := px.l.Accept()
        if err == nil && px.dead == false {
          if px.unreliable && (rand.Int63() % 1000) < 100 {
            // discard the request.
            conn.Close()
          } else if px.unreliable && (rand.Int63() % 1000) < 200 {
            // process the request but force discard of reply.
            c1 := conn.(*net.UnixConn)
            f, _ := c1.File()
            err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
            if err != nil {
              fmt.Printf("shutdown: %v\n", err)
            }
            px.rpcCount++
            go rpcs.ServeConn(conn)
          } else {
            px.rpcCount++
            go rpcs.ServeConn(conn)
          }
        } else if err == nil {
          conn.Close()
        }
        if err != nil && px.dead == false {
          fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
        }
      }
    }()
  }


  return px
}
