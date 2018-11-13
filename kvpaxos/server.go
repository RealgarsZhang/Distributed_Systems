package kvpaxos

import "net"
import "fmt"
import "net/rpc"
import "log"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "strconv"
import "time"
import "strings"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    log.Printf(format, a...)
  }
  return
}


type Op struct {
  // Your definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Op_type string
  Request_id string
  Key string
  New_value string

}

type KVPaxos struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  // Your definitions here.
  /*
  replies, top, database
  */
  database map[string]string // key -> value
  top int
  handled_replies map[string]interface{}// request_id->reply
  done_bool map[string]bool
}

func (kv *KVPaxos) clear_handled_replies(delete_id string){
  if delete_id == ""{
    return
  }
  splited_id := strings.Split(delete_id,"W")
  client_id := splited_id[0]
  seq,_ := strconv.ParseInt(splited_id[1],10,64)
  var i int64
  for i=0;i<=seq;i++{
    id := client_id + "W" + strconv.FormatInt(i, 10)
    if _,ok := kv.handled_replies[id];ok{
      delete(kv.handled_replies,id)
    }
  }
}

func (kv *KVPaxos) execute_op(decided_op Op) {
  kv.done_bool[decided_op.Request_id] = true
  if decided_op.Op_type == "Get"{
    if val,ok := kv.database[decided_op.Key]; ok{
      kv.handled_replies[decided_op.Request_id] = GetReply{OK,val}
    }else{
      kv.handled_replies[decided_op.Request_id] = GetReply{ErrNoKey,""}
    }
  }else if decided_op.Op_type =="Put" {
    old_value := kv.database[decided_op.Key]
    kv.database[decided_op.Key] = decided_op.New_value
    kv.handled_replies[decided_op.Request_id] = PutReply{OK,old_value}
  }else{//"PutHash"
    old_value := kv.database[decided_op.Key]
    h := hash(old_value + decided_op.New_value)
    kv.database[decided_op.Key] = strconv.Itoa(int(h))
    kv.handled_replies[decided_op.Request_id] = PutReply{OK,old_value}
  }
  //return kv.handled_replies[decided_op.request_id]
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if val, ok := kv.handled_replies[args.Request_id]; ok {
    *reply = val.(GetReply)
    //reply.Err = val.Err
    //reply.Value = val.Value
    return nil
  }
  if _,ok:= kv.done_bool[args.Request_id]; ok{
    return nil
  }
  //if never handled this request:
  requested_op := Op{}
  requested_op.Op_type = "Get"
  requested_op.Request_id = args.Request_id
  requested_op.Key = args.Key
  requested_op.New_value = ""
  var decided bool
  var decided_op Op
  var temp interface{}
  //to_out := 10 * time.Millisecond
  for !kv.dead{
    kv.px.Start(kv.top, requested_op)

    to := 10 * time.Millisecond
    for !kv.dead{
      decided,temp = kv.px.Status(kv.top)
      if decided{
        break
      }else{
        time.Sleep(to)
        if to < 10 * time.Second {
          to *= 2
        }
      }
    }
    //execute the op, TODO
    if temp == nil{
      return nil
    }
    decided_op = temp.(Op)
    kv.execute_op(decided_op)
    kv.top ++
    kv.px.Done(kv.top-1)

    if decided_op.Request_id == args.Request_id{
      *reply = kv.handled_replies[decided_op.Request_id].(GetReply)
      kv.clear_handled_replies(args.Delete_id)
      /*
      if _,ok := kv.handled_replies[args.Delete_id];ok{
        delete(kv.handled_replies,args.Delete_id)
      }
      */
      return nil
    }else{
      sleep_duration := time.Duration(rand.Intn(100)+60)
      time.Sleep(sleep_duration*time.Millisecond)
      /*
      time.Sleep(to_out)
      if to_out < 4 * time.Second {
        to_out *= 2
      }
      */
    }

  }


  return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  /* Find a position(seq) for the op.
  lock, defer lock
  if op_id in replies: reply and return
  else:
  kv.px.start(kv.top,income_op) (Guarantee a consensus will be eventually reached!!!)
  for not decided:
     decided = kv.px.status(kv.top)
  execute(decided_op)(How is the order enforced? )
  kv.top += 1
  if decided_id is not the op_id: (maybe sleep a while) retry since start
  kv.px.Done(kv.top -1)
  build reply,put it in replies map. DON'T clear the replies map! For at most once!
  */
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if val, ok := kv.handled_replies[args.Request_id]; ok {
    *reply = val.(PutReply)
    //reply.Err = val.Err
    //reply.Value = val.PreviousValue
    return nil
  }
  if _,ok:= kv.done_bool[args.Request_id]; ok{
    return nil
  }

  requested_op := Op{}
  if args.DoHash{
    requested_op.Op_type = "PutHash"
  }else{
    requested_op.Op_type = "Put"
  }
  requested_op.Request_id = args.Request_id
  requested_op.Key = args.Key
  requested_op.New_value = args.Value
  var decided bool
  var decided_op Op
  var temp interface{}
  //to_out := 10 * time.Millisecond
  for !kv.dead{
    kv.px.Start(kv.top, requested_op)

    to := 10 * time.Millisecond
    for !kv.dead{
      decided,temp = kv.px.Status(kv.top)
      if decided{
        break
      }else{
        time.Sleep(to)
        if to < 10 * time.Second {
          to *= 2
        }
      }
    }
    //execute the op, TODO
    if temp == nil{
      return nil
    }
    decided_op = temp.(Op)
    kv.execute_op(decided_op)
    kv.top ++
    kv.px.Done(kv.top-1)

    if decided_op.Request_id == args.Request_id{
      *reply = kv.handled_replies[decided_op.Request_id].(PutReply)
      kv.clear_handled_replies(args.Delete_id)
      /*
      if _,ok := kv.handled_replies[args.Delete_id];ok{
        delete(kv.handled_replies,args.Delete_id)
      }
      */
      return nil
    }else{
      sleep_duration := time.Duration(rand.Intn(100)+60)
      time.Sleep(sleep_duration*time.Millisecond)
      //time.Sleep(70 * time.Millisecond)
      /*
      time.Sleep(to_out)
      if to_out < 4 * time.Second {
        to_out *= 2
      }
      */
    }

  }


  return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
  DPrintf("Kill(%d): die\n", kv.me)
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *KVPaxos {
  // call gob.Register on structures you want
  // Go's RPC library to marshall/unmarshall.
  gob.Register(Op{})
  gob.Register(GetReply{})
  gob.Register(PutReply{})

  kv := new(KVPaxos)
  kv.me = me
  kv.database = make(map[string]string)
  kv.top = 0
  kv.handled_replies = make(map[string]interface{})
  kv.done_bool = make(map[string]bool)

  // Your initialization code here.

  rpcs := rpc.NewServer()
  rpcs.Register(kv)

  kv.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  kv.l = l


  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for kv.dead == false {
      conn, err := kv.l.Accept()
      if err == nil && kv.dead == false {
        if kv.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if kv.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          go rpcs.ServeConn(conn)
        } else {
          go rpcs.ServeConn(conn)
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && kv.dead == false {
        fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  return kv
}
