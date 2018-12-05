package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"
import "strconv"

const Debug=0

func DPrintf(format string, a ...interface{}) (n int, err error) {
        if Debug > 0 {
                log.Printf(format, a...)
        }
        return
}


type Op struct {
  // Your definitions here.
  Type string // Put, Get, Receive, Config
  Dohash bool
  Key string
  Value string
  Database [shardmaster.NShards]map[string]string
  Handled_replies [shardmaster.NShards]map[string]interface{}
  Shards_sent [shardmaster.NShards]bool
  Config shardmaster.Config // for Config, not Receive
  Cnum int // for receive.
  ID string
}


type ShardKV struct {
  mu sync.Mutex
  tick_lock sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  sm *shardmaster.Clerk
  px *paxos.Paxos

  gid int64 // my replica group ID

  // Your definitions here.
  database [shardmaster.NShards]map[string]string
  handled_replies [shardmaster.NShards]map[string]interface{}
  serve [shardmaster.NShards]bool

  cur_config shardmaster.Config
  next_config shardmaster.Config
  receive_replies map[string] ReceiveReply
  config_started map[int]bool// lock this!
  top int
}

func (kv *ShardKV) config_end()bool{
  for i:=0;i<shardmaster.NShards;i++{
    if kv.next_config.Shards[i]==kv.gid&&kv.serve[i]==false{
        return false
    }
  }
  kv.cur_config = kv.next_config
  return true
}

func (kv *ShardKV) execute_op(op Op) {// execute and build reply
  var shard int
  //var reply interface{}
/*
if true{
  fmt.Print("GID:")
  fmt.Println(kv.gid)
  fmt.Print("me: ")
  fmt.Println(kv.me)
  fmt.Print("Op Type: ")
  fmt.Println(op.Type)
  fmt.Print("Config: ")
  fmt.Println(op.Config.Num)
  fmt.Println()
}
*/

  if op.Type == "Get"{

    shard = key2shard(op.Key)
    reply := GetReply{}
    if val,ok := kv.database[shard][op.Key];ok{
      reply.Value = val
      reply.Err = OK
    }else{
      reply.Value = ""
      reply.Err = ErrNoKey
    }
    kv.handled_replies[shard][op.ID] = reply
    return

  }else if op.Type=="Put"{

    shard = key2shard(op.Key)
    reply := PutReply{}
    if op.Dohash{
      old_value := kv.database[shard][op.Key]
      h := hash(old_value + op.Value)
      kv.database[shard][op.Key] = strconv.Itoa(int(h))
      reply.Err = OK
      reply.PreviousValue = old_value
    }else{
      old_value := kv.database[shard][op.Key]
      kv.database[shard][op.Key] = op.Value
      reply.Err = OK
      reply.PreviousValue = old_value
    }
    kv.handled_replies[shard][op.ID] = reply
    return

  }else if op.Type=="Receive"{

    reply := ReceiveReply{}
    reply.Received = true
    for i:=0;i<shardmaster.NShards;i++{
      if op.Shards_sent[i]{
        kv.database[i] = op.Database[i]
        kv.handled_replies[i] = op.Handled_replies[i]
        kv.serve[i] = true
      }
    }
    kv.config_end()
    kv.receive_replies[op.ID] = reply
    return
  }else{//Config
    kv.next_config = op.Config
    //Special case,when no shard claimed
    if kv.no_shard_claimed(kv.cur_config){
      kv.cur_config = kv.next_config
      for i:=0;i<shardmaster.NShards;i++{
        if kv.next_config.Shards[i]==kv.gid{
          kv.serve[i]= true
        }else{
          kv.serve[i]= false
        }
      }
    }else{
      var tmp ReceiveArgs
      args_map := make(map[int64]ReceiveArgs)
      for i:=0;i<shardmaster.NShards;i++{
        if kv.next_config.Shards[i]!=kv.gid && kv.serve[i]==true{
          kv.serve[i]=false
          //send this shard
          if _,ok:=args_map[kv.next_config.Shards[i]];!ok{
            args_map[kv.next_config.Shards[i]] = kv.initialize_receiveargs()
          }
          tmp = args_map[kv.next_config.Shards[i]]
          tmp.Database[i] = kv.database[i]
          tmp.Handled_replies[i] = kv.handled_replies[i]
          tmp.Shards_sent[i] = true
          args_map[kv.next_config.Shards[i]] = tmp
        }
      }
      for id, args:= range args_map{
        kv.send_shards(id,args)
      }
      kv.config_end()
      /*
      fmt.Print("GID:")
      fmt.Println(kv.gid)
      fmt.Print("me: ")
      fmt.Println(kv.me)
      fmt.Print("Config: ")
      fmt.Print(op.Config.Num)
      fmt.Println(" Done!")
      */
    }
    reply := ReceiveReply{}
    reply.Received = true
    kv.receive_replies[op.ID] = reply
  }
}
func (kv *ShardKV) send_shards (dst_gid int64, args ReceiveArgs){
  go func(){
    reply := ReceiveReply{}
    reply.Received = false
    ok := false
    servers := kv.next_config.Groups[dst_gid]
    for (!ok || !reply.Received)&&!kv.dead{
      for _, srv := range servers{
        ok = call(srv,"ShardKV.Receive",&args,&reply)
        if ok && reply.Received{
          break
        }
        //time.Sleep(10*time.Millisecond)
        if ok && !reply.Received{
          time.Sleep(2*time.Millisecond)
        }
      }
      //sleep_duration := time.Duration(20)
      //time.Sleep(200*time.Millisecond)
    }

  }()

}

func (kv *ShardKV)initialize_receiveargs() ReceiveArgs{
  res := ReceiveArgs{}
  res.Request_id = "R."+strconv.FormatInt(kv.gid, 10)+"."+strconv.Itoa(kv.next_config.Num)
  res.Cnum = kv.next_config.Num
  res.GID = kv.gid
  return res
}

func (kv *ShardKV) no_shard_claimed(config shardmaster.Config)bool{
  for i:=0;i<shardmaster.NShards;i++{
    if config.Shards[i]>0{
      return false
    }
  }
  return true
}

func (kv *ShardKV) propose(op Op) bool {//False P/G when configuring
  var decided bool
  var decided_op Op
  var temp interface{}
  for !kv.dead{

    kv.px.Start(kv.top, op)
    to := 5 * time.Millisecond
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
      return true
    }
    decided_op = temp.(Op)
    kv.execute_op(decided_op)
    kv.top ++
    kv.px.Done(kv.top-1)
    //handle done, handle P/G when configuring
    if decided_op.ID == op.ID{
      return true
    }else if (decided_op.Type=="Config"||decided_op.Type=="Receive")&&
             (op.Type=="Put"||op.Type=="Get"){
      return false
    }else{
      //sleep_duration := time.Duration(rand.Intn(10))
      //time.Sleep(sleep_duration*time.Millisecond)
    }
  }
  return true
}


func (kv *ShardKV) apply_logs(){//apply decided logs, to catch up
  var decided bool
  var decided_op Op
  var temp interface{}
  for !kv.dead{
    decided,temp = kv.px.Status(kv.top)
    if !decided{
      return
    }
    if temp == nil{
      return
    }
    decided_op = temp.(Op)
    kv.execute_op(decided_op)
    kv.top ++
    kv.px.Done(kv.top-1)
  }

}


func (kv *ShardKV) Receive(args *ReceiveArgs, reply *ReceiveReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.apply_logs()

  if val, ok:= kv.receive_replies[args.Request_id];ok{
    *reply = val
    return nil
  }
  if kv.cur_config.Num == kv.next_config.Num{
    reply.Received = false
    return nil
  }
  if kv.next_config.Num != args.Cnum{
    reply.Received = false
    return nil
  }
  op := Op{}
  op.Type = "Receive"
  op.Database = args.Database
  op.Handled_replies = args.Handled_replies
  op.Shards_sent = args.Shards_sent
  op.Cnum = args.Cnum
  op.ID = args.Request_id//"R."+strconv.FormatInt(args.GID, 10)+"."+strconv.Itoa(args.Cnum)//TODO
  kv.propose(op)

  reply.Received = true
  return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.apply_logs()

  if kv.cur_config.Num != kv.next_config.Num{
    reply.Err = ErrConfiguring
    return nil
  }

  shard := key2shard(args.Key)
  if kv.serve[shard] == false{
    reply.Err = ErrWrongGroup
    return nil
  }

  if val, ok := kv.handled_replies[shard][args.Request_id]; ok {
    *reply = val.(GetReply)
    return nil
  }


  op := Op{}
  op.Type = "Get"
  op.Key = args.Key
  op.ID = args.Request_id
  ok := kv.propose(op)
  if ok{
    *reply = kv.handled_replies[shard][args.Request_id].(GetReply)
  }else{
    reply.Err = ErrConfiguring
  }
  return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  kv.mu.Lock()
  defer kv.mu.Unlock()
  kv.apply_logs()

  if kv.cur_config.Num != kv.next_config.Num{
    reply.Err = ErrConfiguring
    return nil
  }

  shard := key2shard(args.Key)
  if kv.serve[shard] == false{
    reply.Err = ErrWrongGroup
    return nil
  }

  if val, ok := kv.handled_replies[shard][args.Request_id]; ok {
    *reply = val.(PutReply)
    return nil
  }

  op := Op{}
  op.Type = "Put"
  op.Dohash = args.DoHash
  op.Key = args.Key
  op.Value = args.Value
  op.ID = args.Request_id

  ok := kv.propose(op)
  if ok{
    *reply = kv.handled_replies[shard][args.Request_id].(PutReply)
  }else{
    reply.Err = ErrConfiguring
  }

  return nil
}

func (kv *ShardKV) Config(config shardmaster.Config) error {
  // Your code here.
  kv.apply_logs()

  op := Op{}
  op.Type = "Config"
  op.Config = config
  op.ID = "C."+strconv.Itoa(config.Num)

  if _, ok := kv.receive_replies[op.ID]; ok {
    return nil
  }

  kv.propose(op)


  return nil
}

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
  kv.mu.Lock()
  defer kv.mu.Unlock()
  if kv.cur_config.Num!=kv.next_config.Num{
    return
  }
  query_num := kv.cur_config.Num+1
  config := kv.sm.Query(query_num)
  if config.Num == query_num {

    if val,ok := kv.config_started[config.Num];ok&&val{
      return
    }else{
      kv.config_started[config.Num] = true
      /*
      if kv.unreliable{
        fmt.Print("GID:")
        fmt.Println(kv.gid)
        fmt.Print("me: ")
        fmt.Println(kv.me)
        fmt.Print("Config num: ")
        fmt.Println(config.Num)
        fmt.Println()
      }*/
      kv.Config(config)
      return
    }

  }
  return
}


// tell the server to shut itself down.
func (kv *ShardKV) kill() {
  kv.dead = true
  kv.l.Close()
  kv.px.Kill()
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
                 servers []string, me int) *ShardKV {
  gob.Register(Op{})
  gob.Register(PutReply{})
  gob.Register(GetReply{})

  kv := new(ShardKV)
  kv.me = me
  kv.gid = gid
  kv.sm = shardmaster.MakeClerk(shardmasters)

  // Your initialization code here.
  // Don't call Join().
  for k := 0; k < shardmaster.NShards; k++ {
		kv.database[k] = make(map[string]string)
		kv.handled_replies[k] = make(map[string]interface{})
    kv.serve[k] = false
	}
  kv.cur_config = shardmaster.Config{}
  kv.next_config = shardmaster.Config{}
  kv.receive_replies = make(map[string]ReceiveReply)
  kv.config_started = make(map[int]bool)
  kv.top = 0


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
        fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
        kv.kill()
      }
    }
  }()

  go func() {
    for kv.dead == false {
      kv.tick()
      time.Sleep(250 * time.Millisecond)
    }
  }()

  return kv
}
