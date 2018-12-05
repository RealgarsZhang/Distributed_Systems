package shardmaster

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
//import "math/rand"
import "sort"

type ShardMaster struct {
  mu sync.Mutex
  l net.Listener
  me int
  dead bool // for testing
  unreliable bool // for testing
  px *paxos.Paxos

  configs []Config // indexed by config num
  top int
  op_seq int
  shards_map map[int64][]int
}


type Op struct {
  // Your data here.
  Op_type string
  Op_ID string
  Servers []string
  GID int64
  Shard int
  Config_num int // for query

}


func (sm *ShardMaster) try_n_exe(requested_op Op){
  //caller should get Lock
  var decided bool
  var decided_op Op
  var temp interface{}
  for !sm.dead{
    sm.px.Start(sm.top, requested_op)

    to := 5 * time.Millisecond
    for !sm.dead{
      decided,temp = sm.px.Status(sm.top)
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
      return
    }
    decided_op = temp.(Op)
    sm.execute_op(decided_op)
    sm.top ++
    sm.px.Done(sm.top-1)

    if decided_op.Op_ID == requested_op.Op_ID{
      return
    }else{
      sleep_duration := time.Duration(rand.Intn(20)+10)
      time.Sleep(sleep_duration*time.Millisecond)
      /*
      time.Sleep(to_out)
      if to_out < 4 * time.Second {
        to_out *= 2
      }
      */
    }

  }

}

func (sm *ShardMaster) execute_op(decided_op Op){
  if decided_op.Op_type == "Join"{
    sm.handle_join(decided_op)
  }else if decided_op.Op_type == "Leave"{
    sm.handle_leave(decided_op)
  }else if decided_op.Op_type == "Move"{
    sm.handle_move(decided_op)
  }else{//query
    sm.handle_query(decided_op)
  }
}

func get_shards(shards_map map[int64][]int)[NShards]int64{
  var res [NShards]int64
  for k,v := range shards_map{
    for _,val := range v{
      res[val] = k
    }
  }
  return res
}

func move_element(shards_map map[int64][]int, src int64, dst int64, src_idx int){
  shards_map[dst] = append(shards_map[dst],shards_map[src][src_idx])
  shards_map[src] = append(shards_map[src][:src_idx], shards_map[src][src_idx+1:]...)
}

func copy_map(m1 map[int64][]string,m2 map[int64][]string){
  for k,v := range m2{
    m1[k] = v
  }
}

func balance(shards_map map[int64][]int){
  //generate the target needed

/*  tmp :=0
  for _,v:= range shards_map{
    if tmp<len(v){
      tmp = len(v)
    }
  }
  if tmp<=1{
    return
  }
  */
  target := make([]int,0)
  total := NShards
  n := len(shards_map)
  for total>0{
    target = append(target,total/n)
    total -= (total/n)
    n -= 1
  }
  sort.Sort(sort.Reverse(sort.IntSlice(target)))
  //bubble sort the keys wrt value
  keys := make([]int64,0)
  n = len(shards_map)
  for k := range shards_map{
    keys = append(keys,k)
  }
  swapped := true
  for swapped {
        swapped = false
        for i := 1; i < n; i++ {
            if len(shards_map[keys[i]])>len(shards_map[keys[i-1]]){
                keys[i], keys[i-1] = keys[i-1], keys[i]
                swapped = true
            }
        }
  }
/*
fmt.Print("Keys are:")
fmt.Println(keys)
fmt.Print("Shards_map is:")
fmt.Println(shards_map)
fmt.Print("Target is:")
fmt.Println(target)
*/

  for i:=0;i<n;i++{

    for len(shards_map[keys[i]])<target[i]{
      for j:=0;j<n;j++{
        if len(shards_map[keys[j]])>target[j]{
          move_element(shards_map,keys[j],keys[i],len(shards_map[keys[j]])-1)
          break
        }
      }
    }

    for len(shards_map[keys[i]])>target[i]{
      for j:=0;j<n;j++{
        if len(shards_map[keys[j]])<target[j]{
          move_element(shards_map,keys[i],keys[j],len(shards_map[keys[i]])-1)
          break
        }
      }
    }

  }

}


func (sm *ShardMaster) handle_join(op Op){
  if _, ok := sm.shards_map[op.GID]; ok {
    sm.configs = append(sm.configs,sm.configs[len(sm.configs)-1])
    sm.configs[len(sm.configs)-1].Num ++
    return
  }
  new_config := Config{}
  new_config.Num = len(sm.configs)
  new_config.Groups = make(map[int64][]string)
  copy_map(new_config.Groups,sm.configs[len(sm.configs)-1].Groups)
  new_config.Groups[op.GID] = op.Servers
  /*
  if len(sm.shards_map) == NShards{
    new_config.Shards = get_shards(sm.shards_map)
    sm.configs = append(sm.configs,new_config)
    return
  }
  */


  if len(sm.shards_map) ==0{
    sm.shards_map[op.GID] = make([]int,0)
    for i:=0;i<NShards;i++{
      sm.shards_map[op.GID] = append(sm.shards_map[op.GID],i)
    }
    new_config.Shards = get_shards(sm.shards_map)
  }else{
    //construct shards_map

    sm.shards_map[op.GID] = make([]int,0)
    //balance
//fmt.Println("ever balancing!")
    balance(sm.shards_map)
    /*
    target := NShards/len(sm.shards_map)
    for len(sm.shards_map[op.GID])<target{
      cur_max := make([]int)
      var cur_max_key int64 = 0
      for k,v := range(sm.shards_map){
        if len(v)>len(cur_max)&&k!=op.GID{ cur_max = v; cur_max_key = k;}
      }
      move_element(sm.shards_map,cur_max_key, op.GID,0)
    }
    */
    //assign shards
    new_config.Shards = get_shards(sm.shards_map)
  }

  sm.configs = append(sm.configs,new_config)
  return
}

func (sm *ShardMaster) handle_leave(op Op){
  if _, ok := sm.shards_map[op.GID]; !ok {
    sm.configs = append(sm.configs,sm.configs[len(sm.configs)-1])
    sm.configs[len(sm.configs)-1].Num ++
    return
  }
  new_config := Config{}
  new_config.Num = len(sm.configs)
  new_config.Groups = make(map[int64][]string)
  copy_map(new_config.Groups,sm.configs[len(sm.configs)-1].Groups)
  delete(new_config.Groups, op.GID)

  if len(new_config.Groups) == 0{
    sm.shards_map = make(map[int64][]int)
    new_config.Shards = sm.configs[0].Shards
  }else{
    //add my value to max
    var cur_key int64 = 0
    cur_max := 0
    for k,v:= range sm.shards_map{
      if k!= op.GID && len(v)>cur_max{
        cur_key = k
        cur_max = len(v)
      }
    }
    for len(sm.shards_map[op.GID])>0{
      move_element(sm.shards_map, op.GID, cur_key, 0)
    }

    delete(sm.shards_map,op.GID)
    balance(sm.shards_map)
    /*
    for len(sm.shards_map[op.GID])>0{
      cur_min := make([]int,NShards+1)
      var cur_min_key int64 = 0
      for k,v := range(sm.shards_map){
        if len(v)<len(cur_min)&&k!=op.GID{ cur_min = v; cur_min_key = k;}
      }
      move_element(sm.shards_map,op.GID,cur_min_key,0)
    }
    */

    // assign shards
    new_config.Shards = get_shards(sm.shards_map)

  }

  sm.configs = append(sm.configs,new_config)
  return
}

func (sm *ShardMaster) find_shard(target_shard int)(int64, int) {
  for k,v := range sm.shards_map{
    for idx,shard := range v{
      if shard == target_shard{
        return k,idx
      }
    }
  }
  return 0,0
}

func (sm *ShardMaster) handle_move(op Op){
  dst := op.GID
  src,src_idx := sm.find_shard(op.Shard)
  move_element(sm.shards_map,src,dst,src_idx)

  new_config := Config{}
  new_config.Num = len(sm.configs)
  new_config.Groups = make(map[int64][]string)
  copy_map(new_config.Groups,sm.configs[len(sm.configs)-1].Groups)
  new_config.Shards = get_shards(sm.shards_map)
  sm.configs = append(sm.configs,new_config)
  return
}

func (sm *ShardMaster) handle_query(op Op){
  return
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()
  // construct op
  op := Op{}
  op.Op_type = "Join"
  op.Op_ID = sm.get_op_id()
  op.GID = args.GID
  op.Servers = args.Servers
  // try_n_exe
  sm.try_n_exe(op)
  return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{}
  op.Op_type = "Leave"
  op.Op_ID = sm.get_op_id()
  op.GID = args.GID
  sm.try_n_exe(op)

  return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{}
  op.Op_type = "Move"
  op.Op_ID = sm.get_op_id()
  op.Shard = args.Shard
  op.GID = args.GID
  sm.try_n_exe(op)

  return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
  // Your code here.
  sm.mu.Lock()
  defer sm.mu.Unlock()

  op := Op{}
  op.Op_type = "Query"
  op.Op_ID = sm.get_op_id()
  op.Config_num = args.Num
  sm.try_n_exe(op)

  n := len(sm.configs)
  if args.Num==-1 || args.Num>=n{
    reply.Config = sm.configs[n-1]
  }else{
    reply.Config = sm.configs[args.Num]
  }
  //remember to write reply
  return nil
}




func (sm *ShardMaster) get_op_id() string{
  // The caller should have got the lock.
  res := strconv.Itoa(sm.me) + "." +strconv.Itoa(sm.op_seq)
  sm.op_seq ++
  return res
}



// please don't change this function.
func (sm *ShardMaster) Kill() {
  sm.dead = true
  sm.l.Close()
  sm.px.Kill()
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
  gob.Register(Op{})

  sm := new(ShardMaster)
  sm.me = me

  sm.configs = make([]Config, 1)
  sm.configs[0].Groups = map[int64][]string{}
  sm.shards_map = make(map[int64][]int)

  rpcs := rpc.NewServer()
  rpcs.Register(sm)

  sm.px = paxos.Make(servers, me, rpcs)

  os.Remove(servers[me])
  l, e := net.Listen("unix", servers[me]);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  sm.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for sm.dead == false {
      conn, err := sm.l.Accept()
      if err == nil && sm.dead == false {
        if sm.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if sm.unreliable && (rand.Int63() % 1000) < 200 {
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
      if err != nil && sm.dead == false {
        fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
        sm.Kill()
      }
    }
  }()

  return sm
}
