package kvpaxos

import "net/rpc"
import "fmt"
import "sync"
import "strconv"
//import "math/rand"
import "time"


type Clerk struct {
  servers []string
  // You will have to modify this struct.
  mu sync.Mutex // restrict the increment on request_seq
  clerk_id int64
  request_seq int64
  delete_id string
}


func MakeClerk(servers []string) *Clerk {
  ck := new(Clerk)
  ck.servers = servers
  // You'll have to add code here.
  ck.clerk_id = nrand()
  ck.request_seq = 0
  ck.delete_id = ""
  return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
          args interface{}, reply interface{}) bool {
  c, errx := rpc.Dial("unix", srv)
  if errx != nil {
    return false
  }
  defer c.Close()

  err := c.Call(rpcname, args, reply)
  if err == nil {
    return true
  }

  fmt.Println(err)
  return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
  // You will have to modify this function.
  request_id := ck.get_request_id()
  args := GetArgs{}
  args.Key = key
  args.Request_id = request_id
  args.Delete_id = ck.delete_id
  reply := GetReply{}
  num_server := len(ck.servers)
  i := 0
  to := 25 * time.Millisecond
  for true{
    success := call(ck.servers[i],"KVPaxos.Get",&args,&reply)
    if success{
      ck.delete_id = request_id
      if reply.Err == OK{
        return reply.Value
      }else{
        return ""
      }
    }else{
      i = (i+1)%num_server
      time.Sleep(to)
      if to < 5 * time.Second {
        to *= 2
      }
    }
  }
  return ""
}

//
// set the value for a key.
// keeps trying until it succeeds.
//
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
  // You will have to modify this function.
  request_id := ck.get_request_id()
  args := PutArgs{}
  args.Key = key
  args.Value = value
  args.DoHash = dohash
  args.Request_id = request_id
  args.Delete_id = ck.delete_id
  reply := PutReply{}
  num_server := len(ck.servers)
  i := 0
  to := 25 * time.Millisecond
  for true{
    success := call(ck.servers[i],"KVPaxos.Put",&args,&reply)
    if success{
      ck.delete_id = request_id
      if reply.Err == OK{
        return reply.PreviousValue
      }else{
        return ""
      }
    }else{
      i = (i+1)%num_server
      time.Sleep(to)
      if to < 5 * time.Second {
        to *= 2
      }
    }
  }

  return ""
}

func (ck *Clerk) Put(key string, value string) {
  ck.PutExt(key, value, false)
}
func (ck *Clerk) PutHash(key string, value string) string {
  v := ck.PutExt(key, value, true)
  return v
}

func (ck *Clerk) get_request_id() string{
  ck.mu.Lock()
  defer ck.mu.Unlock()
  res := strconv.FormatInt(ck.clerk_id, 10)+"W"+strconv.FormatInt(ck.request_seq, 10)
  ck.request_seq ++
  return res
}
