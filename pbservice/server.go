package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "os"
import "syscall"
import "math/rand"
import "sync"
import "encoding/gob"
import "strconv"

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
  if Debug > 0 {
    n, err = fmt.Printf(format, a...)
  }
  return
}

type PBServer struct {
  l net.Listener
  dead bool // for testing
  unreliable bool // for testing
  me string
  vs *viewservice.Clerk
  done sync.WaitGroup
  finish chan interface{}
  // Your declarations here.
  cur_view viewservice.View
  cur_view_lock sync.Mutex
  database map[string]string
  put_get_lock sync.Mutex
  //done_map map[int64]bool
  done_args map[int64] interface{}
  copied bool

}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
  // Your code here.
  pb.put_get_lock.Lock()

  if val, ok := pb.done_args[args.RequestID]; ok {
    *reply = val.(PutReply)
    pb.put_get_lock.Unlock()
    return nil
  }
  call_res := false
  forward_args := ForwardArgs{true,args.DoHash,args.Key,args.Value,args.Viewnum,args.RequestID}
  forward_reply := ForwardReply{}
  for call_res == false{

    pb.cur_view_lock.Lock()
    cur_view := pb.cur_view
    pb.cur_view_lock.Unlock()
    if args.Viewnum==0 || cur_view.Viewnum!=args.Viewnum{
      reply.Err = ErrWrongServer
      pb.put_get_lock.Unlock()
      return nil
    }
    if cur_view.Backup == ""{
      if args.DoHash{
        old_value := pb.database[args.Key]// "" if key not exist
        h := hash(old_value + args.Value)
        put_value := strconv.Itoa(int(h))
        pb.database[args.Key] = put_value
        reply.Err = OK
        reply.PreviousValue = old_value
        pb.done_args[args.RequestID] = *reply
        pb.put_get_lock.Unlock()
        return nil
      }else{
        pb.database[args.Key] = args.Value
        reply.Err = OK
        pb.done_args[args.RequestID] = *reply
        pb.put_get_lock.Unlock()
        return nil
      }
    }else{
      forward_reply = ForwardReply{}
      call_res = call(cur_view.Backup,"PBServer.Forward",&forward_args,&forward_reply)
    }
  }
  //fmt.Print("In Put: ")
  //fmt.Println(forward_reply.Pass)
  if forward_reply.Pass == false{
    reply.Err = ErrWrongServer
    pb.put_get_lock.Unlock()
    return nil
  }else{//Success
    if args.DoHash{
      old_value := pb.database[args.Key]// "" if key not exist
      h := hash(old_value + args.Value)
      put_value := strconv.Itoa(int(h))
      pb.database[args.Key] = put_value
      reply.Err = OK
      reply.PreviousValue = old_value
      pb.done_args[args.RequestID] = *reply
      pb.put_get_lock.Unlock()
      return nil
    }else{
      pb.database[args.Key] = args.Value
      reply.Err = OK
      pb.done_args[args.RequestID] = *reply
      pb.put_get_lock.Unlock()
      return nil
    }
  }
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
  // Your code here.
  pb.put_get_lock.Lock()
  //check if duplicates
  if val, ok := pb.done_args[args.RequestID]; ok {
    *reply = val.(GetReply)
    pb.put_get_lock.Unlock()
    return nil
  }
  /*
  pb.cur_view_lock.Lock()
  cur_view := pb.cur_view
  pb.cur_view_lock.Unlock()
  if args.Viewnum==0 || cur_view.Viewnum!=args.Viewnum{
    reply.Err = ErrWrongServer
    pb.put_get_lock.Unlock()
    return nil
  }
  */
  //maybe need to compare viewnum with primary's view
  call_res := false
  forward_args := ForwardArgs{false,false,args.Key,"",args.Viewnum,args.RequestID}
  forward_reply := ForwardReply{}
  for call_res==false{
    pb.cur_view_lock.Lock()
    cur_view := pb.cur_view
    pb.cur_view_lock.Unlock()
    if args.Viewnum==0 || cur_view.Viewnum!=args.Viewnum{
      reply.Err = ErrWrongServer
      pb.put_get_lock.Unlock()
      return nil
    }
    if cur_view.Backup==""{
      if val,ok := pb.database[args.Key]; ok{
        pb.done_args[args.RequestID] = GetReply{OK,val}
      }else{
        pb.done_args[args.RequestID] = GetReply{ErrNoKey,""}
      }
      *reply = pb.done_args[args.RequestID].(GetReply)
      pb.put_get_lock.Unlock()
      return nil
    }else{
      forward_reply = ForwardReply{}
      call_res = call(cur_view.Backup,"PBServer.Forward",&forward_args,&forward_reply)
    }
  }
  if forward_reply.Pass == false{
    reply.Err = ErrWrongServer
    pb.put_get_lock.Unlock()
    return nil
  }else{
    //sucess
    if val,ok := pb.database[args.Key]; ok{
      pb.done_args[args.RequestID] = GetReply{OK,val}
    }else{
      pb.done_args[args.RequestID] = GetReply{ErrNoKey,""}
    }
    *reply = pb.done_args[args.RequestID].(GetReply)
    pb.put_get_lock.Unlock()
    return nil
  }
  //check cur_view
  //get backup view. Check.
  //or simply forward the request to backup
}

func (pb *PBServer) Forward(args *ForwardArgs, reply *ForwardReply) error{
  pb.put_get_lock.Lock()
  if _, ok := pb.done_args[args.RequestID]; ok {
    reply.Pass = true
    //fmt.Print("In Forward: ")
    //fmt.Println(reply.Pass)
    pb.put_get_lock.Unlock()
    return nil
  }
  if !args.IsPut{
    pb.cur_view_lock.Lock()
    cur_view := pb.cur_view
    pb.cur_view_lock.Unlock()
    //fmt.Print("In forward: ")
    //fmt.Println(cur_view)
    reply.Pass = (cur_view.Viewnum == args.Viewnum)

    if reply.Pass{
      if val,ok := pb.database[args.Key]; ok{
        pb.done_args[args.RequestID] = GetReply{OK,val}
      }else{
        pb.done_args[args.RequestID] = GetReply{ErrNoKey,""}
      }
    }
    pb.put_get_lock.Unlock()
    return nil
  }else{
    //Put
    pb.cur_view_lock.Lock()
    cur_view := pb.cur_view
    pb.cur_view_lock.Unlock()
    //fmt.Print("In forward: ")
    //fmt.Println(cur_view)
    reply.Pass = (cur_view.Viewnum == args.Viewnum)
    //fmt.Print("In forward: ")
    //fmt.Println(reply.Pass)
    if reply.Pass{
      if args.DoHash{
        old_value := pb.database[args.Key]// "" if key not exist
        h := hash(old_value + args.Value)
        put_value := strconv.Itoa(int(h))
        pb.database[args.Key] = put_value
        pb.done_args[args.RequestID] = PutReply{OK,old_value}
        pb.put_get_lock.Unlock()
        return nil
      }else{
        pb.database[args.Key] = args.Value
        pb.done_args[args.RequestID] = PutReply{OK,""}
        pb.put_get_lock.Unlock()
        return nil
      }
    }

    pb.put_get_lock.Unlock()
    return nil
  }

}

func (pb *PBServer) Copy(args *CopyArgs, reply *CopyReply) error{
  pb.put_get_lock.Lock()
  if pb.copied{
    pb.put_get_lock.Unlock()
    return nil
  }else{
    pb.database = args.Database
    pb.done_args = args.Done_args
    pb.copied = true
    pb.put_get_lock.Unlock()
    return nil
  }
}

// ping the viewserver periodically.
func (pb *PBServer) tick() {
  // Your code here:
  pb.cur_view_lock.Lock()
  old_view := pb.cur_view
  for true{
    new_view,err := pb.vs.Ping(pb.cur_view.Viewnum)
    if err==nil{
      pb.cur_view = new_view
      break
    }
  }
  //fmt.Print("In tick:cur_view: ")
  //fmt.Println(pb.cur_view)
  cur_view := pb.cur_view
  pb.cur_view_lock.Unlock()
  if cur_view.Primary==pb.me && cur_view.Backup!="" &&
     cur_view.Viewnum != old_view.Viewnum{
       //copy everything to the backup!!
       // Maybe in a new thread, holding write clock to the Backup
       go func(){
         temp_view := cur_view // temp_view is the view to

         pb.cur_view_lock.Lock()
         cur_view = pb.cur_view
         pb.cur_view_lock.Unlock()

         pb.put_get_lock.Lock()
         args := CopyArgs{pb.database,pb.done_args}
         call_res := false
         for !call_res&&temp_view.Viewnum == cur_view.Viewnum{
           reply := CopyReply{}
           call_res = call(cur_view.Backup,"PBServer.Copy",&args,&reply)
           pb.cur_view_lock.Lock()
           cur_view = pb.cur_view
           pb.cur_view_lock.Unlock()
         }
         pb.put_get_lock.Unlock()
         //fmt.Println("Copy Done")
       }()
     }

}


// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
  pb.dead = true
  pb.l.Close()
}


func StartServer(vshost string, me string) *PBServer {
  pb := new(PBServer)
  pb.me = me
  pb.vs = viewservice.MakeClerk(me, vshost)
  pb.finish = make(chan interface{})
  // Your pb.* initializations here.
  pb.cur_view = viewservice.View{}
  pb.database = make(map[string]string)
  pb.done_args = make(map[int64]interface{})
  pb.copied = false
  gob.Register(PutReply{})
  gob.Register(GetReply{})

  rpcs := rpc.NewServer()
  rpcs.Register(pb)

  os.Remove(pb.me)
  l, e := net.Listen("unix", pb.me);
  if e != nil {
    log.Fatal("listen error: ", e);
  }
  pb.l = l

  // please do not change any of the following code,
  // or do anything to subvert it.

  go func() {
    for pb.dead == false {
      conn, err := pb.l.Accept()
      if err == nil && pb.dead == false {
        if pb.unreliable && (rand.Int63() % 1000) < 100 {
          // discard the request.
          conn.Close()
        } else if pb.unreliable && (rand.Int63() % 1000) < 200 {
          // process the request but force discard of reply.
          c1 := conn.(*net.UnixConn)
          f, _ := c1.File()
          err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
          if err != nil {
            fmt.Printf("shutdown: %v\n", err)
          }
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        } else {
          pb.done.Add(1)
          go func() {
            rpcs.ServeConn(conn)
            pb.done.Done()
          }()
        }
      } else if err == nil {
        conn.Close()
      }
      if err != nil && pb.dead == false {
        fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
        pb.kill()
      }
    }
    DPrintf("%s: wait until all request are done\n", pb.me)
    pb.done.Wait()
    // If you have an additional thread in your solution, you could
    // have it read to the finish channel to hear when to terminate.
    close(pb.finish)
  }()

  pb.done.Add(1)
  go func() {
    for pb.dead == false {
      pb.tick()
      time.Sleep(viewservice.PingInterval)
    }
    pb.done.Done()
  }()

  return pb
}
