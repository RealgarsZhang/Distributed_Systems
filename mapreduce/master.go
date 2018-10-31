package mapreduce
import "container/list"
import "fmt"
import "sync"
//import "strconv"
var Map_Lock sync.Mutex

type WorkerInfo struct {
  address string
  // You can add definitions here.
}

func (mr *MapReduce) FindWorker() (bool,string){
  available_worker := ""
  worker_found := false
  Map_Lock.Lock()
  for k,v :=range mr.WorkerAvailable{
    if v{
      available_worker = k
      worker_found = true
      mr.WorkerAvailable[k] = false
      break
    }
  }
  Map_Lock.Unlock()
  return worker_found, available_worker

}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
  l := list.New()
  for _, w := range mr.Workers {
    DPrintf("DoWork: shutdown %s\n", w.address)
    args := &ShutdownArgs{}
    var reply ShutdownReply;
    ok := call(w.address, "Worker.Shutdown", args, &reply)
    if ok == false {
      fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
    } else {
      l.PushBack(reply.Njobs)
    }
  }
  return l
}

func (mr *MapReduce) RunMaster() *list.List {
  // Your code here
  go func(){
    for true{
      new_worker := <- mr.registerChannel
      Map_Lock.Lock()
      mr.WorkerAvailable[new_worker] = true //Maybe need to lock the set?
      Map_Lock.Unlock()
      mr.Workers[new_worker] = &WorkerInfo{new_worker}
    }
  }()

  for mr.MapStarted<mr.nMap{
    /*
    var available_worker string
    worker_found := false
    Map_Lock.Lock()
    for k,v :=range mr.WorkerAvailable{
      if v{
        available_worker = k
        worker_found = true
        mr.WorkerAvailable[k] = false
        break
      }
    }
    Map_Lock.Unlock()
    */
    worker_found,available_worker:=mr.FindWorker()

    if worker_found{
//      Map_Lock.Lock()
//      mr.WorkerAvailable[available_worker] = false
//      Map_Lock.Unlock()
      mr.MapStarted += 1
      go func(){
          available_worker := <- mr.WorkerAssignmentChannel
          job_num := <- mr.JobNumberChannel
          args := &DoJobArgs{mr.file,Map,job_num,mr.nReduce}
          var reply DoJobReply;
          ok := call(available_worker,"Worker.DoJob",args,&reply)
          for !ok{
            worker_found := false
            for !worker_found{
              worker_found,available_worker = mr.FindWorker()
            }
            ok = call(available_worker,"Worker.DoJob",args,&reply)
          }
          Map_Lock.Lock()
          mr.WorkerAvailable[available_worker] = true
          Map_Lock.Unlock()
          mr.PhaseFinishChannel <- 1
      }()
      mr.WorkerAssignmentChannel <- available_worker
      mr.JobNumberChannel <- mr.MapStarted-1
    }

  }

  for i := 0; i<mr.nMap;i++{
    <-mr.PhaseFinishChannel
  }

  for mr.ReduceStarted<mr.nReduce{

    worker_found, available_worker := mr.FindWorker()
    /*
    var available_worker string
    worker_found := false
    Map_Lock.Lock()
    for k,v :=range mr.WorkerAvailable{
      if v{
        available_worker = k
        worker_found = true
        break
      }
    }
    Map_Lock.Unlock()
    */

    if worker_found{
      /*
      Map_Lock.Lock()
      mr.WorkerAvailable[available_worker] = false
      Map_Lock.Unlock()
      */
      mr.ReduceStarted += 1
      go func(){
          available_worker := <- mr.WorkerAssignmentChannel
          job_num := <- mr.JobNumberChannel
          args := &DoJobArgs{mr.file,Reduce,job_num,mr.nMap}
          var reply DoJobReply;
          ok := call(available_worker,"Worker.DoJob",args,&reply)
          for !ok{
            worker_found := false
            for !worker_found{
              worker_found,available_worker = mr.FindWorker()
            }
            ok = call(available_worker,"Worker.DoJob",args,&reply)
          }
          Map_Lock.Lock()
          mr.WorkerAvailable[available_worker] = true
          Map_Lock.Unlock()
          mr.PhaseFinishChannel <- 1
      }()
      mr.WorkerAssignmentChannel <- available_worker
      mr.JobNumberChannel <- mr.ReduceStarted-1
    }

  }
  for i := 0; i<mr.nReduce;i++{
    <-mr.PhaseFinishChannel
  }




  return mr.KillWorkers()
}
