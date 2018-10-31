package pbservice

import "hash/fnv"
//import "viewservice"
const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongServer = "ErrWrongServer"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool // For PutHash
  // You'll have to add definitions here.
  Viewnum uint
  RequestID int64
  // Field names must start with capital letters,
  // otherwise RPC will break.
}

type PutReply struct {
  Err Err
  PreviousValue string // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Viewnum uint
  RequestID int64
}

type GetReply struct {
  Err Err
  Value string
}

type CopyArgs struct {
  Database map[string]string
  Done_args map[int64]interface{}
}

type CopyReply struct{

}

type ForwardArgs struct{
  IsPut bool
  DoHash bool
  Key string
  Value string
  Viewnum uint
  RequestID int64
}

type ForwardReply struct{
  Err Err
  Pass bool
}


// Your RPC definitions here.

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}
