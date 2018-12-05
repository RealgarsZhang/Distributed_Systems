package shardkv
import "hash/fnv"
import "crypto/rand"
import "math/big"
import "shardmaster"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
  OK = "OK"
  ErrNoKey = "ErrNoKey"
  ErrWrongGroup = "ErrWrongGroup"
  ErrConfiguring = "ErrConfiguring"
)
type Err string

type PutArgs struct {
  Key string
  Value string
  DoHash bool  // For PutHash
  // You'll have to add definitions here.
  // Field names must start with capital letters,
  // otherwise RPC will break.
  Request_id string
}

type PutReply struct {
  Err Err
  PreviousValue string   // For PutHash
}

type GetArgs struct {
  Key string
  // You'll have to add definitions here.
  Request_id string
}

type GetReply struct {
  Err Err
  Value string
}

type ReceiveArgs struct{
  Request_id string
  Database [shardmaster.NShards]map[string]string
  Handled_replies [shardmaster.NShards]map[string]interface{}
  Shards_sent [shardmaster.NShards]bool
  Cnum int
  GID int64
}

type ReceiveReply struct{
  Received bool
}

func hash(s string) uint32 {
  h := fnv.New32a()
  h.Write([]byte(s))
  return h.Sum32()
}

func nrand() int64 {
  max := big.NewInt(int64(1) << 62)
  bigx, _ := rand.Int(rand.Reader, max)
  x := bigx.Int64()
  return x
}
