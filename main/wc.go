package main

import "os"
import "fmt"
import "mapreduce"
import "unicode"
import "strings"
import "container/list"
import "strconv"


// our simplified version of MapReduce does not supply a
// key to the Map function, as in the paper; only a value,
// which is a part of the input file contents
func Map(value string) *list.List {
  f := func(c rune) bool {
		return !unicode.IsLetter(c);
	}
  split_string := strings.FieldsFunc(value, f);
  res := list.New();
  for _,k := range split_string{
    temp := mapreduce.KeyValue{k,"1"};
    res.PushBack(temp);
  }
  return res;
}

// iterate over list and add values
func Reduce(key string, values *list.List) string {
  res := 0;
  for e := values.Front(); e != nil; e = e.Next() {
    v := e.Value.(string);
    temp, _ :=  strconv.Atoi(v);

    res += temp;
  }
  return strconv.Itoa(res);
}

// Can be run in 3 ways:
// 1) Sequential (e.g., go run wc.go master x.txt sequential)
// 2) Master (e.g., go run wc.go master x.txt localhost:7777)
// 3) Worker (e.g., go run wc.go worker localhost:7777 localhost:7778 &)
func main() {
  if len(os.Args) != 4 {
    fmt.Printf("%s: see usage comments in file\n", os.Args[0])
  } else if os.Args[1] == "master" {
    if os.Args[3] == "sequential" {
      mapreduce.RunSingle(5, 3, os.Args[2], Map, Reduce)
    } else {
      mr := mapreduce.MakeMapReduce(5, 3, os.Args[2], os.Args[3])
      // Wait until MR is done
      <- mr.DoneChannel
    }
  } else {
    mapreduce.RunWorker(os.Args[2], os.Args[3], Map, Reduce, 100)
  }
}
