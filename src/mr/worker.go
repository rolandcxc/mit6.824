package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type WorkerStatus int32

const (
	WorkerStatusIdle = 0
	WorkerStatusBusy = 1
)

type worker struct {
	id       int32
	status   WorkerStatus
	needWork chan struct{}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	w := &worker{
		id:       0,
		status:   WorkerStatusIdle,
		needWork: make(chan struct{}),
	}

	go func() {
		for {
			<-time.After(time.Second)
			w.HeartBeat()
		}
	}()

	for {
		<-w.needWork
		log.Printf("%d: need work", w.id)
		w.status = WorkerStatusBusy

		job := w.GetJob()
		log.Printf("Receive Job: %v, job: %v", job, job.Job)
		switch job.Job.Type {
		case JobMap:
			log.Println("start map job")
			w.doMap(job, mapf)
			log.Println("finish map job")
			w.JobSucceed(job.Job)

		case JobReduce:
			w.doReduce(job, reducef)
			w.JobSucceed(job.Job)
		}
		w.status = WorkerStatusIdle
	}

}

func (w *worker) doMap(job GetJobResp, mapf func(string, string) []KeyValue) {
	filename := job.Job.Key
	content, err := os.ReadFile(filename)
	if err != nil {
		log.Fatalf("cannot ReadFile %v", filename)
	}

	log.Println("finish ReadFile")
	kvs := mapf(filename, string(content))

	res := make(map[int][]KeyValue)
	for _, kv := range kvs {
		taskNum := ihash(kv.Key) % job.NReduce
		res[taskNum] = append(res[taskNum], kv)
	}

	log.Println("finish sort")
	for num, kva := range res {
		imPath := fmt.Sprintf("%s%d", job.ImBasePath, num)

		if err = os.MkdirAll(imPath, os.ModePerm); err != nil && !os.IsExist(err) {
			log.Fatalf("MkdirAll %s error: %s", imPath, err)
		}

		imFilename := fmt.Sprintf("%s/%d", imPath, job.Job.MapTaskNumber)
		f, err := ioutil.TempFile(imPath, "map_task_tmp_*")
		if err != nil {
			log.Fatalf("TempFile, error: %s", err)
		}
		encoder := json.NewEncoder(f)
		for _, kv := range kva {
			if err = encoder.Encode(&kv); err != nil {
				log.Fatalf("Encode: %v, error: %s", kv, err)
			}
		}

		if err = os.Rename(f.Name(), imFilename); err != nil {
			log.Fatalf("Rename: %v, error: %s", f.Name(), err)
		}
		f.Close()
		log.Println("finish " + imFilename)
	}
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

func (w *worker) doReduce(job GetJobResp, reducef func(string, []string) string) {
	imPath := fmt.Sprintf("%s%s", job.ImBasePath, job.Job.Key)
	intermediate := make([]KeyValue, 0)
	for i := 0; i < job.NMap; i++ {
		fname := fmt.Sprintf("%s/%d", imPath, i)
		f, err := os.Open(fname)
		if err != nil {
			//log.Fatalf("Open: %v, error: %s", fname, err)
			continue
		}

		decoder := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(intermediate))

	oname := fmt.Sprintf("mr-out-%s", job.Job.Key)
	ofile, err := ioutil.TempFile("./", "mr-out-tmp_*")
	if err != nil {
		log.Fatalf("TempFile, error: %s", err)
	}

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	if err = os.Rename(ofile.Name(), oname); err != nil {
		log.Fatalf("Rename: %v, error: %s", ofile.Name(), err)
	}
	ofile.Close()

}

func (w *worker) GetJob() GetJobResp {
	req := GetJobReq{
		WorkerID: w.id,
	}

	resp := GetJobResp{}

	call("Coordinator.GetJob", &req, &resp)

	return resp
}

func (w *worker) HeartBeat() {
	req := HeartBeatReq{
		WorkerID: w.id,
	}

	resp := HeartBeatResp{}

	call("Coordinator.HeartBeat", &req, &resp)

	if w.id == 0 {
		w.id = resp.WorkerID
	}

	if resp.HaveWork && w.status == WorkerStatusIdle {
		w.needWork <- struct{}{}
	}
}

func (w *worker) JobSucceed(job *Job) {
	req := JobSucceedReq{
		WorkerID: w.id,
		Job:      job,
	}

	resp := JobSucceedResp{}

	call("Coordinator.JobSucceed", &req, &resp)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	fmt.Println(err)
	return false
}
