package mr

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

type WorkerInfo struct {
	id     int32
	lastHB int64
	job    *Job
}

func (w *WorkerInfo) takeJob(j *Job) {
	w.job = j
}

func (w *WorkerInfo) commitJob() {
	w.job = nil
}

type Coordinator struct {
	// Your definitions here.
	IdGenerator
	nMap       int
	nReduce    int
	imBasePath string
	mapLock    sync.Mutex
	pendingM   []*Job
	scheduleM  map[string]*Job
	pendingR   []*Job
	scheduleR  map[string]*Job
	workers    map[int32]*WorkerInfo //保存被分配了任务的worker
	done       bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) HeartBeat(req *HeartBeatReq, resp *HeartBeatResp) error {
	//log.Printf("Receive RPC HeartBeat: %v", *req)

	c.mapLock.Lock()
	defer c.mapLock.Unlock()

	var w *WorkerInfo
	if req.WorkerID == 0 {
		w = &WorkerInfo{
			id: c.getId(),
		}

		c.workers[w.id] = w
	} else {
		w = c.workers[req.WorkerID]
	}

	w.lastHB = time.Now().Unix()

	resp.WorkerID = w.id
	if len(c.pendingM) > 0 || len(c.pendingR) > 0 {
		resp.HaveWork = true
	}

	return nil
}

func (c *Coordinator) GetJob(req *GetJobReq, resp *GetJobResp) error {
	log.Printf("Receive RPC GetJob: %v", *req)
	c.mapLock.Lock()
	defer c.mapLock.Unlock()

	wi, exist := c.workers[req.WorkerID]
	if !exist {
		return nil
	}

	// if len(c.pendingM) == 0 && len(c.pendingR) == 0 {
	// 	// TODO
	// 	resp.Job = &Job{
	// 		Type: JobNone,
	// 	}

	// 	return nil
	// }

	var job *Job
	if len(c.pendingM) > 0 {
		job = c.pendingM[0]
		c.pendingM = c.pendingM[1:]

		c.scheduleM[job.Key] = job
	}

	if len(c.pendingR) > 0 {
		job = c.pendingR[0]
		c.pendingR = c.pendingR[1:]

		c.scheduleR[job.Key] = job
	}

	if job == nil && len(c.scheduleM) > 0 {
		// TODO
		for _, j := range c.scheduleM {
			// 超额分配
			job = j
			break
		}
	}

	if job == nil && len(c.scheduleR) > 0 {
		for _, j := range c.scheduleR {
			// 超额分配
			job = j
			break
		}
	}

	if job == nil {
		resp.Job = &Job{
			Type: JobNone,
		}

		return nil
	}
	wi.takeJob(job)

	resp.Job = job
	resp.NMap = c.nMap
	resp.ImBasePath = c.imBasePath
	resp.NReduce = c.nReduce

	return nil
}

func (c *Coordinator) JobSucceed(req *JobSucceedReq, resp *JobSucceedResp) error {
	log.Printf("Receive RPC JobSucceed, Worker: %d, Job: %+v", req.WorkerID, *req.Job)

	c.mapLock.Lock()
	defer c.mapLock.Unlock()

	job := req.Job
	w := c.workers[req.WorkerID]
	if w.job == nil || w.job.Key != job.Key {
		log.Printf("worker 提交的任务已经分配给别人")
		// TODO worker提交的任务和记录的发给它的任务不一样
	}

	w.commitJob()
	switch job.Type {
	case JobMap:
		_, exist := c.scheduleM[job.Key]
		if !exist {
			// 该任务已经被其它worker执行完成
			log.Printf("该任务已经被其它worker提交， %+v", *job)
			return nil
		}
		delete(c.scheduleM, job.Key)

		if len(c.pendingM) > 0 || len(c.scheduleM) > 0 {
			return nil
		}
		// Map任务结束
		log.Printf("map jobs all completed")
		for i := 0; i < c.nReduce; i++ {
			key := strconv.Itoa(i)
			c.pendingR = append(c.pendingR, &Job{
				Type: JobReduce,
				Key:  key,
			})
		}
		return nil
	case JobReduce:
		_, exist := c.scheduleR[job.Key]
		if !exist {
			return nil
		}

		delete(c.scheduleR, job.Key)

		if len(c.pendingR) > 0 || len(c.scheduleR) > 0 {
			return nil
		}
		// Reduce任务结束
		c.done = true
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	if err := os.RemoveAll(c.imBasePath); err != nil {
		log.Fatalf("remove %s error: %s", c.imBasePath, err)
	}
	if err := os.Mkdir(c.imBasePath, os.ModePerm); err != nil {
		log.Fatalf("mkdir %s error: %s", c.imBasePath, err)
	}

	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		IdGenerator: IdGenerator{},
		nMap:        len(files),
		nReduce:     nReduce,
		imBasePath:  "im/",
		pendingM:    make([]*Job, 0),
		scheduleM:   make(map[string]*Job),
		pendingR:    make([]*Job, 0),
		scheduleR:   make(map[string]*Job),
		workers:     make(map[int32]*WorkerInfo),
	}

	for i, f := range files {
		c.pendingM = append(c.pendingM, &Job{
			Type:          JobMap,
			Key:           f,
			MapTaskNumber: i,
		})
	}

	marshal, _ := json.Marshal(c.pendingM)
	fmt.Printf("Map Jobs: %s\n", string(marshal))
	// Your code here.

	go func() {
		for {
			<-time.After(time.Second)
			c.checkWorkers()
		}
	}()

	log.Printf("start coordinator")
	c.server()
	return &c
}

func (c *Coordinator) checkWorkers() {
	c.mapLock.Lock()
	defer c.mapLock.Unlock()
	now := time.Now().Unix()

	for _, w := range c.workers {
		if now-w.lastHB < 3 {
			continue
		}
		// 超过3秒未HeartBeat
		if w.job != nil {
			c.recycleJob(w.job)
			w.job = nil
		}
	}
}

func (c *Coordinator) recycleJob(job *Job) {
	switch job.Type {
	case JobMap:
		c.pendingM = append(c.pendingM, job)
		delete(c.scheduleM, job.Key)
	case JobReduce:
		c.pendingR = append(c.pendingR, job)
		delete(c.scheduleR, job.Key)
	}
}

type JobType int32

const (
	JobNone   = 0
	JobMap    = 1
	JobReduce = 2
)

func (j JobType) String() string {
	switch j {
	case JobNone:
		return "JobNone"
	case JobMap:
		return "JobMap"
	case JobReduce:
		return "JobReduce"
	}
	return ""
}

type Job struct {
	Type          JobType
	Key           string
	MapTaskNumber int
}

type IdGenerator struct {
	mu    sync.Mutex
	maxId int32
}

func (g *IdGenerator) getId() int32 {
	// TODO
	g.mu.Lock()
	defer g.mu.Unlock()

	g.maxId = (g.maxId + 1) % 1024
	return g.maxId
}
