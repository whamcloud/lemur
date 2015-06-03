/**

Parallel Data Mover is scalable system to copy or migrate data between
various storage systems. It supports multliple types of sources and
destinations, including POSIX, S3, HPSS, etc.

Use cases include:
  * Data movement for Lustre HSM.
  * Offsite replication for DR
  * Lustre file-level replication
  * Storage rebalancing within a single tier
  * Migration between filesytems (e.g GPFS - > Lustre)

Initially the main focus is for HSM.
*/

package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/garyburd/redigo/redis"
	"github.com/golang/glog"
	"github.intel.com/hpdd/lustre/fs"
	"github.intel.com/hpdd/policy/pdm"
	"github.intel.com/hpdd/policy/pkg/mq"
)

type (
	// Mover for a single filesytem and a collection of backends.
	Mover struct {
		root     fs.RootDir
		requestQ mq.Receiver
		replyQ   mq.Sender
		backends map[uint]Backend
		wg       sync.WaitGroup
	}
)

func (mover *Mover) initBackends(conf *pdm.HSMConfig) error {
	mover.backends = make(map[uint]Backend, 0)
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}
	for _, a := range conf.Archives {
		glog.V(3).Info(a)
		switch a.Type {
		case "posix":
			//			{
			//				mover.backends[a.ArchiveID] = NewPosixBackend(root, a.PosixDir, a.SnapshotsEnabled)
			//			}
		// case "mirror":
		// 	{
		// 		mover.backends[a.ArchiveID] = NewS3MirrorBackend(root)
		// 	}
		// case "s3":
		// 	{
		// 		mover.backends[a.ArchiveID] = NewS3Backend(root, a.S3Url)
		// 	}
		default:
			{
				mover.backends[a.ArchiveID] = NewNoopBackend(root)
			}
		}
		glog.Infof("created: %d %s", a.ArchiveID, mover.backends[a.ArchiveID])

	}
	return nil
}

func (mover *Mover) Worker() {
	for {
		var r pdm.Request
		err := mover.requestQ.Get(&r)
		if err != nil && err != redis.ErrNil {
			panic(err)
		}
		var backend Backend
		result, err := handleAction(backend, &r)
		log.Printf("received: %v %v\n", r, err)

		mover.replyQ.Put(result)
	}
}

func (mover *Mover) addWorker() {
	mover.wg.Add(1)
	go func() {
		defer mover.wg.Done()
		mover.Worker()
	}()
}

func mover(conf *pdm.HSMConfig, pool *redis.Pool) {
	requestQName := "pdm:request" // get from config
	replyQName := "pdm:reply"     // should be specific to this agent
	root, err := fs.MountRoot(conf.Lustre)
	if err != nil {
		glog.Fatal(err)
	}

	done := make(chan struct{})
	mover := &Mover{
		root:     root,
		requestQ: mq.RedisMQ(pool, requestQName),
		replyQ:   mq.RedisMQSender(pool, replyQName),
	}

	interruptHandler(func() {
		close(done)
		os.Exit(0)
		//		mover.Stop()
	})

	go func() {
		if err != nil {
			log.Fatal(err)
			return
		}

		for i := 0; i < conf.Processes; i++ {
			mover.addWorker()
		}
	}()

	<-done
	mover.wg.Wait()
}

func newPool(server, password string, logger *log.Logger) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if logger != nil {
				c = redis.NewLoggingConn(c, logger, "redis")
			}
			if password != "" {
				if _, err := c.Do("AUTH", password); err != nil {
					c.Close()
					return nil, err
				}
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			_, err := c.Do("PING")
			return err
		},
	}
}

var (
	reset = false
	trace = false
)

func init() {
	flag.BoolVar(&reset, "reset", false, "Reset queue")
	flag.BoolVar(&trace, "trace", false, "Print redis trace")
}

func main() {
	flag.Parse()
	server := ":6379"
	password := ""
	requestQName := "pdm:request" // get from config
	replyQName := "pdm:reply"     // should be specific to this agent

	log.SetFlags(log.LstdFlags | log.Lshortfile)

	var logger *log.Logger
	if trace {
		logger = log.New(os.Stderr, "", log.LstdFlags)
	}
	pool := newPool(server, password, logger)

	if reset {
		mq.RedisMQReset(pool, requestQName)
		mq.RedisMQReset(pool, replyQName)
	}

	defer glog.Flush()
	conf := pdm.ConfigInitMust()

	mover(conf, pool)

	// go sender(mq.RedisMQSender(pool, queueName))

	// q := mq.RedisMQ(pool, queueName)
	// for i := 0; i < ReceiverCount; i++ {
	// 	name := fmt.Sprintf("recv%d", i)
	// 	go receiver(name, q)
	// }
}
func interruptHandler(once func()) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt, syscall.SIGQUIT, syscall.SIGTERM)

	go func() {
		stopping := false
		for sig := range c {
			glog.Infoln("signal received:", sig)
			if !stopping {
				stopping = true
				once()
			}
		}
	}()

}
