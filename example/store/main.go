package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"github.com/allegro/bigcache/v3"
	"github.com/gin-gonic/gin"
	"github.com/treeforest/gossip"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

const (
	SET = iota
	DEL
)

type Operate struct {
	Code int
	Key  string
	Val  string
}

func (o Operate) Marshal() []byte {
	data, _ := json.Marshal(o)
	return data
}

func (o *Operate) Unmarshal(data []byte) {
	_ = json.Unmarshal(data, o)
}

type eventDelegate struct{}

func (d *eventDelegate) NotifyJoin(n *pb.Node) {
	log.Infof("node join -> addr[%s] id[%s]", n.FullAddress(), n.Id)
}

func (d *eventDelegate) NotifyLeave(n *pb.Node) {
	log.Infof("node leave -> addr[%s] id[%s]", n.FullAddress(), n.Id)
}

func (d *eventDelegate) NotifyUpdate(n *pb.Node) {
	log.Infof("node update -> addr[%s] id[%s] Meta[%s]", n.FullAddress(), n.Id, n.Meta)
}

type Server struct {
	cache *bigcache.BigCache
	queue [][]byte
	mu    sync.Mutex
}

func (s *Server) NotifyMsg(data []byte) {
	if len(data) == 0 {
		return
	}
	var op Operate
	op.Unmarshal(data)
	switch op.Code {
	case SET:
		_ = s.cache.Set(op.Key, []byte(op.Val))
		log.Infof("set key=%s val=%s", op.Key, op.Val)
	case DEL:
		_ = s.cache.Delete(op.Key)
		log.Infof("del key=%s", op.Key)
	}
}

func (s *Server) GetBroadcasts() [][]byte {
	s.mu.Lock()
	msgs := s.queue
	s.queue = s.queue[:0]
	s.mu.Unlock()
	return msgs
}

func (s *Server) LocalState(join bool) []byte {
	mp := make(map[string]string)
	it := s.cache.Iterator()
	for it.SetNext() {
		e, _ := it.Value()
		mp[e.Key()] = string(e.Value())
	}

	if len(mp) == 0 {
		return []byte{}
	}
	data, _ := json.Marshal(mp)
	return data
}

func (s *Server) MergeRemoteState(buf []byte, join bool) {
	if len(buf) == 0 {
		return
	}

	mp := make(map[string]string)
	_ = json.Unmarshal(buf, &mp)
	for k, v := range mp {
		_ = s.cache.Set(k, []byte(v))
	}
}

func (s *Server) Broadcasts(data []byte) {
	if len(s.queue) > 32 {
		return
	}
	s.queue = append(s.queue, data)
}

func (s *Server) Run(addr string) {
	r := gin.Default()
	r.POST("/set", func(c *gin.Context) {
		key := c.Query("key")
		val := c.Query("val")
		_ = s.cache.Set(key, []byte(val))
		s.queue = append(s.queue, Operate{Code: SET, Key: key, Val: val}.Marshal())
		c.JSON(http.StatusOK, gin.H{"code": 0})
	})
	r.GET("/get", func(c *gin.Context) {
		key := c.Query("key")
		v, err := s.cache.Get(key)
		if err != nil {
			if err == bigcache.ErrEntryNotFound {
				c.JSON(http.StatusOK, gin.H{"code": 1, "message": "not found"})
			} else {
				c.Status(http.StatusInternalServerError)
			}
			return
		}
		c.JSON(http.StatusOK, gin.H{"code": 0, "value": string(v)})
	})
	r.POST("/del", func(c *gin.Context) {
		key := c.Query("key")
		_ = s.cache.Delete(key)
		s.queue = append(s.queue, Operate{Code: DEL, Key: key}.Marshal())
		c.JSON(http.StatusOK, gin.H{"code": 0})
	})
	log.Fatal(r.Run(addr))
}

func main() {
	member := flag.String("member", "", "existing member")
	port := flag.Int("port", 0, "listen port")
	flag.Parse()

	log.SetLevel(log.INFO)

	cache, _ := bigcache.NewBigCache(bigcache.DefaultConfig(24 * time.Hour))

	s := &Server{
		cache: cache,
		queue: make([][]byte, 0),
	}
	go s.Run(fmt.Sprintf("0.0.0.0:%d", *port+1))
	time.Sleep(time.Millisecond * 500)

	conf := gossip.DefaultConfig()
	conf.BindAddress = "localhost"
	conf.BindPort = int32(*port)
	conf.Delegate = s
	conf.EventDelegate = &eventDelegate{}
	conf.GossipNodes = 3

	g := gossip.New(conf)
	if len(*member) != 0 {
		g.Join(strings.Split(*member, ","))
	}

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	g.Close()
	time.Sleep(time.Second)
}
