package main

import (
	"bytes"
	"encoding/gob"
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

type Server struct {
	cache *bigcache.BigCache
	queue [][]byte
	mu    sync.Mutex
}

func (s *Server) NotifyJoin(m pb.Membership) {
	log.Infof("node join -> addr[%s] id[%s]", m.Endpoint, m.Id)
}

func (s *Server) NotifyLeave(m pb.Membership) {
	log.Infof("node leave -> addr[%s] id[%s]", m.Endpoint, m.Id)
}

func (s *Server) NotifyUpdate(m pb.Membership) {
	log.Infof("node update -> addr[%s] id[%s] Meta[%s]", m.Endpoint, m.Id, m.Metadata)
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

func (s *Server) GetPullRequest() []byte {
	mp := make(map[string]struct{})
	it := s.cache.Iterator()
	for it.SetNext() {
		e, _ := it.Value()
		mp[e.Key()] = struct{}{}
	}
	buf := bytes.NewBuffer(nil)
	_ = gob.NewEncoder(buf).Encode(mp)
	return buf.Bytes()
}

func (s *Server) ProcessPullRequest(summary []byte) []byte {
	mp := make(map[string]struct{})
	if summary != nil {
		_ = gob.NewDecoder(bytes.NewReader(summary)).Decode(&mp)
	}

	state := make(map[string][]byte)
	it := s.cache.Iterator()
	for it.SetNext() {
		e, _ := it.Value()
		if _, ok := mp[e.Key()]; ok {
			continue
		}
		state[e.Key()] = e.Value()
	}

	buf := bytes.NewBuffer(nil)
	_ = gob.NewEncoder(buf).Encode(state)
	return buf.Bytes()
}

func (s *Server) ProcessPullResponse(buf []byte) {
	state := make(map[string][]byte)
	_ = gob.NewDecoder(bytes.NewReader(buf)).Decode(&state)
	for k, v := range state {
		if _, err := s.cache.Get(k); err == bigcache.ErrEntryNotFound {
			_ = s.cache.Set(k, v)
		}
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
	endpoint := flag.String("endpoint", "", "exposed endpoint")
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
	conf.Port = *port
	conf.Endpoint = *endpoint
	conf.Delegate = s
	conf.EventDelegate = s
	conf.BootstrapPeers = strings.Split(*member, ",")

	g := gossip.New(conf)

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	g.Stop()
	time.Sleep(time.Second)
}
