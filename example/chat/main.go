package main

import (
	"bufio"
	"bytes"
	"encoding/gob"
	"encoding/json"
	"flag"
	"fmt"
	"github.com/treeforest/gossip"
	"github.com/treeforest/gossip/pb"
	log "github.com/treeforest/logger"
	"io"
	"os"
	"os/signal"
	"strings"
	"sync"
	"time"
)

type Message struct {
	Mid  int64
	From string
	Time time.Time
	Data string
}

func (m Message) Marshal() []byte {
	data, _ := json.Marshal(m)
	return data
}

func (m *Message) Unmarshal(data []byte) {
	_ = json.Unmarshal(data, m)
}

func (m *Message) Print() {
	fmt.Printf("%s\n\tfrom:%s\n\ttime:%s\n> ", m.Data, m.From, m.Time.Format("2006-01-02 15:04:05.99"))
}

type MessageCache struct {
	msgs   []*Message
	mp     map[int64]int
	locker sync.RWMutex
}

func (c *MessageCache) IDs() map[int64]struct{} {
	ids := make(map[int64]struct{})
	c.locker.RLock()
	for id, _ := range c.mp {
		ids[id] = struct{}{}
	}
	c.locker.RUnlock()
	return ids
}

func (c *MessageCache) Get(id int64) *Message {
	c.locker.RLock()
	m := c.msgs[c.mp[id]]
	c.locker.RUnlock()
	return m
}

func (c *MessageCache) Data() []byte {
	c.locker.RLock()
	data, _ := json.Marshal(c.msgs)
	c.locker.RUnlock()
	return data
}

func (c *MessageCache) Exist(mid int64) bool {
	c.locker.RLock()
	_, ok := c.mp[mid]
	c.locker.RUnlock()
	return ok
}

func (c *MessageCache) Add(msg *Message) {
	if c.Exist(msg.Mid) {
		return
	}
	c.locker.Lock()
	c.mp[msg.Mid] = len(c.msgs)
	c.msgs = append(c.msgs, msg)
	c.locker.Unlock()
}

type Chat struct {
	cache *MessageCache
	queue [][]byte
	mu    sync.Mutex
}

func (c *Chat) NotifyMsg(data []byte) {
	if len(data) == 0 {
		return
	}
	var msg Message
	msg.Unmarshal(data)
	c.cache.Add(&msg)
	// msg.Print()
	fmt.Printf("%s\n> ", msg.Data)
}

func (c *Chat) GetBroadcasts() [][]byte {
	c.mu.Lock()
	broadcasts := c.queue
	c.queue = c.queue[:0]
	c.mu.Unlock()
	return broadcasts
}

func (c *Chat) GetPullRequest() []byte {
	ids := c.cache.IDs()
	log.Debugf("local digest: %v", ids)
	buffer := bytes.NewBuffer(nil)
	_ = gob.NewEncoder(buffer).Encode(ids)
	return buffer.Bytes()
}

func (c *Chat) ProcessPullRequest(digest []byte) []byte {
	ids := map[int64]struct{}{}
	if digest != nil || len(digest) != 0 {
		_ = gob.NewDecoder(bytes.NewReader(digest)).Decode(&ids)
	}

	log.Debugf("local state, digest: %v", ids)

	localIds := c.cache.IDs()

	if len(ids) >= len(localIds) {
		return []byte{}
	}

	var diff []*Message
	for id, _ := range localIds {
		if _, ok := ids[id]; !ok {
			diff = append(diff, c.cache.Get(id))
		}
	}

	log.Debugf("diff: %v", diff)

	buf := bytes.NewBuffer(nil)
	_ = gob.NewEncoder(buf).Encode(diff)
	return buf.Bytes()
}

func (c *Chat) ProcessPullResponse(buf []byte) {
	if buf == nil || len(buf) == 0 {
		return
	}

	//time.Sleep(time.Millisecond * 300)
	msgs := make([]*Message, 0)
	_ = gob.NewDecoder(bytes.NewReader(buf)).Decode(&msgs)

	log.Debugf("merge remote state:%v", msgs)

	for _, msg := range msgs {
		if c.cache.Exist(msg.Mid) {
			continue
		}
		// msg.Print()
		fmt.Printf("%s\n> ", msg.Data)
		c.cache.Add(msg)
	}
}

// NotifyJoin 通知节点加入网络
func (c *Chat) NotifyJoin(member pb.Membership) {
	log.Infof("peer join %s %s", member.Endpoint, member.Id)
}

// NotifyLeave 通知节点离开网络
func (c *Chat) NotifyLeave(member pb.Membership) {
	log.Infof("peer leave %s %s", member.Endpoint, member.Id)
}

// NotifyUpdate 当节点的元数据发生改变时，通知节点更新
func (c *Chat) NotifyUpdate(member pb.Membership) {
	log.Infof("peer update %s %s", member.Endpoint, member.Id)
}

func (c *Chat) Send(nick, msg string) {
	m := Message{
		Mid:  time.Now().UnixNano(),
		From: nick,
		Time: time.Now(),
		Data: msg,
	}
	// fmt.Printf("\tfrom:%s\n\ttime:%s\n> ", m.From, m.Time.Format("2006-01-02 15:04:05.99"))
	fmt.Printf("> ")
	c.mu.Lock()
	c.queue = append(c.queue, m.Marshal())
	c.mu.Unlock()
	c.cache.Add(&m)
}

func main() {
	member := flag.String("member", "", "existing member")
	port := flag.Int("port", 0, "port")
	ip := flag.String("ip", "localhost", "enpoint ip")
	nick := flag.String("nick", "小李", "nick name")
	flag.Parse()

	log.SetLevel(log.INFO)

	chat := &Chat{
		cache: &MessageCache{
			msgs: make([]*Message, 0),
			mp:   make(map[int64]int),
		},
		queue: make([][]byte, 0),
	}

	conf := gossip.DefaultConfig()
	conf.Port = *port
	conf.Endpoint = fmt.Sprintf("%s:%d", *ip, *port)
	conf.Delegate = chat
	conf.EventDelegate = chat
	if len(*member) != 0 {
		conf.BootstrapPeers = strings.Split(*member, ",")
	}
	g := gossip.New(conf)

	fmt.Printf("> ")
	go func() {
		r := bufio.NewReader(os.Stdin)
		for {
			msg, err := r.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					break
				}
				panic(err)
			}
			if (len(msg) == 1 && (msg[0] == byte(13) || msg[0] == byte(10))) ||
				(len(msg) == 2 && msg[0] == byte(13) && msg[1] == byte(10)) {
				fmt.Print("> ")
				continue
			}
			chat.Send(*nick, string(msg[:len(msg)-2]))
		}
	}()

	done := make(chan os.Signal, 1)
	signal.Notify(done, os.Interrupt, os.Kill)
	<-done
	g.Stop()
	time.Sleep(time.Second)
}
