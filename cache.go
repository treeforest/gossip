package gossip

import (
	"container/list"
	"github.com/google/uuid"
	"sync"
	"time"
)

// LRUCache 消息id的过期+lru缓存
type LRUCache struct {
	Cap    int
	TTL    time.Duration
	cache  map[string]*list.Element
	ll     list.List
	locker sync.RWMutex
}

type entry struct {
	key    string
	expire time.Time
}

func NewCache(cap int, ttl time.Duration) *LRUCache {
	c := &LRUCache{
		Cap:    cap,
		TTL:    ttl,
		ll:     list.List{},
		cache:  make(map[string]*list.Element),
		locker: sync.RWMutex{},
	}
	go c.trigger()
	return c
}

func (c *LRUCache) Set(key string) {
	c.locker.Lock()

	element, exist := c.cache[key]
	if !exist {
		element = c.ll.PushFront(&entry{key: key, expire: time.Now().Add(c.TTL)})
		c.cache[key] = element
	} else {
		// 更新过期时间
		element.Value.(*entry).expire = time.Now().Add(c.TTL)
		// 移到队首
		c.ll.MoveToFront(element)
	}

	c.locker.Unlock()

	c.locker.RLock()
	length := c.ll.Len()
	c.locker.RUnlock()

	if c.Cap > 0 && length > c.Cap {
		c.removeOldest()
		return
	}
}

func (c *LRUCache) NewID() string {
	id := uuid.NewString()
	c.Set(id)
	return id
}

func (c *LRUCache) Has(key string) bool {
	c.locker.RLock()
	_, exist := c.cache[key]
	c.locker.RUnlock()
	return exist
}

func (c *LRUCache) removeOldest() {
	c.locker.Lock()
	el := c.ll.Back()
	if el == nil {
		c.locker.Unlock()
		return
	}
	c.ll.Remove(el)
	e := el.Value.(*entry)
	delete(c.cache, e.key)
	c.locker.Unlock()
}

func (c *LRUCache) trigger() {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-t.C:
			expired := map[string]*list.Element{}

			// 从后往前检查：越接近过期时间的越靠后
			for {
				c.locker.RLock()
				element := c.ll.Back()
				c.locker.RUnlock()

				if element == nil {
					break
				}

				e := element.Value.(*entry)
				if time.Now().Sub(e.expire) >= 0 {
					// 过期了
					expired[e.key] = element
					continue
				}

				break
			}

			if len(expired) == 0 {
				break
			}

			c.locker.Lock()
			for key, element := range expired {
				c.ll.Remove(element)
				delete(c.cache, key)
			}
			c.locker.Unlock()
		}
	}
}
