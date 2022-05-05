package gossip

import (
	"container/list"
	"sync"
	"time"
)

// Cache 消息id的过期+lru缓存
type Cache struct {
	Cap    int
	TTL    time.Duration
	cache  sync.Map
	ll     list.List
	locker sync.RWMutex
}

type entry struct {
	key    string
	expire time.Time
}

func NewCache(cap int, ttl time.Duration) *Cache {
	c := &Cache{
		Cap:    cap,
		TTL:    ttl,
		ll:     list.List{},
		cache:  sync.Map{},
		locker: sync.RWMutex{},
	}
	go c.trigger()
	return c
}

func (c *Cache) Set(key string) {
	if val, ok := c.cache.Load(key); ok {
		el := val.(*list.Element)
		c.locker.Lock()
		c.ll.Remove(el)
		c.locker.Unlock()
		c.cache.Delete(key)
	}

	c.locker.Lock()
	el := c.ll.PushFront(&entry{key: key, expire: time.Now().Add(c.TTL)})
	c.locker.Unlock()
	c.cache.Store(key, el)

	if c.Cap > 0 && c.ll.Len() > c.Cap {
		c.removeOldest()
	}
}

func (c *Cache) Has(key string) bool {
	c.locker.RLock()
	_, ok := c.cache.Load(key)
	c.locker.RUnlock()
	return ok
}

func (c *Cache) removeOldest() {
	c.locker.RLock()
	el := c.ll.Back()
	c.locker.RUnlock()
	if el != nil {
		c.locker.Lock()
		c.ll.Remove(el)
		c.locker.Unlock()
		e := el.Value.(*entry)
		c.cache.Delete(e.key)
	}
}

func (c *Cache) trigger() {
	t := time.NewTicker(time.Second * 5)
	for {
		select {
		case <-t.C:
			c.cache.Range(func(key, value any) bool {
				el := value.(*list.Element)
				e := el.Value.(*entry)
				if time.Now().Sub(e.expire) > 0 {
					// 过期
					c.locker.Lock()
					c.ll.Remove(el)
					c.locker.Unlock()
					c.cache.Delete(key)
				}
				return true
			})
		}
	}
}
