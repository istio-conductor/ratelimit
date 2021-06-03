package inmem

import (
	"github.com/patrickmn/go-cache"
	"sync"

	"time"
)

func New() *TTLCache{
	return &TTLCache{
		cache: cache.New(5*time.Minute, 10*time.Minute),
	}
}

type TTLCache struct {
	sync.Mutex
	cache *cache.Cache
}

func (c *TTLCache) Get(key string) (interface{}, bool) {
	return c.cache.Get(key)
}

func (c *TTLCache) GetMul(keys []string) []interface{} {
	res := make([]interface{}, 0, len(keys))
	for _, k := range keys {
		re, _ := c.cache.Get(k)
		res = append(res, re)
	}
	return res
}

func (c *TTLCache) PutKv(key string, val interface{}, ttl time.Duration) {
	c.cache.Set(key, val, ttl)
}

func (c *TTLCache) Inc(key string, added uint32) (uint32, error) {
	return c.cache.IncrementUint32(key, added)
}

func (c *TTLCache) IncOrSet(key string, added uint32, ttl time.Duration) {
	c.Lock()
	defer c.Unlock()
	if _, err := c.cache.IncrementUint32(key, added); err != nil {
		c.cache.Set(key, added, ttl)
	}
}
