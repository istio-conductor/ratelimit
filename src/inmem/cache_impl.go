package inmem

import (
	"context"
	"github.com/coocood/freecache"
	pb "github.com/envoyproxy/go-control-plane/envoy/service/ratelimit/v3"
	"github.com/envoyproxy/ratelimit/src/config"
	"github.com/envoyproxy/ratelimit/src/limiter"
	"github.com/envoyproxy/ratelimit/src/settings"
	"github.com/envoyproxy/ratelimit/src/utils"
	stats "github.com/lyft/gostats"
	logger "github.com/sirupsen/logrus"
	"math/rand"
	"sync"
	"time"
)

type rateLimitMemcacheImpl struct {
	inMemCache                 *TTLCache
	timeSource                 utils.TimeSource
	jitterRand                 *rand.Rand
	expirationJitterMaxSeconds int64
	localCache                 *freecache.Cache
	waitGroup                  sync.WaitGroup
	nearLimitRatio             float32
	baseRateLimiter            *limiter.BaseRateLimiter
}

var AutoFlushForIntegrationTests bool = false

var _ limiter.RateLimitCache = (*rateLimitMemcacheImpl)(nil)

func (this *rateLimitMemcacheImpl) DoLimit(
	ctx context.Context,
	request *pb.RateLimitRequest,
	limits []*config.RateLimit) []*pb.RateLimitResponse_DescriptorStatus {

	logger.Debugf("starting cache lookup")

	// request.HitsAddend could be 0 (default value) if not specified by the caller in the Ratelimit request.
	hitsAddend := utils.Max(1, request.HitsAddend)

	// First build a list of all cache keys that we are actually going to hit.
	cacheKeys := this.baseRateLimiter.GenerateCacheKeys(request, limits, hitsAddend)

	isOverLimitWithLocalCache := make([]bool, len(request.Descriptors))

	for i, cacheKey := range cacheKeys {
		if cacheKey.Key == "" {
			continue
		}

		// Check if key is over the limit in local cache.
		if this.baseRateLimiter.IsOverLimitWithLocalCache(cacheKey.Key) {
			isOverLimitWithLocalCache[i] = true
			logger.Debugf("cache key is over the limit: %s", cacheKey.Key)
			continue
		}

		logger.Debugf("looking up cache key: %s", cacheKey.Key)
	}

	// Now fetch from memcache.
	responseDescriptorStatuses := make([]*pb.RateLimitResponse_DescriptorStatus,
		len(request.Descriptors))

	for i, cacheKey := range cacheKeys {
		if cacheKey.Key == "" {
			continue
		}
		limitBeforeIncrease, ok := this.inMemCache.Get(cacheKey.Key)
		if !ok {
			limitBeforeIncrease = 0
		}
		limitAfterIncrease := limitBeforeIncrease.(uint32) + hitsAddend

		limitInfo := limiter.NewRateLimitInfo(limits[i], limitBeforeIncrease.(uint32), limitAfterIncrease, 0, 0)

		responseDescriptorStatuses[i] = this.baseRateLimiter.GetResponseDescriptorStatus(cacheKey.Key,
			limitInfo, isOverLimitWithLocalCache[i], hitsAddend)
	}

	this.waitGroup.Add(1)
	go this.increaseAsync(cacheKeys, isOverLimitWithLocalCache, limits, hitsAddend)
	if AutoFlushForIntegrationTests {
		this.Flush()
	}

	return responseDescriptorStatuses
}

func (this *rateLimitMemcacheImpl) increaseAsync(cacheKeys []limiter.CacheKey, isOverLimitWithLocalCache []bool,
	limits []*config.RateLimit, hitsAddend uint32) {
	defer this.waitGroup.Done()
	for i, cacheKey := range cacheKeys {
		if cacheKey.Key == "" || isOverLimitWithLocalCache[i] {
			continue
		}
		expirationSeconds := utils.UnitToDivider(limits[i].Limit.Unit)
		if this.expirationJitterMaxSeconds > 0 {
			expirationSeconds += this.jitterRand.Int63n(this.expirationJitterMaxSeconds)
		}
		this.inMemCache.IncOrSet(cacheKey.Key, hitsAddend, time.Duration(expirationSeconds)*time.Second )
	}
}

func (this *rateLimitMemcacheImpl) Flush() {
	this.waitGroup.Wait()
}

func NewRateLimitCacheImpl(client *TTLCache, timeSource utils.TimeSource, jitterRand *rand.Rand,
	expirationJitterMaxSeconds int64, localCache *freecache.Cache, scope stats.Scope, nearLimitRatio float32, cacheKeyPrefix string) limiter.RateLimitCache {
	return &rateLimitMemcacheImpl{
		inMemCache:                     client,
		timeSource:                 timeSource,
		jitterRand:                 jitterRand,
		expirationJitterMaxSeconds: expirationJitterMaxSeconds,
		localCache:                 localCache,
		nearLimitRatio:             nearLimitRatio,
		baseRateLimiter:            limiter.NewBaseRateLimit(timeSource, jitterRand, expirationJitterMaxSeconds, localCache, nearLimitRatio, cacheKeyPrefix),
	}
}

func NewRateLimitCacheImplFromSettings(s settings.Settings, timeSource utils.TimeSource, jitterRand *rand.Rand,
	localCache *freecache.Cache, scope stats.Scope) limiter.RateLimitCache {
	return NewRateLimitCacheImpl(
		New(),
		timeSource,
		jitterRand,
		s.ExpirationJitterMaxSeconds,
		localCache,
		scope,
		s.NearLimitRatio,
		s.CacheKeyPrefix,
	)
}
