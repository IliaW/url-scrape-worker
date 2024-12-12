package cache

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	netUrl "net/url"
	"os"
	"strings"
	"sync"

	"github.com/IliaW/url-scrape-worker/config"
	"github.com/bradfitz/gomemcache/memcache"
)

type CachedClient interface {
	SaveS3Link(string, string)
	DecrementThreshold(string)
	Close()
}

type MemcachedClient struct {
	client *memcache.Client
	cfg    *config.CacheConfig
	log    *slog.Logger
	mu     sync.Mutex
}

func NewMemcachedClient(cacheConfig *config.CacheConfig, log *slog.Logger) *MemcachedClient {
	log.Info("connecting to memcached...")
	ss := new(memcache.ServerList)
	servers := strings.Split(cacheConfig.Servers, ",")
	err := ss.SetServers(servers...)
	if err != nil {
		log.Error("failed to set memcached servers.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c := &MemcachedClient{
		client: memcache.NewFromSelector(ss),
		cfg:    cacheConfig,
		log:    log,
	}
	c.log.Info("pinging the memcached.")
	err = c.client.Ping()
	if err != nil {
		log.Error("connection to the memcached is failed.", slog.String("err", err.Error()))
		os.Exit(1)
	}
	c.log.Info("connected to memcached!")

	return c
}

func (mc *MemcachedClient) SaveS3Link(url string, linkToS3 string) {
	if linkToS3 == "" {
		mc.log.Warn("s3 link is empty. Skip saving to cache.")
		return
	}
	key := hashURL(url)
	if err := mc.set(key, linkToS3, int32((mc.cfg.TtlForScrape).Seconds())); err != nil {
		mc.log.Error("failed to save s3 link to cache.", slog.String("key", key),
			slog.String("err", err.Error()))
	}
	mc.log.Debug("s3 link saved to cache.")
}

func (mc *MemcachedClient) DecrementThreshold(url string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.log.Debug("decrementing the threshold.")
	key := mc.generateDomainHash(url)
	_, err := mc.client.Decrement(key, 1)
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			mc.log.Debug("cache expired.", slog.String("key", key))
		} else {
			mc.log.Warn("failed to decrement the threshold.", slog.String("key", key),
				slog.String("err", err.Error()))
		}
	}
}

func (mc *MemcachedClient) Close() {
	mc.log.Info("closing memcached connection.")
	err := mc.client.Close()
	if err != nil {
		mc.log.Error("failed to close memcached connection.", slog.String("err", err.Error()))
	}
}

func (mc *MemcachedClient) set(key string, value any, expiration int32) error {
	byteValue, err := json.Marshal(value)
	if err != nil {
		return err
	}
	item := &memcache.Item{
		Key:        key,
		Value:      byteValue,
		Expiration: expiration,
	}

	return mc.client.Set(item)
}

func (mc *MemcachedClient) generateDomainHash(url string) string {
	u, err := netUrl.Parse(url)
	var key string
	if err != nil {
		mc.log.Error("failed to parse url. Use full url as a key.", slog.String("url", url),
			slog.String("err", err.Error()))
		key = fmt.Sprintf("%s-1m-scrapes", hashURL(url))
	} else {
		key = fmt.Sprintf("%s-1m-scrapes", hashURL(u.Host))
		mc.log.Debug("", slog.String("key:", key))
	}

	return key
}

func hashURL(url string) string {
	hash := sha256.New()
	hash.Write([]byte(url))
	return hex.EncodeToString(hash.Sum(nil))
}
