//go:build linux

package chunk

import (
	"time"

	"github.com/cloudwego/shmipc-go"
)

// BenchCacheClient is an exported wrapper for benchmarking in separate processes.
type BenchCacheClient struct {
	mgr *remoteCacheManager
}

func NewRemoteCacheManagerForBench(dataSock, ctrlSock string, spinMicros int) (*BenchCacheClient, error) {
	mgr, err := newRemoteCacheManagerWithSpin(dataSock, ctrlSock, nil, spinMicros)
	if err != nil {
		return nil, err
	}
	return &BenchCacheClient{mgr: mgr.(*remoteCacheManager)}, nil
}

func (c *BenchCacheClient) Load(key string) (ReadCloser, error) {
	return c.mgr.load(key)
}

func (c *BenchCacheClient) Close() {
	c.mgr.close()
}

func (c *BenchCacheClient) AsManager() CacheManager {
	return c.mgr
}

func (c *BenchCacheClient) CacheViaManager(key string, data []byte) {
	page := NewPage(data)
	page.Acquire()
	c.mgr.cache(key, page, true, false)
}

// NewCacheServerForBench creates a cache server with optional spin polling.
func NewCacheServerForBench(dataSock, ctrlSock, cacheDir string, spinMicros int) (*CacheServer, error) {
	conf := &Config{
		CacheDir:      cacheDir,
		CacheSize:     500 << 20,
		BlockSize:     4 << 20,
		AutoCreate:    true,
		CacheEviction: Eviction2Random,
		CacheChecksum: CsNone,
	}
	cm := newCacheManager(conf, nil, nil)
	srv := NewCacheServer(cm, dataSock, ctrlSock, CsNone)
	if spinMicros > 0 {
		srv.shmConf = shmipc.DefaultConfig()
		srv.shmConf.ShareMemoryBufferCap = 32 << 20
		srv.shmConf.PollingSpinDuration = time.Duration(spinMicros) * time.Microsecond
	}
	return srv, nil
}

func (s *CacheServer) CacheManager() CacheManager { return s.cache }

func (s *CacheServer) WaitPending() {
	if cm, ok := s.cache.(*cacheManager); ok {
		cm.waitPending()
	}
}

// NewCacheManagerForBench creates a local cache manager for in-process benchmarking.
func NewCacheManagerForBench(conf *Config) CacheManager {
	return newCacheManager(conf, nil, nil)
}

// BenchCacheManager wraps a CacheManager with exported methods for bench binaries.
type BenchCacheManager struct{ M CacheManager }

func (b *BenchCacheManager) Load(key string) (ReadCloser, error) { return b.M.load(key) }
func (b *BenchCacheManager) Cache(key string, p *Page, force, dropCache bool) {
	b.M.cache(key, p, force, dropCache)
}
func (b *BenchCacheManager) Close() {
	switch m := b.M.(type) {
	case *cacheManager:
		m.close()
	case *remoteCacheManager:
		m.close()
	}
}
func (b *BenchCacheManager) WaitPending() {
	if m, ok := b.M.(*cacheManager); ok {
		m.waitPending()
	}
}
func (b *BenchCacheManager) ReadAt(key string, buf []byte, off int64) (int, error) {
	rc, err := b.M.load(key)
	if err != nil {
		return 0, err
	}
	n, err := rc.ReadAt(buf, off)
	rc.Close()
	return n, err
}
