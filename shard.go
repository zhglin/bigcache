package bigcache

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/allegro/bigcache/v3/queue"
)

type onRemoveCallback func(wrappedEntry []byte, reason RemoveReason)

// Metadata contains information of a specific entry
type Metadata struct {
	RequestCount uint32
}

type cacheShard struct {
	hashmap     map[uint64]uint32 // hash后的key所在的起始位置
	entries     queue.BytesQueue  // 存储数据条目
	lock        sync.RWMutex
	entryBuffer []byte           // 单条缓存条目内存空间
	onRemove    onRemoveCallback // 删除key是的回调

	isVerbose    bool // 是否记录hash碰撞的日志
	statsEnabled bool // 是否记录key的命中次数
	logger       Logger
	clock        clock
	lifeWindow   uint64 // key的存活时间

	hashmapStats map[uint64]uint32 // key的命中次数
	stats        Stats             // 统计信息
}

// 返回key的value，以及是否过期
func (s *cacheShard) getWithInfo(key string, hashedKey uint64) (entry []byte, resp Response, err error) {
	currentTime := uint64(s.clock.Epoch())

	// 读取数据
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, resp, err
	}

	// 校验是否键冲突
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, resp, ErrEntryNotFound
	}

	// 解析value，写入时间
	entry = readEntry(wrappedEntry)
	oldestTimeStamp := readTimestampFromEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)

	// 校验是否过期
	if currentTime-oldestTimeStamp >= s.lifeWindow {
		resp.EntryStatus = Expired
	}
	return entry, resp, nil
}

// 获取value
func (s *cacheShard) get(key string, hashedKey uint64) ([]byte, error) {
	// 加锁读取
	s.lock.RLock()
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		s.lock.RUnlock()
		return nil, err
	}

	// 校验key是否冲突
	if entryKey := readKeyFromEntry(wrappedEntry); key != entryKey {
		s.lock.RUnlock()
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, entryKey, hashedKey)
		}
		return nil, ErrEntryNotFound
	}

	// 读取value值
	entry := readEntry(wrappedEntry)
	s.lock.RUnlock()
	s.hit(hashedKey)

	return entry, nil
}

// 根据hashedKey获取对应的value
func (s *cacheShard) getWrappedEntry(hashedKey uint64) ([]byte, error) {
	itemIndex := s.hashmap[hashedKey]

	// hashedKey不存在
	if itemIndex == 0 {
		s.miss()
		return nil, ErrEntryNotFound
	}

	// 读取
	wrappedEntry, err := s.entries.Get(int(itemIndex))
	if err != nil {
		s.miss()
		return nil, err
	}

	return wrappedEntry, err
}

// 获取并校验是否产生hash碰撞
func (s *cacheShard) getValidWrapEntry(key string, hashedKey uint64) ([]byte, error) {
	wrappedEntry, err := s.getWrappedEntry(hashedKey)
	if err != nil {
		return nil, err
	}

	// hash碰撞
	if !compareKeyFromEntry(wrappedEntry, key) {
		s.collision()
		if s.isVerbose {
			s.logger.Printf("Collision detected. Both %q and %q have the same hash %x", key, readKeyFromEntry(wrappedEntry), hashedKey)
		}

		return nil, ErrEntryNotFound
	}
	s.hitWithoutLock(hashedKey)

	return wrappedEntry, nil
}

// 设置缓存条目
func (s *cacheShard) set(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	s.lock.Lock()

	// 已存在
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetKeyFromEntry(previousEntry) // 删除掉底层数据的hash值
			//remove hashkey 删除key
			delete(s.hashmap, hashedKey)
		}
	}

	// 校验头部数据是否过期
	if oldestEntry, err := s.entries.Peek(); err == nil {
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	// 编码entry
	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			s.lock.Unlock()
			return nil
		}
		// 空间不够导致写入失败，删除头部元素进行重试
		if s.removeOldestEntry(NoSpace) != nil {
			s.lock.Unlock()
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

// 明确不存在情况下的添加，无锁
func (s *cacheShard) addNewWithoutLock(key string, hashedKey uint64, entry []byte) error {
	currentTimestamp := uint64(s.clock.Epoch())

	if oldestEntry, err := s.entries.Peek(); err == nil {
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	w := wrapEntry(currentTimestamp, hashedKey, key, entry, &s.entryBuffer)

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

// 无锁set
func (s *cacheShard) setWrappedEntryWithoutLock(currentTimestamp uint64, w []byte, hashedKey uint64) error {
	// 旧值已存在
	if previousIndex := s.hashmap[hashedKey]; previousIndex != 0 {
		if previousEntry, err := s.entries.Get(int(previousIndex)); err == nil {
			resetKeyFromEntry(previousEntry) // 设置删除
		}
	}

	// 校验是否删除头元素
	if oldestEntry, err := s.entries.Peek(); err == nil {
		s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry)
	}

	for {
		if index, err := s.entries.Push(w); err == nil {
			s.hashmap[hashedKey] = uint32(index)
			return nil
		}
		if s.removeOldestEntry(NoSpace) != nil {
			return fmt.Errorf("entry is bigger than max shard size")
		}
	}
}

// 对key追加内容
func (s *cacheShard) append(key string, hashedKey uint64, entry []byte) error {
	s.lock.Lock()
	wrappedEntry, err := s.getValidWrapEntry(key, hashedKey)

	// 不存在就直接进行添加
	if err == ErrEntryNotFound {
		err = s.addNewWithoutLock(key, hashedKey, entry)
		s.lock.Unlock()
		return err
	}

	if err != nil {
		s.lock.Unlock()
		return err
	}

	currentTimestamp := uint64(s.clock.Epoch())

	// 编码追加
	w := appendToWrappedEntry(currentTimestamp, wrappedEntry, entry, &s.entryBuffer)

	err = s.setWrappedEntryWithoutLock(currentTimestamp, w, hashedKey)
	s.lock.Unlock()

	return err
}

// 删除键
func (s *cacheShard) del(hashedKey uint64) error {
	// Optimistic pre-check using only readlock
	// 只使用readlock的乐观预检查
	s.lock.RLock()
	{
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.RUnlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		if err := s.entries.CheckGet(int(itemIndex)); err != nil {
			s.lock.RUnlock()
			s.delmiss()
			return err
		}
	}
	s.lock.RUnlock()

	s.lock.Lock()
	{
		// After obtaining the writelock, we need to read the same again,
		// since the data delivered earlier may be stale now
		// 获取写锁后，我们需要再次读取相同的数据，因为之前的数据可能已经过时
		itemIndex := s.hashmap[hashedKey]

		if itemIndex == 0 {
			s.lock.Unlock()
			s.delmiss()
			return ErrEntryNotFound
		}

		// 取值
		wrappedEntry, err := s.entries.Get(int(itemIndex))
		if err != nil {
			s.lock.Unlock()
			s.delmiss()
			return err
		}

		// 删除key
		delete(s.hashmap, hashedKey)
		s.onRemove(wrappedEntry, Deleted)
		if s.statsEnabled {
			delete(s.hashmapStats, hashedKey)
		}
		// 重置
		resetKeyFromEntry(wrappedEntry)
	}
	s.lock.Unlock()

	s.delhit()
	return nil
}

// 校验entry是否过期
func (s *cacheShard) onEvict(oldestEntry []byte, currentTimestamp uint64, evict func(reason RemoveReason) error) bool {
	oldestTimestamp := readTimestampFromEntry(oldestEntry)
	if currentTimestamp-oldestTimestamp > s.lifeWindow {
		evict(Expired)
		return true
	}
	return false
}

// 驱逐过期key
func (s *cacheShard) cleanUp(currentTimestamp uint64) {
	s.lock.Lock()
	for {
		if oldestEntry, err := s.entries.Peek(); err != nil {
			break
		} else if evicted := s.onEvict(oldestEntry, currentTimestamp, s.removeOldestEntry); !evicted {
			break
		}
	}
	s.lock.Unlock()
}

func (s *cacheShard) getEntry(hashedKey uint64) ([]byte, error) {
	s.lock.RLock()

	entry, err := s.getWrappedEntry(hashedKey)
	// copy entry
	newEntry := make([]byte, len(entry))
	copy(newEntry, entry)

	s.lock.RUnlock()

	return newEntry, err
}

// 复制当前分片的key
func (s *cacheShard) copyHashedKeys() (keys []uint64, next int) {
	s.lock.RLock()
	keys = make([]uint64, len(s.hashmap))

	for key := range s.hashmap {
		keys[next] = key
		next++
	}

	s.lock.RUnlock()
	return keys, next
}

// 删除头部数据
func (s *cacheShard) removeOldestEntry(reason RemoveReason) error {
	oldest, err := s.entries.Pop()
	if err == nil {
		hash := readHashFromEntry(oldest)
		if hash == 0 { // 条目已经被resetKeyFromEntry显式删除，忽略
			// entry has been explicitly deleted with resetKeyFromEntry, ignore
			return nil
		}
		delete(s.hashmap, hash)
		s.onRemove(oldest, reason)
		if s.statsEnabled {
			delete(s.hashmapStats, hash)
		}
		return nil
	}
	return err
}

// 重置分片
func (s *cacheShard) reset(config Config) {
	s.lock.Lock()
	s.hashmap = make(map[uint64]uint32, config.initialShardSize())
	s.entryBuffer = make([]byte, config.MaxEntrySize+headersSizeInBytes)
	s.entries.Reset()
	s.lock.Unlock()
}

// 计算缓存数量
func (s *cacheShard) len() int {
	s.lock.RLock()
	res := len(s.hashmap)
	s.lock.RUnlock()
	return res
}

// 分片的字节数
func (s *cacheShard) capacity() int {
	s.lock.RLock()
	res := s.entries.Capacity()
	s.lock.RUnlock()
	return res
}

func (s *cacheShard) getStats() Stats {
	var stats = Stats{
		Hits:       atomic.LoadInt64(&s.stats.Hits),
		Misses:     atomic.LoadInt64(&s.stats.Misses),
		DelHits:    atomic.LoadInt64(&s.stats.DelHits),
		DelMisses:  atomic.LoadInt64(&s.stats.DelMisses),
		Collisions: atomic.LoadInt64(&s.stats.Collisions),
	}
	return stats
}

// 加锁返回key的访问次数
func (s *cacheShard) getKeyMetadataWithLock(key uint64) Metadata {
	s.lock.RLock()
	c := s.hashmapStats[key]
	s.lock.RUnlock()
	return Metadata{
		RequestCount: c,
	}
}

// 不加锁返回key的访问次数
func (s *cacheShard) getKeyMetadata(key uint64) Metadata {
	return Metadata{
		RequestCount: s.hashmapStats[key],
	}
}

// 增加key的命中次数
func (s *cacheShard) hit(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.lock.Lock()
		s.hashmapStats[key]++
		s.lock.Unlock()
	}
}

// 命中数量
func (s *cacheShard) hitWithoutLock(key uint64) {
	atomic.AddInt64(&s.stats.Hits, 1)
	if s.statsEnabled {
		s.hashmapStats[key]++
	}
}

// miss计数
func (s *cacheShard) miss() {
	atomic.AddInt64(&s.stats.Misses, 1)
}

// 删除命中次数
func (s *cacheShard) delhit() {
	atomic.AddInt64(&s.stats.DelHits, 1)
}

// 删除时未命中
func (s *cacheShard) delmiss() {
	atomic.AddInt64(&s.stats.DelMisses, 1)
}

// hash碰撞次数
func (s *cacheShard) collision() {
	atomic.AddInt64(&s.stats.Collisions, 1)
}

// 初始化cacheShard
func initNewShard(config Config, callback onRemoveCallback, clock clock) *cacheShard {
	// 计算内存大小  当前分片的缓存数目*缓存的最大长度
	bytesQueueInitialCapacity := config.initialShardSize() * config.MaxEntrySize
	// 每个分片的最大内存大小 硬限制
	maximumShardSizeInBytes := config.maximumShardSizeInBytes()
	if maximumShardSizeInBytes > 0 && bytesQueueInitialCapacity > maximumShardSizeInBytes {
		bytesQueueInitialCapacity = maximumShardSizeInBytes
	}
	return &cacheShard{
		hashmap:      make(map[uint64]uint32, config.initialShardSize()),
		hashmapStats: make(map[uint64]uint32, config.initialShardSize()),
		entries:      *queue.NewBytesQueue(bytesQueueInitialCapacity, maximumShardSizeInBytes, config.Verbose),
		entryBuffer:  make([]byte, config.MaxEntrySize+headersSizeInBytes),
		onRemove:     callback,

		isVerbose:    config.Verbose,
		logger:       newLogger(config.Logger),
		clock:        clock,
		lifeWindow:   uint64(config.LifeWindow.Seconds()),
		statsEnabled: config.StatsEnabled,
	}
}
