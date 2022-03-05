package bigcache

import "time"

// Config for BigCache
type Config struct {
	// Number of cache shards, value must be a power of two
	// 缓存分片的个数，值必须为2的幂
	Shards int
	// Time after which entry can be evicted
	// 缓存条目存活时间
	LifeWindow time.Duration
	// Interval between removing expired entries (clean up).
	// If set to <= 0 then no action is performed. Setting to < 1 second is counterproductive — bigcache has a one second resolution.
	// 删除过期条目的时间间隔(清理)。如果设置为<= 0，则不执行任何操作。设置为< 1秒会适得其反——bigcache的分辨率为1秒。
	CleanWindow time.Duration
	// Max number of entries in life window. Used only to calculate initial size for cache shards.
	// When proper value is set then additional memory allocation does not occur.
	// 生命窗口的最大条目数。仅用于计算缓存分片的初始大小。当设置了适当的值时，就不会分配额外的内存。
	// cache中能保存的最大的缓存数目
	MaxEntriesInWindow int
	// Max size of entry in bytes. Used only to calculate initial size for cache shards.
	// 以字节为单位的最大条目大小。仅用于计算缓存分片的初始大小。
	MaxEntrySize int
	// StatsEnabled if true calculate the number of times a cached resource was requested.
	// true计算缓存资源被请求的次数。
	StatsEnabled bool
	// Verbose mode prints information about new memory allocation
	// Verbose模式打印关于新内存分配的信息
	Verbose bool
	// Hasher used to map between string keys and unsigned 64bit integers, by default fnv64 hashing is used.
	// 用于映射字符串键和无符号64位整数的哈希器，默认情况下使用fnv64哈希。
	Hasher Hasher
	// HardMaxCacheSize is a limit for BytesQueue size in MB.
	// It can protect application from consuming all available memory on machine, therefore from running OOM Killer.
	// Default value is 0 which means unlimited size. When the limit is higher than 0 and reached then
	// the oldest entries are overridden for the new ones. The max memory consumption will be bigger than
	// HardMaxCacheSize due to Shards' s additional memory. Every Shard consumes additional memory for map of keys
	// and statistics (map[uint64]uint32) the size of this map is equal to number of entries in
	// cache ~ 2×(64+32)×n bits + overhead or map itself.
	// HardMaxCacheSize是以MB为单位的BytesQueue大小限制。
	// 它可以保护应用程序不会消耗机器上所有可用的内存，因此不会运行OOM Killer。
	// 默认值为0，表示无限大小。当限制大于0并达到时，旧的条目将被新条目覆盖。
	// 由于Shards的额外内存，最大内存消耗将大于HardMaxCacheSize。
	// 每个Shard会为map of keys和statistics消耗额外的内存(map[uint64]uint32)，该map的大小等于cache中条目数~ 2×(64+32)×n位+开销或map本身。
	HardMaxCacheSize int
	// OnRemove is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// ignored if OnRemoveWithMetadata is specified.
	// OnRemove是一个回调函数，当最老的条目被删除时，因为它的过期时间，或者没有空间留给新条目，或者因为delete被调用。
	// 默认值为nil，这意味着没有回调，它阻止解除最老的条目。如果指定了OnRemoveWithMetadata则被忽略。
	OnRemove func(key string, entry []byte)
	// OnRemoveWithMetadata is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A structure representing details about that specific entry.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// OnRemoveWithMetadata是一个回调函数，当最老的条目被删除时，因为它的过期时间，或者没有空间留给新条目，或者因为delete被调用。
	// 表示有关特定条目的详细信息的结构。默认值为nil，这意味着没有回调，并且它阻止打开最老的条目。
	OnRemoveWithMetadata func(key string, entry []byte, keyMetadata Metadata)
	// OnRemoveWithReason is a callback fired when the oldest entry is removed because of its expiration time or no space left
	// for the new entry, or because delete was called. A constant representing the reason will be passed through.
	// Default value is nil which means no callback and it prevents from unwrapping the oldest entry.
	// Ignored if OnRemove is specified.
	OnRemoveWithReason func(key string, entry []byte, reason RemoveReason)

	onRemoveFilter int

	// Logger is a logging interface and used in combination with `Verbose`
	// Defaults to `DefaultLogger()`
	Logger Logger
}

// DefaultConfig initializes config with default values.
// When load for BigCache can be predicted in advance then it is better to use custom config.
func DefaultConfig(eviction time.Duration) Config {
	return Config{
		Shards:             1024,
		LifeWindow:         eviction,
		CleanWindow:        1 * time.Second,
		MaxEntriesInWindow: 1000 * 10 * 60,
		MaxEntrySize:       500,
		StatsEnabled:       false,
		Verbose:            true,
		Hasher:             newDefaultHasher(),
		HardMaxCacheSize:   0,
		Logger:             DefaultLogger(),
	}
}

// initialShardSize computes initial shard size
// 计算初始分片大小
func (c Config) initialShardSize() int {
	return max(c.MaxEntriesInWindow/c.Shards, minimumEntriesInShard)
}

// maximumShardSizeInBytes computes maximum shard size in bytes
// 以字节为单位计算最大分片大小
func (c Config) maximumShardSizeInBytes() int {
	maxShardSize := 0

	if c.HardMaxCacheSize > 0 {
		maxShardSize = convertMBToBytes(c.HardMaxCacheSize) / c.Shards
	}

	return maxShardSize
}

// OnRemoveFilterSet sets which remove reasons will trigger a call to OnRemoveWithReason.
// Filtering out reasons prevents bigcache from unwrapping them, which saves cpu.
func (c Config) OnRemoveFilterSet(reasons ...RemoveReason) Config {
	c.onRemoveFilter = 0
	for i := range reasons {
		c.onRemoveFilter |= 1 << uint(reasons[i])
	}

	return c
}
