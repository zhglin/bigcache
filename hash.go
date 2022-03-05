package bigcache

// Hasher is responsible for generating unsigned, 64 bit hash of provided string. Hasher should minimize collisions
// (generating same hash for different strings) and while performance is also important fast functions are preferable (i.e.
// you can use FarmHash family).
// 哈希器负责生成所提供字符串的无符号、64位哈希。
// 哈希器应该最小化冲突(为不同的字符串生成相同的哈希)，虽然性能也很重要，但快速函数更可取(例如，你可以使用FarmHash家族)。
type Hasher interface {
	Sum64(string) uint64
}
