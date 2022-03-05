package bigcache

// Stats stores cache statistics
type Stats struct {
	// Hits is a number of successfully found keys
	// 命中的key数量
	Hits int64 `json:"hits"`
	// Misses is a number of not found keys
	// miss是没有找到钥匙的数量
	Misses int64 `json:"misses"`
	// DelHits is a number of successfully deleted keys
	// 删除命中次数
	DelHits int64 `json:"delete_hits"`
	// DelMisses is a number of not deleted keys
	// 删除时未命中
	DelMisses int64 `json:"delete_misses"`
	// Collisions is a number of happened key-collisions
	// 发生的键碰撞的数量
	Collisions int64 `json:"collisions"`
}
