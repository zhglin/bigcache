package bigcache

import "time"

// 时钟接口
type clock interface {
	Epoch() int64 // 返回当前时间戳
}

// 当前时间
type systemClock struct {
}

func (c systemClock) Epoch() int64 {
	return time.Now().Unix()
}
