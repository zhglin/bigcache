// +build !appengine

package bigcache

import (
	"unsafe"
)

// 字节数组转字符串
func bytesToString(b []byte) string {
	return *(*string)(unsafe.Pointer(&b))
}
