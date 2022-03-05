package bigcache

import (
	"encoding/binary"
)

const (
	timestampSizeInBytes = 8                                                       // Number of bytes used for timestamp
	hashSizeInBytes      = 8                                                       // Number of bytes used for hash
	keySizeInBytes       = 2                                                       // Number of bytes used for size of entry key
	headersSizeInBytes   = timestampSizeInBytes + hashSizeInBytes + keySizeInBytes // Number of bytes used for all headers
)

// 编码缓存条目
func wrapEntry(timestamp uint64, hash uint64, key string, entry []byte, buffer *[]byte) []byte {
	keyLength := len(key)
	blobLength := len(entry) + headersSizeInBytes + keyLength // kv的总长度 key长度+value长度+额外占用的长度

	// 超过buffer长度 新建
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}
	blob := *buffer

	binary.LittleEndian.PutUint64(blob, timestamp)                                                // 写入时间戳
	binary.LittleEndian.PutUint64(blob[timestampSizeInBytes:], hash)                              // 写入key的hash值
	binary.LittleEndian.PutUint16(blob[timestampSizeInBytes+hashSizeInBytes:], uint16(keyLength)) // 写入key的长度
	copy(blob[headersSizeInBytes:], key)                                                          // 写入key只
	copy(blob[headersSizeInBytes+keyLength:], entry)                                              // 写入value

	return blob[:blobLength]
}

// 追加entry
func appendToWrappedEntry(timestamp uint64, wrappedEntry []byte, entry []byte, buffer *[]byte) []byte {
	// 新的长度
	blobLength := len(wrappedEntry) + len(entry)
	if blobLength > len(*buffer) {
		*buffer = make([]byte, blobLength)
	}

	blob := *buffer

	binary.LittleEndian.PutUint64(blob, timestamp)                         // 写入时间
	copy(blob[timestampSizeInBytes:], wrappedEntry[timestampSizeInBytes:]) // 写入旧值
	copy(blob[len(wrappedEntry):], entry)                                  // 写入追加的值

	return blob[:blobLength]
}

// 获取缓存的值 复制
func readEntry(data []byte) []byte {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	dst := make([]byte, len(data)-int(headersSizeInBytes+length))
	copy(dst, data[headersSizeInBytes+length:])

	return dst
}

// 取出时间戳
func readTimestampFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data)
}

// 读取出来key 复制
func readKeyFromEntry(data []byte) string {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	// copy on read
	dst := make([]byte, length)
	copy(dst, data[headersSizeInBytes:headersSizeInBytes+length])

	return bytesToString(dst)
}

// 校验key是不相等
func compareKeyFromEntry(data []byte, key string) bool {
	length := binary.LittleEndian.Uint16(data[timestampSizeInBytes+hashSizeInBytes:])

	return bytesToString(data[headersSizeInBytes:headersSizeInBytes+length]) == key
}

func readHashFromEntry(data []byte) uint64 {
	return binary.LittleEndian.Uint64(data[timestampSizeInBytes:])
}

// 删除key的hash值
func resetKeyFromEntry(data []byte) {
	binary.LittleEndian.PutUint64(data[timestampSizeInBytes:], 0)
}
