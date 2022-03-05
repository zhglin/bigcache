package bigcache

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}

// mb转byte
func convertMBToBytes(value int) int {
	return value * 1024 * 1024
}

// 校验是否是2的倍数
func isPowerOfTwo(number int) bool {
	return (number != 0) && (number&(number-1)) == 0
}
