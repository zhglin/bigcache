package queue

import (
	"encoding/binary"
	"log"
	"time"
)

const (
	// Number of bytes to encode 0 in uvarint format
	minimumHeaderSize = 17 // 1 byte blobsize + timestampSizeInBytes + hashSizeInBytes
	// Bytes before left margin are not used. Zero index means element does not exist in queue, useful while reading slice from index
	leftMarginIndex = 1
)

var (
	errEmptyQueue       = &queueError{"Empty queue"}
	errInvalidIndex     = &queueError{"Index must be greater than zero. Invalid index."}
	errIndexOutOfBounds = &queueError{"Index out of range"}
)

// BytesQueue is a non-thread safe queue type of fifo based on bytes array.
// For every push operation index of entry is returned. It can be used to read the entry later
type BytesQueue struct {
	full         bool   // 是否已满
	array        []byte // 缓存内容
	capacity     int    // array长度
	maxCapacity  int    // 最大的内存数量
	head         int    // 头指针
	tail         int    // 尾指针
	count        int    // 以缓存的条目数量
	rightMargin  int    // 右边界位置 最大=len(array)
	headerBuffer []byte // 重用的内存空间
	verbose      bool
}

type queueError struct {
	message string
}

// getNeededSize returns the number of bytes an entry of length need in the queue
// 返回队列中长度项所需的字节数
func getNeededSize(length int) int {
	var header int
	switch {
	case length < 127: // 1<<7-1
		header = 1
	case length < 16382: // 1<<14-2
		header = 2
	case length < 2097149: // 1<<21 -3
		header = 3
	case length < 268435452: // 1<<28 -4
		header = 4
	default:
		header = 5
	}

	return length + header // 数据长度+额外记录长度
}

// NewBytesQueue initialize new bytes queue.
// capacity is used in bytes array allocation
// When verbose flag is set then information about memory allocation are printed
// NewBytesQueue初始化新字节队列。设置verbose标志后，将打印有关内存分配的信息
func NewBytesQueue(capacity int, maxCapacity int, verbose bool) *BytesQueue {
	return &BytesQueue{
		array:        make([]byte, capacity),
		capacity:     capacity,
		maxCapacity:  maxCapacity,
		headerBuffer: make([]byte, binary.MaxVarintLen32),
		tail:         leftMarginIndex,
		head:         leftMarginIndex,
		rightMargin:  leftMarginIndex,
		verbose:      verbose,
	}
}

// Reset removes all entries from queue
// 将从队列中删除所有条目
func (q *BytesQueue) Reset() {
	// Just reset indexes
	q.tail = leftMarginIndex
	q.head = leftMarginIndex
	q.rightMargin = leftMarginIndex
	q.count = 0
	q.full = false
}

// Push copies entry at the end of queue and moves tail pointer. Allocates more space if needed.
// Returns index for pushed data or error if maximum size queue limit is reached.
// Push复制队列末尾的条目，并移动尾指针。如果需要，分配更多的空间。返回推入数据的索引，如果达到最大队列限制则返回错误。
func (q *BytesQueue) Push(data []byte) (int, error) {
	// 获取data需要的长度
	neededSize := getNeededSize(len(data))

	if !q.canInsertAfterTail(neededSize) { // 从尾部插入
		if q.canInsertBeforeHead(neededSize) { // 首次从头插入，调整tail的位置=>head
			q.tail = leftMarginIndex
		} else if q.capacity+neededSize >= q.maxCapacity && q.maxCapacity > 0 { // 内存超限，不能扩容
			return -1, &queueError{"Full queue. Maximum size limit reached."}
		} else {
			q.allocateAdditionalMemory(neededSize)
		}
	}

	index := q.tail

	q.push(data, neededSize)

	return index, nil
}

// 扩容  minimum需要扩容的大小
func (q *BytesQueue) allocateAdditionalMemory(minimum int) {
	start := time.Now()
	if q.capacity < minimum {
		q.capacity += minimum
	}
	q.capacity = q.capacity * 2
	if q.capacity > q.maxCapacity && q.maxCapacity > 0 {
		q.capacity = q.maxCapacity
	}

	oldArray := q.array
	q.array = make([]byte, q.capacity)

	if leftMarginIndex != q.rightMargin { // 缓存中已有数据
		copy(q.array, oldArray[:q.rightMargin]) // copy现有数据

		// 回环了
		if q.tail <= q.head {
			// 未满 填补空洞 重置head tail位置
			if q.tail != q.head {
				// created slice is slightly larger then need but this is fine after only the needed bytes are copied
				q.push(make([]byte, q.head-q.tail), q.head-q.tail)
			}

			// 扩容后重置head tail
			q.head = leftMarginIndex
			q.tail = q.rightMargin
		}
	}

	q.full = false

	if q.verbose {
		log.Printf("Allocated new queue in %s; Capacity: %d \n", time.Since(start), q.capacity)
	}
}

// 写入数据到array
func (q *BytesQueue) push(data []byte, len int) {
	headerEntrySize := binary.PutUvarint(q.headerBuffer, uint64(len))
	q.copy(q.headerBuffer, headerEntrySize) // 写入长度

	q.copy(data, len-headerEntrySize) // 写入内容

	if q.tail > q.head {
		q.rightMargin = q.tail // 记录右边界
	}
	if q.tail == q.head {
		q.full = true
	}

	q.count++
}

// 数据copy到array中 并更新tail位置
func (q *BytesQueue) copy(data []byte, len int) {
	q.tail += copy(q.array[q.tail:], data[:len])
}

// Pop reads the oldest entry from queue and moves head pointer to the next one
// Pop从队列中读取最老的条目，并将头指针移动到下一个条目
func (q *BytesQueue) Pop() ([]byte, error) {
	data, blockSize, err := q.peek(q.head)
	if err != nil {
		return nil, err
	}

	q.head += blockSize // 下一条起始位置
	q.count--           // 缓存条目数量

	// head，tail都等于rightMargin 说明空了
	if q.head == q.rightMargin { // head在最右边
		q.head = leftMarginIndex // head回环
		if q.tail == q.rightMargin {
			q.tail = leftMarginIndex
		}
		q.rightMargin = q.tail
	}

	q.full = false

	return data, nil
}

// Peek reads the oldest entry from list without moving head pointer
// 在不移动头指针的情况下从列表中读取最老的条目
func (q *BytesQueue) Peek() ([]byte, error) {
	data, _, err := q.peek(q.head)
	return data, err
}

// Get reads entry from index
// Get从索引中读取条目
func (q *BytesQueue) Get(index int) ([]byte, error) {
	data, _, err := q.peek(index)
	return data, err
}

// CheckGet checks if an entry can be read from index
// 检查一个条目是否可以从索引中读取
func (q *BytesQueue) CheckGet(index int) error {
	return q.peekCheckErr(index)
}

// Capacity returns number of allocated bytes for queue
// 返回分配给队列的字节数
func (q *BytesQueue) Capacity() int {
	return q.capacity
}

// Len returns number of entries kept in queue
// 返回队列中保存的条目数
func (q *BytesQueue) Len() int {
	return q.count
}

// Error returns error message
func (e *queueError) Error() string {
	return e.message
}

// peekCheckErr is identical to peek, but does not actually return any data
// peekCheckErr与peek相同，但实际上不返回任何数据
func (q *BytesQueue) peekCheckErr(index int) error {

	if q.count == 0 {
		return errEmptyQueue
	}

	if index <= 0 {
		return errInvalidIndex
	}

	if index >= len(q.array) {
		return errIndexOutOfBounds
	}
	return nil
}

// peek returns the data from index and the number of bytes to encode the length of the data in uvarint format
// Peek返回来自index的数据和将数据长度编码为uvarint格式的字节数
func (q *BytesQueue) peek(index int) ([]byte, int, error) {
	err := q.peekCheckErr(index)
	if err != nil {
		return nil, 0, err
	}

	blockSize, n := binary.Uvarint(q.array[index:]) // 读取的长度
	return q.array[index+n : index+int(blockSize)], int(blockSize), nil
}

// canInsertAfterTail returns true if it's possible to insert an entry of size of need after the tail of the queue
// 如果可以在队列尾部插入一个大小为need的条目，则返回true
func (q *BytesQueue) canInsertAfterTail(need int) bool {
	// 已满
	if q.full {
		return false
	}
	// tail在head的右边
	if q.tail >= q.head {
		return q.capacity-q.tail >= need
	}
	// 1. there is exactly need bytes between head and tail, so we do not need
	// to reserve extra space for a potential empty entry when realloc this queue
	// 2. still have unused space between tail and head, then we must reserve
	// at least headerEntrySize bytes so we can put an empty entry
	// 回环之后head与tail交换位置了 即tail变为head，head变为tail，中间的为空洞
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}

// canInsertBeforeHead returns true if it's possible to insert an entry of size of need before the head of the queue
// 如果可以在队列头之前插入一个大小为need的条目，则返回true
func (q *BytesQueue) canInsertBeforeHead(need int) bool {
	if q.full {
		return false
	}
	// tail在最后并且未回环
	if q.tail >= q.head {
		return q.head-leftMarginIndex == need || q.head-leftMarginIndex >= need+minimumHeaderSize
	}
	return q.head-q.tail == need || q.head-q.tail >= need+minimumHeaderSize
}
