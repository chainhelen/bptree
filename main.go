package bptree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
	"syscall"
)

var (
	err error
	order          = 4
)

const (
	INVALID_OFFSET = 0xdeadbeef
	MAX_FREEBLOCKS = 100
)

var HasExistedKeyError = errors.New("hasExistedKey")
var NotFoundKey = errors.New("notFoundKey")
var InvalidDBFormat = errors.New("invalid db format")

type OFFTYPE uint64

type Tree struct {
	rootOff OFFTYPE
	nodePool *sync.Pool
	freeBlocks []OFFTYPE
	file *os.File
	blockSize uint64
	fileSize uint64
}

type Node struct {
	IsActive bool // 节点所在的磁盘空间是否在当前b+树内
	Children []OFFTYPE
	Self 	OFFTYPE
	Next     OFFTYPE
	Prev     OFFTYPE
	Parent   OFFTYPE
	Keys 	 []uint64
	Records  []string
	IsLeaf   bool
}

func NewTree(filename string) (*Tree, error) {
	var (
		stat syscall.Statfs_t
		fstat os.FileInfo
		err error
	)

	t := &Tree{}

	t.rootOff = INVALID_OFFSET
	t.nodePool = &sync.Pool{
		New: func()interface{}{
			return &Node{}
		},
	}
	t.freeBlocks = make([]OFFTYPE, 0, MAX_FREEBLOCKS)
	if t.file, err = os.OpenFile(filename, os.O_CREATE | os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	if err = syscall.Statfs(filename, &stat); err != nil {
		return nil, err
	}
	t.blockSize = uint64(stat.Bsize)
	if t.blockSize == 0 {
		return nil, errors.New("blockSize should be zero")
	}
	if fstat, err = t.file.Stat(); err != nil {
		return nil, err
	}

	t.fileSize = uint64(fstat.Size())
	if t.fileSize != 0 {
		if err = t.restructRootNode(); err != nil {
			return nil, err
		}
		if err = t.checkDiskBlockForFreeNodeList(); err != nil {
			return nil, err
		}
	}
	return t, nil
}


func (t *Tree)Close() error {
	if t.file != nil {
		t.file.Sync()
		return t.file.Close()
	}
	return nil
}

func (t *Tree)restructRootNode() error {
	var (
		err error
	)
	node := &Node{}

	for off := uint64(0); off < t.fileSize; off += t.blockSize {
		if err = t.seekNode(node, OFFTYPE(off)); err != nil {
			return err
		}
		if node.IsActive {
			break
		}
	}
	if !node.IsActive {
		return InvalidDBFormat
	}
	for node.Parent != INVALID_OFFSET {
		if err = t.seekNode(node, node.Parent); err != nil {
			return err
		}
	}

	t.rootOff = node.Self

	return nil
}

func (t *Tree)checkDiskBlockForFreeNodeList() error {
	var (
		err error
	)
	node := &Node{}
	bs := t.blockSize
	for off := uint64(0); off < t.fileSize && len(t.freeBlocks) < MAX_FREEBLOCKS; off += bs {
		if off + bs > t.fileSize {
			break
		}
		if err = t.seekNode(node, OFFTYPE(off)); err != nil {
			return err
		}
		if !node.IsActive {
			t.freeBlocks = append(t.freeBlocks, OFFTYPE(off))
		}
	}
	next_file := ((t.fileSize + 4095) / 4096) * 4096
	for len(t.freeBlocks) < MAX_FREEBLOCKS {
		t.freeBlocks = append(t.freeBlocks, OFFTYPE(next_file))
		next_file += bs
	}
	t.fileSize = next_file
	return nil
}

func (t *Tree) initNodeForUsage(node *Node) {
	node.IsActive = true
	node.Children = nil
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) clearNodeForUsage(node *Node) {
	node.IsActive = false
	node.Children = nil
	node.Self = INVALID_OFFSET
	node.Next = INVALID_OFFSET
	node.Prev = INVALID_OFFSET
	node.Parent = INVALID_OFFSET
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree)seekNode(node *Node, off OFFTYPE) error {
	if node == nil {
		return fmt.Errorf("cant use nil for seekNode")
	}
	t.clearNodeForUsage(node)

	var err error
	buf := make([]byte, 8)
	if n, err := t.file.ReadAt(buf, int64(off)); err != nil {
		return err
	} else if uint64(n) != 8 {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", off, t.file.Name(), 4, n)
	}
	bs := bytes.NewBuffer(buf)

	dataLen := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return err
	}
	if uint64(dataLen) + 8 > t.blockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen) + 4, t.blockSize)
	}

	buf = make([]byte, dataLen)
	if n, err := t.file.ReadAt(buf, int64(off) + 8); err != nil {
		return err
	} else if uint64(n) != uint64(dataLen) {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", int64(off) + 4, t.file.Name(), dataLen, n)
	}

	bs = bytes.NewBuffer(buf)

	// IsActive
	if err = binary.Read(bs, binary.LittleEndian, &node.IsActive); err != nil {
		return err
	}

	// Children
	childCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &childCount); err != nil {
		return err
	}
	node.Children = make([]OFFTYPE, childCount)
	for i := uint8(0);i < childCount;i++ {
		child := uint64(0)
		if err = binary.Read(bs, binary.LittleEndian, &child); err != nil {
			return err
		}
		node.Children[i] = OFFTYPE(child)
	}

	// Self
	self := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &self); err != nil {
		return err
	}
	node.Self = OFFTYPE(self)

	// Next
	next := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &next); err != nil {
		return err
	}
	node.Next = OFFTYPE(next)

	// Prev
	prev := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &prev); err != nil {
		return err
	}
	node.Prev = OFFTYPE(prev)

	// Parent
	parent := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &parent); err != nil {
		return err
	}
	node.Parent = OFFTYPE(parent)

	// Keys
	keysCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &keysCount); err != nil {
		return err
	}
	node.Keys = make([]uint64, keysCount)
	for i := uint8(0); i < keysCount;i++ {
		if err = binary.Read(bs, binary.LittleEndian, &node.Keys[i]); err != nil {
			return err
		}
	}

	// Records
	recordCount := uint8(0)
	if err = binary.Read(bs, binary.LittleEndian, &recordCount); err != nil {
		return err
	}
	node.Records = make([]string, recordCount)
	for i := uint8(0); i < recordCount;i++ {
		l := uint8(0)
		if err = binary.Read(bs, binary.LittleEndian, &l); err != nil {
			return err
		}
		v := make([]byte, l)
		if err = binary.Read(bs, binary.LittleEndian, &v); err != nil {
			return err
		}
		node.Records[i] = string(v)
	}

	// IsLeaf
	if err = binary.Read(bs, binary.LittleEndian, &node.IsLeaf); err != nil {
		return err
	}

	return nil
}

func (t *Tree)flushNodesAndPutNodesPool(nodes ...*Node) error {
	for _, n := range nodes {
		if err := t.flushNodeAndPutNodePool(n); err != nil {
			return err
		}
	}
	return err
}

func (t *Tree)flushNodeAndPutNodePool(n *Node) error {
	if err := t.flushNode(n); err != nil {
		return err
	}
	t.putNodePool(n)
	return nil
}

func (t *Tree)putNodePool(n *Node) {
	t.nodePool.Put(n)
}

func (t *Tree)flushNode(n *Node) error {
	if n == nil {
		return fmt.Errorf("flushNode == nil")
	}
	if t.file == nil {
		return fmt.Errorf("flush node into disk, but not open file")
	}

	var (
		length int
		err error
	)

	bs := bytes.NewBuffer(make([]byte, 0))

	// IsActive
	if err = binary.Write(bs, binary.LittleEndian, n.IsActive); err != nil {
		return nil
	}

	// Children
	childCount := uint8(len(n.Children))
	if err = binary.Write(bs, binary.LittleEndian, childCount); err != nil {
		return err
	}
	for _, v := range n.Children {
		if err = binary.Write(bs, binary.LittleEndian, uint64(v)); err != nil {
			return err
		}
	}

	// Self
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Self)); err != nil {
		return err
	}

	// Next
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Next)); err != nil {
		return err
	}

	// Prev
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Prev)); err != nil {
		return err
	}

	// Parent
	if err = binary.Write(bs, binary.LittleEndian, uint64(n.Parent)); err != nil {
		return err
	}

	// Keys
	keysCount := uint8(len(n.Keys))
	if err = binary.Write(bs, binary.LittleEndian, keysCount); err != nil {
		return err
	}
	for _, v := range n.Keys {
		if err = binary.Write(bs, binary.LittleEndian, v); err != nil {
			return err
		}
	}

	// Record
	recordCount := uint8(len(n.Records))
	if err = binary.Write(bs, binary.LittleEndian, recordCount); err != nil {
		return err
	}
	for _, v := range n.Records {
		if err = binary.Write(bs, binary.LittleEndian, uint8(len([]byte(v)))); err != nil {
			return err
		}
		if err = binary.Write(bs, binary.LittleEndian, []byte(v)); err != nil {
			return err
		}
	}

	// IsLeaf
	if err = binary.Write(bs, binary.LittleEndian, n.IsLeaf); err != nil {
		return err
	}

	dataLen := len(bs.Bytes())
	if uint64(dataLen) + 8 > t.blockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen) + 4, t.blockSize)
	}
	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}

	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.file.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.file.Name(), len(data), length)
	}
	return nil
}

func (t *Tree) newMappingNodeFromPool(off OFFTYPE) (*Node, error) {
	node := t.nodePool.Get().(*Node)
	t.initNodeForUsage(node)
	if off == INVALID_OFFSET {
		return node, nil
	}
	t.clearNodeForUsage(node)
	if err := t.seekNode(node, off); err != nil {
		return nil, err
	}
	return node, nil
}

func (t *Tree)newNodeFromDisk()(*Node, error) {
	var (
		node *Node
		err error
	)
	node = t.nodePool.Get().(*Node)
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		return node, nil
	}
	if err = t.checkDiskBlockForFreeNodeList(); err != nil {
		return nil, err
	}
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		return node, nil
	}
	return nil, fmt.Errorf("can't not alloc more node")
}

func (t *Tree) putFreeBlocks(off OFFTYPE) {
	if len(t.freeBlocks) >= MAX_FREEBLOCKS {
		return
	}
	t.freeBlocks = append(t.freeBlocks, off)
}

func (t *Tree) Find(key uint64) (string, error) {
	var (
		node *Node
		err error
	)

	if t.rootOff == INVALID_OFFSET {
		return "", nil
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return "", err
	}

	if err = t.findLeaf(node, key); err != nil {
		return "", err
	}
	defer t.putNodePool(node)

	for i, nkey := range node.Keys {
		if nkey == key {
			return node.Records[i], nil
		}
	}
	return "", NotFoundKey
}

func (t *Tree)findLeaf(node *Node, key uint64) error {
	var (
		err error
		root *Node
	)

	c := t.rootOff
	if c == INVALID_OFFSET {
		return nil
	}

	if root, err = t.newMappingNodeFromPool(c); err != nil {
		return err
	}
	defer t.putNodePool(root)

	*node = *root

	for !node.IsLeaf {
		idx := sort.Search(len(node.Keys), func(i int) bool {
			return  key <= node.Keys[i]
		})
		if idx == len(node.Keys) {
			idx = len(node.Keys) - 1
		}
		if err = t.seekNode(node, node.Children[idx]); err != nil {
			return err
		}
	}
	return nil
}

func cut(length int) int {
	return (length + 1) / 2
}

func insertKeyValIntoLeaf(n *Node, key uint64, rec string) (int,  error){
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return key <= n.Keys[i]
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, HasExistedKeyError
	}

	n.Keys = append(n.Keys, key)
	n.Records = append(n.Records, rec)
	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Records[i] = n.Records[i-1]
	}
	n.Keys[idx] = key
	n.Records[idx] = rec
	return idx, nil
}

func insertKeyValIntoNode(n *Node, key uint64, child OFFTYPE) (int,  error){
	idx := sort.Search(len(n.Keys), func(i int) bool {
		return key <= n.Keys[i]
	})
	if idx < len(n.Keys) && n.Keys[idx] == key {
		return 0, HasExistedKeyError
	}

	n.Keys = append(n.Keys, key)
	n.Children = append(n.Children, child)
	for i := len(n.Keys) - 1; i > idx; i-- {
		n.Keys[i] = n.Keys[i-1]
		n.Children[i] = n.Children[i-1]
	}
	n.Keys[idx] = key
	n.Children[idx] = child
	return idx, nil
}


func removeKeyFromLeaf(leaf *Node, idx int) {
	tmpKeys := append([]uint64{}, leaf.Keys[idx + 1:]...)
	leaf.Keys = append(leaf.Keys[:idx], tmpKeys...)

	tmpRecords := append([]string{}, leaf.Records[idx + 1:]...)
	leaf.Records = append(leaf.Records[:idx], tmpRecords...)
}

func removeKeyFromNode(node *Node, idx int) {
	tmpKeys := append([]uint64{}, node.Keys[idx + 1:]...)
	node.Keys = append(node.Keys[:idx], tmpKeys...)

	tmpChildren := append([]OFFTYPE{}, node.Children[idx + 1:]...)
	node.Children = append(node.Children[:idx], tmpChildren...)
}

func (t *Tree)splitLeafIntoTowleaves(leaf *Node, new_leaf *Node) error {
	var (
		i, split int
	)
	split = cut(order)

	for i = split;i <= order;i++ {
		new_leaf.Keys = append(new_leaf.Keys, leaf.Keys[i])
		new_leaf.Records = append(new_leaf.Records, leaf.Records[i])
	}

	// adjust relation
	leaf.Keys = leaf.Keys[:split]
	leaf.Records = leaf.Records[:split]

	new_leaf.Next = leaf.Next
	leaf.Next = new_leaf.Self
	new_leaf.Prev = leaf.Self

	new_leaf.Parent = leaf.Parent

	if new_leaf.Next != INVALID_OFFSET {
		var (
			nextNode *Node
			err error
		)
		if nextNode, err = t.newMappingNodeFromPool(new_leaf.Next); err != nil {
			return err
		}
		nextNode.Prev = new_leaf.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}

	return err
}

func (t *Tree)insertIntoLeaf(key uint64, rec string) error {
	var (
		leaf *Node
		err error
		idx int
		new_leaf *Node
	)

	if leaf, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}

	if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
		return err
	}

	// update the last key of parent's if necessary
	if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
		return err
	}

	// insert key/val into leaf
	if len(leaf.Keys) <= order {
		return t.flushNodeAndPutNodePool(leaf)
	}

	// split leaf so new leaf node
	if new_leaf, err = t.newNodeFromDisk(); err != nil {
		return err
	}
	new_leaf.IsLeaf = true
	if err = t.splitLeafIntoTowleaves(leaf, new_leaf); err != nil {
		return err
	}

	if err = t.flushNodesAndPutNodesPool(new_leaf, leaf); err != nil {
		return err
	}

	// insert split key into parent
	return t.insertIntoParent(leaf.Parent, leaf.Self, leaf.Keys[len(leaf.Keys) - 1], new_leaf.Self)
}

func getIndex(keys []uint64, key uint64) int {
	idx := sort.Search(len(keys), func(i int) bool {
		return key <= keys[i]
	})
	return idx
}

func insertIntoNode(parent *Node, idx int, left_off OFFTYPE, key uint64, right_off OFFTYPE) {
	var (
		i int
	)
	parent.Keys = append(parent.Keys, key)
	for i = len(parent.Keys) - 1; i > idx; i-- {
		parent.Keys[i] = parent.Keys[i - 1]
	}
	parent.Keys[idx] = key

	if idx == len(parent.Children) {
		parent.Children = append(parent.Children, right_off)
		return
	}
	tmpChildren := append([]OFFTYPE{}, parent.Children[idx+1:]...)
	parent.Children = append(append(parent.Children[:idx + 1], right_off), tmpChildren...)
}

func (t *Tree) insertIntoNodeAfterSplitting(old_node *Node) error {
	var (
		newNode, child, nextNode *Node
		err error
		i, split int
	)

	if newNode, err = t.newNodeFromDisk(); err != nil {
		return err
	}

	split = cut(order)

	for i = split;i <= order;i++ {
		newNode.Children = append(newNode.Children, old_node.Children[i])
		newNode.Keys = append(newNode.Keys, old_node.Keys[i])

		// update new_node children relation
		if child, err = t.newMappingNodeFromPool(old_node.Children[i]); err != nil {
			return err
		}
		child.Parent = newNode.Self
		if err = t.flushNodesAndPutNodesPool(child); err != nil {
			return err
		}
	}
	newNode.Parent = old_node.Parent

	old_node.Children = old_node.Children[:split]
	old_node.Keys = old_node.Keys[:split]

	newNode.Next = old_node.Next
	old_node.Next = newNode.Self
	newNode.Prev = old_node.Self

	if newNode.Next != INVALID_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(newNode.Next); err != nil {
			return err
		}
		nextNode.Prev = newNode.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}


	if err = t.flushNodesAndPutNodesPool(old_node, newNode); err != nil {
		return err
	}

	return t.insertIntoParent(old_node.Parent, old_node.Self, old_node.Keys[len(old_node.Keys) - 1], newNode.Self)
}

func (t *Tree)insertIntoParent(parent_off OFFTYPE, left_off OFFTYPE, key uint64, right_off OFFTYPE) error {
	var (
		idx int
		parent *Node
		err error
		left *Node
		right *Node
	)
	if parent_off ==  OFFTYPE(INVALID_OFFSET){
		if left, err = t.newMappingNodeFromPool(left_off); err != nil {
			return err
		}
		if right, err = t.newMappingNodeFromPool(right_off); err != nil {
			return err
		}
		if err = t.newRootNode(left, right); err != nil {
			return err
		}
		return t.flushNodesAndPutNodesPool(left, right)
	}

	if parent, err = t.newMappingNodeFromPool(parent_off); err != nil {
		return err
	}

	idx = getIndex(parent.Keys, key)
	insertIntoNode(parent, idx, left_off, key, right_off)

	if len(parent.Keys) <= order {
		return t.flushNodesAndPutNodesPool(parent)
	}


	return t.insertIntoNodeAfterSplitting(parent)
}

func (t *Tree) newRootNode(left *Node, right *Node) error {
	var (
		root *Node
		err error
	)

	if root, err = t.newNodeFromDisk(); err != nil {
		return err
	}
	root.Keys = append(root.Keys, left.Keys[len(left.Keys) - 1])
	root.Keys = append(root.Keys, right.Keys[len(right.Keys) - 1])
	root.Children = append(root.Children, left.Self)
	root.Children = append(root.Children, right.Self)
	left.Parent = root.Self
	right.Parent = root.Self

	t.rootOff = root.Self
	return t.flushNodeAndPutNodePool(root)
}

func (t *Tree) Insert(key uint64, val string) error {
	var (
		err  error
		node *Node
	)

	if t.rootOff == INVALID_OFFSET {
		if node, err = t.newNodeFromDisk(); err != nil {
			return err
		}
		t.rootOff = node.Self
		node.IsActive = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true
		return t.flushNodeAndPutNodePool(node)
	}

	return t.insertIntoLeaf(key, val)
}

func (t *Tree) Update(key uint64, val string) error {
	var (
		node *Node
		err error
	)

	if t.rootOff == INVALID_OFFSET {
		return NotFoundKey
	}

	if node, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(node, key); err != nil {
		return err
	}

	for i, nkey := range node.Keys {
		if nkey == key {
			node.Records[i] = val
			return t.flushNodesAndPutNodesPool(node)
		}
	}
	return NotFoundKey
}

func (t *Tree) mayUpdatedLastParentKey(leaf *Node, idx int) error {
	// update the last key of parent's if necessary
	if idx == len(leaf.Keys) - 1 && leaf.Parent != INVALID_OFFSET {
		key := leaf.Keys[len(leaf.Keys) - 1]
		updateNodeOff := leaf.Parent
		var (
			updateNode *Node
			node *Node
		)

		if node, err = t.newMappingNodeFromPool(leaf.Self); err != nil {
			return err
		}
		*node = *leaf
		defer t.putNodePool(node)

		for updateNodeOff != INVALID_OFFSET && idx == len(node.Keys) - 1 {
			if updateNode, err = t.newMappingNodeFromPool(updateNodeOff); err != nil {
				return err
			}
			for i, v := range updateNode.Children {
				if v == node.Self {
					idx = i
					break
				}
			}
			updateNode.Keys[idx] = key
			if err = t.flushNodeAndPutNodePool(updateNode); err != nil {
				return err
			}
			updateNodeOff = updateNode.Parent
			*node = *updateNode
		}
	}
	return nil
}

func (t *Tree) deleteKeyFromNode (off OFFTYPE, key uint64) error {
	if off == INVALID_OFFSET {
		return nil
	}
	var (
		node *Node
		nextNode *Node
		prevNode *Node
		newRoot *Node
		childNode *Node
		idx int
		err error
	)
	if node, err = t.newMappingNodeFromPool(off); err != nil {
		return err
	}
	idx = getIndex(node.Keys, key)
	removeKeyFromNode(node, idx)

	// update the last key of parent's if necessary
	if idx == len(node.Keys) {
		if err = t.mayUpdatedLastParentKey(node, idx - 1); err != nil {
			return err
		}
	}

	// if statisfied len
	if len(node.Keys) >= order / 2 {
		return t.flushNodesAndPutNodesPool(node)
	}

	if off == t.rootOff && len(node.Keys) == 1 {
		if newRoot, err = t.newMappingNodeFromPool(node.Children[0]); err != nil {
			return err
		}
		node.IsActive = false
		newRoot.Parent = INVALID_OFFSET
		t.rootOff = newRoot.Self
		return t.flushNodesAndPutNodesPool(node, newRoot)
	}

	if node.Next != INVALID_OFFSET {
		if nextNode, err = t.newMappingNodeFromPool(node.Next); err != nil {
			return err
		}
		// lease from next node
		if len(nextNode.Keys) > order / 2 {
			key := nextNode.Keys[0]
			child := nextNode.Children[0]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(nextNode, 0)
			if idx , err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(node, idx); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(node, nextNode, childNode)
		}
		// merge nextNode and curNode
		if node.Prev != INVALID_OFFSET {
			if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
				return err
			}
			prevNode.Next = nextNode.Self
			nextNode.Prev = prevNode.Self
			if err = t.flushNodesAndPutNodesPool(prevNode); err != nil {
				return err
			}
		} else {
			nextNode.Prev = INVALID_OFFSET
		}

		nextNode.Keys = append(node.Keys, nextNode.Keys...)
		nextNode.Children = append(node.Children, nextNode.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = nextNode.Self
			if err = t.flushNodesAndPutNodesPool(childNode); err != nil {
				return err
			}
		}

		node.IsActive = false
		t.putFreeBlocks(node.Self)

		if err = t.flushNodesAndPutNodesPool(node, nextNode); err != nil {
			return err
		}

		// delete parent's key recursively
		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys) - 1])
	}

	// come here because node.Next = INVALID_OFFSET
	if node.Prev != INVALID_OFFSET {
		if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevNode.Keys) > order / 2 {
			key := prevNode.Keys[len(prevNode.Keys) - 1]
			child := prevNode.Children[len(prevNode.Children) - 1]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(prevNode, len(prevNode.Keys) - 1)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevNode, len(prevNode.Keys) - 1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevNode, node, childNode)
		}
		// merge prevNode and curNode
		prevNode.Next = INVALID_OFFSET
		prevNode.Keys = append(prevNode.Keys, node.Keys...)
		prevNode.Children = append(prevNode.Children, node.Children...)

		// update child's parent
		for _, v := range node.Children {
			if childNode, err = t.newMappingNodeFromPool(v); err != nil {
				return err
			}
			childNode.Parent = prevNode.Self
			if err = t.flushNodesAndPutNodesPool(childNode); err != nil {
				return err
			}
		}

		node.IsActive = false
		t.putFreeBlocks(node.Self)

		if err = t.flushNodesAndPutNodesPool(node, prevNode); err != nil {
			return err
		}

		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys) - 2])
	}
	return nil
}

func (t *Tree) deleteKeyFromLeaf (key uint64) error {
	var (
		leaf *Node
		prevLeaf *Node
		nextLeaf *Node
		err error
		idx int
	)
	if leaf, err = t.newMappingNodeFromPool(INVALID_OFFSET); err != nil {
		return err
	}

	if err = t.findLeaf(leaf, key); err != nil {
		return err
	}

	idx = getIndex(leaf.Keys, key)
	if idx == len(leaf.Keys) || leaf.Keys[idx] != key {
		t.putNodePool(leaf)
		return fmt.Errorf("not found key:%d", key)

	}
	removeKeyFromLeaf(leaf, idx)

	// if leaf is root
	if leaf.Self == t.rootOff {
		return t.flushNodesAndPutNodesPool(leaf)
	}

	// update the last key of parent's if necessary
	if idx == len(leaf.Keys) {
		if err = t.mayUpdatedLastParentKey(leaf, idx - 1); err != nil {
			return err
		}
	}

	// if satisfied len
	if len(leaf.Keys) >= order / 2 {
		return t.flushNodesAndPutNodesPool(leaf)
	}

	if leaf.Next != INVALID_OFFSET {
		if nextLeaf, err = t.newMappingNodeFromPool(leaf.Next); err != nil {
			return err
		}
		// lease from next leaf
		if len(nextLeaf.Keys) > order / 2 {
			key := nextLeaf.Keys[0]
			rec := nextLeaf.Records[0]
			removeKeyFromLeaf(nextLeaf, 0)
			if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(leaf, idx); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(nextLeaf, leaf)
		}

		// merge nextLeaf and curleaf
		if leaf.Prev != INVALID_OFFSET {
			if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
				return err
			}
			prevLeaf.Next = nextLeaf.Self
			nextLeaf.Prev = prevLeaf.Self
			if err = t.flushNodesAndPutNodesPool(prevLeaf); err != nil {
				return err
			}
		} else {
			nextLeaf.Prev = INVALID_OFFSET
		}
		
		nextLeaf.Keys = append(leaf.Keys, nextLeaf.Keys...)
		nextLeaf.Records = append(leaf.Records, nextLeaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushNodesAndPutNodesPool(leaf, nextLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys) - 1])
	}

	// come here because leaf.Next = INVALID_OFFSET
	if leaf.Prev != INVALID_OFFSET {
		if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevLeaf.Keys) > order / 2 {
			key := prevLeaf.Keys[len(prevLeaf.Keys) - 1]
			rec := prevLeaf.Records[len(prevLeaf.Records) - 1]
			removeKeyFromLeaf(prevLeaf, len(prevLeaf.Keys) - 1)
			if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevLeaf, len(prevLeaf.Keys) - 1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevLeaf, leaf)
		}
		// merge prevleaf and curleaf
		prevLeaf.Next = INVALID_OFFSET
		prevLeaf.Keys = append(prevLeaf.Keys, leaf.Keys...)
		prevLeaf.Records = append(prevLeaf.Records, leaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushNodesAndPutNodesPool(leaf, prevLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys) - 2])
	}

	return nil
}

func (t *Tree) Delete (key uint64) error {
	if t.rootOff == INVALID_OFFSET {
		return fmt.Errorf("not found key:%d", key)
	}
	return t.deleteKeyFromLeaf(key)
}

func (t *Tree) ScanTreePrint() error {
	if t.rootOff == INVALID_OFFSET {
		return fmt.Errorf("root = nil")
	}
	Q := make([]OFFTYPE, 0)
	Q = append(Q, t.rootOff)

	floor := 0
	var (
		curNode *Node
		err error
	)
	for 0 != len(Q) {
		floor++

		l := len(Q)
		fmt.Printf("floor %3d:", floor)
		for i := 0;i < l;i++{
			if curNode, err = t.newMappingNodeFromPool(Q[i]); err != nil {
				return err
			}
			defer t.putNodePool(curNode)

			// print keys
			if i == l - 1 {
				fmt.Printf("%d\n", curNode.Keys)
			} else {
				fmt.Printf("%d, ", curNode.Keys)
			}
			for _, v := range curNode.Children {
				Q = append(Q, v)
			}
		}
		Q = Q[l:]
	}
	return nil
}