package bptree

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"sort"
	"sync"
)

var (
	err   error
	order = 4
)

const (
	InvalidOffset = 0xdeadbeef
	MaxFreeBlocks = 100
	DBNAME        = "data.db"
	META          = "meta.dat"
	WAL           = "wal.log"
)

var HasExistedKeyError = errors.New("hasExistedKey")
var NotFoundKey = errors.New("notFoundKey")
var InvalidDBFormat = errors.New("invalid db format")
var InnerWalError = errors.New("inner wal file error")
var InnerMetaError = errors.New("inner meta file error")

type OFFTYPE uint64

type Tree struct {
	rootOff             OFFTYPE
	nodePool            *sync.Pool
	freeBlocks          []OFFTYPE
	dbFile              *os.File
	metaFile            *os.File
	walFile             *os.File
	txid                uint64
	blockSize           uint64
	fileSize            uint64
	flushNodeHookHandle func(n *Node) error
	sync.RWMutex
}

func (t *Tree) restructRootNode() error {
	rootOff := uint64(0)
	txid := uint64(0)
	_, err = fmt.Fscanf(t.metaFile, "rootOff:%020d,txid:%020d", &rootOff, &txid)
	if err != nil {
		return err
	}
	t.rootOff = OFFTYPE(rootOff)
	t.txid = txid
	// TODO need to optimize
	// rollback(t.txid, t)
	return nil
}

func (t *Tree) checkDiskBlockForFreeNodeList() error {
	var (
		err error
	)
	node := &Node{}
	bs := t.blockSize
	for off := uint64(0); off < t.fileSize && len(t.freeBlocks) < MaxFreeBlocks; off += bs {
		if off+bs > t.fileSize {
			break
		}
		if err = t.seekNode(node, OFFTYPE(off)); err != nil {
			return err
		}
		if !node.IsActive {
			t.freeBlocks = append(t.freeBlocks, OFFTYPE(off))
		}
	}
	nextFile := ((t.fileSize + 4095) / 4096) * 4096
	for len(t.freeBlocks) < MaxFreeBlocks {
		t.freeBlocks = append(t.freeBlocks, OFFTYPE(nextFile))
		nextFile += bs
	}
	t.fileSize = nextFile
	return nil
}

func (t *Tree) initNodeForUsage(node *Node) {
	node.IsActive = true
	node.Children = nil
	node.Self = InvalidOffset
	node.Next = InvalidOffset
	node.Prev = InvalidOffset
	node.Parent = InvalidOffset
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) clearNodeForUsage(node *Node) {
	node.IsActive = false
	node.Children = nil
	node.Self = InvalidOffset
	node.Next = InvalidOffset
	node.Prev = InvalidOffset
	node.Parent = InvalidOffset
	node.Keys = nil
	node.Records = nil
	node.IsLeaf = false
}

func (t *Tree) seekNode(node *Node, off OFFTYPE) error {
	if node == nil {
		return fmt.Errorf("cant use nil for seekNode")
	}
	t.clearNodeForUsage(node)

	var err error
	buf := make([]byte, 8)
	if n, err := t.dbFile.ReadAt(buf, int64(off)); err != nil {
		return err
	} else if uint64(n) != 8 {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", off, t.dbFile.Name(), 4, n)
	}
	bs := bytes.NewBuffer(buf)

	dataLen := uint64(0)
	if err = binary.Read(bs, binary.LittleEndian, &dataLen); err != nil {
		return err
	}
	if uint64(dataLen)+8 > t.blockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen)+4, t.blockSize)
	}

	buf = make([]byte, dataLen)
	if n, err := t.dbFile.ReadAt(buf, int64(off)+8); err != nil {
		return err
	} else if uint64(n) != uint64(dataLen) {
		return fmt.Errorf("readat %d from %s, expected len = %d but get %d", int64(off)+4, t.dbFile.Name(), dataLen, n)
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
	for i := uint8(0); i < childCount; i++ {
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
	for i := uint8(0); i < keysCount; i++ {
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
	for i := uint8(0); i < recordCount; i++ {
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

func (t *Tree) flushNodesAndPutNodesPool(nodes ...*Node) error {
	for _, n := range nodes {
		// wal
		if err := WriteWalRedoNode(t.walFile, n); err != nil {
			return err
		}

		if nil != t.flushNodeHookHandle {
			if err := t.flushNodeHookHandle(n); err != nil {
				t.nodePool.Put(n)
				return err
			}
		}

		if err := t.flushNode(n); err != nil {
			return err
		}
		t.putNodePool(n)
	}
	return err
}

func (t *Tree) putNodePool(n *Node) {
	t.nodePool.Put(n)
}

func (t *Tree) flushNode(n *Node) error {
	var (
		length int
		err    error
	)

	if n == nil {
		return fmt.Errorf("flushNode == nil")
	}
	if t.dbFile == nil {
		return fmt.Errorf("flush node into disk, but not open dbFile")
	}

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
	if uint64(dataLen)+8 > t.blockSize {
		return fmt.Errorf("flushNode len(node) = %d exceed t.blockSize %d", uint64(dataLen)+4, t.blockSize)
	}
	tmpbs := bytes.NewBuffer(make([]byte, 0))
	if err = binary.Write(tmpbs, binary.LittleEndian, uint64(dataLen)); err != nil {
		return err
	}

	data := append(tmpbs.Bytes(), bs.Bytes()...)
	if length, err = t.dbFile.WriteAt(data, int64(n.Self)); err != nil {
		return err
	} else if len(data) != length {
		return fmt.Errorf("writeat %d into %s, expected len = %d but get %d", int64(n.Self), t.dbFile.Name(), len(data), length)
	}
	return nil
}

func (t *Tree) newMappingNodeFromPool(off OFFTYPE) (*Node, error) {
	node := t.nodePool.Get().(*Node)
	t.initNodeForUsage(node)
	if off == InvalidOffset {
		if err := WriteWalUndoNode(t.walFile, node); err != nil {
			t.nodePool.Put(node)
			return nil, err
		}
		return node, nil
	}
	t.clearNodeForUsage(node)
	if err := t.seekNode(node, off); err != nil {
		t.nodePool.Put(node)
		return nil, err
	}
	if err := WriteWalUndoNode(t.walFile, node); err != nil {
		t.nodePool.Put(node)
		return nil, err
	}
	return node, nil
}

func (t *Tree) newNodeFromDisk() (*Node, error) {
	var (
		node *Node
		err  error
	)
	node = t.nodePool.Get().(*Node)
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		if err := WriteWalUndoNode(t.walFile, node); err != nil {
			t.nodePool.Put(node)
			return nil, err
		}
		return node, nil
	}
	if err = t.checkDiskBlockForFreeNodeList(); err != nil {
		t.nodePool.Put(node)
		return nil, err
	}
	if len(t.freeBlocks) > 0 {
		off := t.freeBlocks[0]
		t.freeBlocks = t.freeBlocks[1:len(t.freeBlocks)]
		t.initNodeForUsage(node)
		node.Self = off
		if err := WriteWalUndoNode(t.walFile, node); err != nil {
			t.nodePool.Put(node)
			return nil, err
		}
		return node, nil
	}
	t.nodePool.Put(node)
	return nil, fmt.Errorf("can't not alloc more node")
}

func (t *Tree) putFreeBlocks(off OFFTYPE) {
	if len(t.freeBlocks) >= MaxFreeBlocks {
		return
	}
	t.freeBlocks = append(t.freeBlocks, off)
}

func (t *Tree) findLeaf(node *Node, key uint64) error {
	var (
		err  error
		root *Node
	)

	c := t.rootOff
	if c == InvalidOffset {
		return nil
	}

	if root, err = t.newMappingNodeFromPool(c); err != nil {
		return err
	}
	defer t.putNodePool(root)

	*node = *root

	for !node.IsLeaf {
		idx := sort.Search(len(node.Keys), func(i int) bool {
			return key <= node.Keys[i]
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

func (t *Tree) splitLeafIntoTowLeaves(leaf *Node, newLeaf *Node) error {
	var (
		i, split int
	)
	split = cut(order)

	for i = split; i <= order; i++ {
		newLeaf.Keys = append(newLeaf.Keys, leaf.Keys[i])
		newLeaf.Records = append(newLeaf.Records, leaf.Records[i])
	}

	// adjust relation
	leaf.Keys = leaf.Keys[:split]
	leaf.Records = leaf.Records[:split]

	newLeaf.Next = leaf.Next
	leaf.Next = newLeaf.Self
	newLeaf.Prev = leaf.Self

	newLeaf.Parent = leaf.Parent

	if newLeaf.Next != InvalidOffset {
		var (
			nextNode *Node
			err      error
		)
		if nextNode, err = t.newMappingNodeFromPool(newLeaf.Next); err != nil {
			return err
		}
		nextNode.Prev = newLeaf.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}

	return err
}

func (t *Tree) insertIntoLeaf(key uint64, rec string) error {
	var (
		leaf    *Node
		err     error
		idx     int
		newLeaf *Node
	)

	if leaf, err = t.newMappingNodeFromPool(InvalidOffset); err != nil {
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
		return t.flushNodesAndPutNodesPool(leaf)
	}

	// split leaf so new leaf node
	if newLeaf, err = t.newNodeFromDisk(); err != nil {
		return err
	}
	newLeaf.IsLeaf = true
	if err = t.splitLeafIntoTowLeaves(leaf, newLeaf); err != nil {
		return err
	}

	if err = t.flushNodesAndPutNodesPool(newLeaf, leaf); err != nil {
		return err
	}

	// insert split key into parent
	return t.insertIntoParent(leaf.Parent, leaf.Self, leaf.Keys[len(leaf.Keys)-1], newLeaf.Self)
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
		parent.Keys[i] = parent.Keys[i-1]
	}
	parent.Keys[idx] = key

	if idx == len(parent.Children) {
		parent.Children = append(parent.Children, right_off)
		return
	}
	tmpChildren := append([]OFFTYPE{}, parent.Children[idx+1:]...)
	parent.Children = append(append(parent.Children[:idx+1], right_off), tmpChildren...)
}

func (t *Tree) insertIntoNodeAfterSplitting(oldNode *Node) error {
	var (
		newNode, child, nextNode *Node
		err                      error
		i, split                 int
	)

	if newNode, err = t.newNodeFromDisk(); err != nil {
		return err
	}

	split = cut(order)

	for i = split; i <= order; i++ {
		newNode.Children = append(newNode.Children, oldNode.Children[i])
		newNode.Keys = append(newNode.Keys, oldNode.Keys[i])

		// update new_node children relation
		if child, err = t.newMappingNodeFromPool(oldNode.Children[i]); err != nil {
			return err
		}
		child.Parent = newNode.Self
		if err = t.flushNodesAndPutNodesPool(child); err != nil {
			return err
		}
	}
	newNode.Parent = oldNode.Parent

	oldNode.Children = oldNode.Children[:split]
	oldNode.Keys = oldNode.Keys[:split]

	newNode.Next = oldNode.Next
	oldNode.Next = newNode.Self
	newNode.Prev = oldNode.Self

	if newNode.Next != InvalidOffset {
		if nextNode, err = t.newMappingNodeFromPool(newNode.Next); err != nil {
			return err
		}
		nextNode.Prev = newNode.Self
		if err = t.flushNodesAndPutNodesPool(nextNode); err != nil {
			return err
		}
	}

	if err = t.flushNodesAndPutNodesPool(oldNode, newNode); err != nil {
		return err
	}

	return t.insertIntoParent(oldNode.Parent, oldNode.Self, oldNode.Keys[len(oldNode.Keys)-1], newNode.Self)
}

func (t *Tree) insertIntoParent(parentOff OFFTYPE, leftOff OFFTYPE, key uint64, rightOff OFFTYPE) error {
	var (
		idx    int
		parent *Node
		err    error
		left   *Node
		right  *Node
	)
	if parentOff == OFFTYPE(InvalidOffset) {
		if left, err = t.newMappingNodeFromPool(leftOff); err != nil {
			return err
		}
		if right, err = t.newMappingNodeFromPool(rightOff); err != nil {
			return err
		}
		if err = t.newRootNode(left, right); err != nil {
			return err
		}
		return t.flushNodesAndPutNodesPool(left, right)
	}

	if parent, err = t.newMappingNodeFromPool(parentOff); err != nil {
		return err
	}

	idx = getIndex(parent.Keys, key)
	insertIntoNode(parent, idx, leftOff, key, rightOff)

	if len(parent.Keys) <= order {
		return t.flushNodesAndPutNodesPool(parent)
	}

	return t.insertIntoNodeAfterSplitting(parent)
}

func (t *Tree) newRootNode(left *Node, right *Node) error {
	var (
		root *Node
		err  error
	)

	if root, err = t.newNodeFromDisk(); err != nil {
		return err
	}
	root.Keys = append(root.Keys, left.Keys[len(left.Keys)-1])
	root.Keys = append(root.Keys, right.Keys[len(right.Keys)-1])
	root.Children = append(root.Children, left.Self)
	root.Children = append(root.Children, right.Self)
	left.Parent = root.Self
	right.Parent = root.Self

	t.rootOff = root.Self
	return t.flushNodesAndPutNodesPool(root)
}

func (t *Tree) mayUpdatedLastParentKey(leaf *Node, idx int) error {
	// update the last key of parent's if necessary
	if idx == len(leaf.Keys)-1 && leaf.Parent != InvalidOffset {
		key := leaf.Keys[len(leaf.Keys)-1]
		updateNodeOff := leaf.Parent
		var (
			updateNode *Node
			node       *Node
		)

		if node, err = t.newMappingNodeFromPool(leaf.Self); err != nil {
			return err
		}
		*node = *leaf
		defer t.putNodePool(node)

		for updateNodeOff != InvalidOffset && idx == len(node.Keys)-1 {
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
			if err = t.flushNodesAndPutNodesPool(updateNode); err != nil {
				return err
			}
			updateNodeOff = updateNode.Parent
			*node = *updateNode
		}
	}
	return nil
}

func (t *Tree) deleteKeyFromNode(off OFFTYPE, key uint64) error {
	if off == InvalidOffset {
		return nil
	}
	var (
		node      *Node
		nextNode  *Node
		prevNode  *Node
		newRoot   *Node
		childNode *Node
		idx       int
		err       error
	)
	if node, err = t.newMappingNodeFromPool(off); err != nil {
		return err
	}
	idx = getIndex(node.Keys, key)
	removeKeyFromNode(node, idx)

	// update the last key of parent's if necessary
	if idx == len(node.Keys) {
		if err = t.mayUpdatedLastParentKey(node, idx-1); err != nil {
			return err
		}
	}

	// if statisfied len
	if len(node.Keys) >= order/2 {
		return t.flushNodesAndPutNodesPool(node)
	}

	if off == t.rootOff && len(node.Keys) == 1 {
		if newRoot, err = t.newMappingNodeFromPool(node.Children[0]); err != nil {
			return err
		}
		node.IsActive = false
		newRoot.Parent = InvalidOffset
		t.rootOff = newRoot.Self
		return t.flushNodesAndPutNodesPool(node, newRoot)
	}

	if node.Next != InvalidOffset {
		if nextNode, err = t.newMappingNodeFromPool(node.Next); err != nil {
			return err
		}
		// lease from next node
		if len(nextNode.Keys) > order/2 {
			key := nextNode.Keys[0]
			child := nextNode.Children[0]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(nextNode, 0)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(node, idx); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(node, nextNode, childNode)
		}
		// merge nextNode and curNode
		if node.Prev != InvalidOffset {
			if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
				return err
			}
			prevNode.Next = nextNode.Self
			nextNode.Prev = prevNode.Self
			if err = t.flushNodesAndPutNodesPool(prevNode); err != nil {
				return err
			}
		} else {
			nextNode.Prev = InvalidOffset
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
		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-1])
	}

	// come here because node.Next = INVALID_OFFSET
	if node.Prev != InvalidOffset {
		if prevNode, err = t.newMappingNodeFromPool(node.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevNode.Keys) > order/2 {
			key := prevNode.Keys[len(prevNode.Keys)-1]
			child := prevNode.Children[len(prevNode.Children)-1]

			// update child's parent
			if childNode, err = t.newMappingNodeFromPool(child); err != nil {
				return err
			}
			childNode.Parent = node.Self

			removeKeyFromNode(prevNode, len(prevNode.Keys)-1)
			if idx, err = insertKeyValIntoNode(node, key, child); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevNode, len(prevNode.Keys)-1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevNode, node, childNode)
		}
		// merge prevNode and curNode
		prevNode.Next = InvalidOffset
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

		return t.deleteKeyFromNode(node.Parent, node.Keys[len(node.Keys)-2])
	}
	return nil
}

func (t *Tree) deleteKeyFromLeaf(key uint64) error {
	var (
		leaf     *Node
		prevLeaf *Node
		nextLeaf *Node
		err      error
		idx      int
	)
	if leaf, err = t.newMappingNodeFromPool(InvalidOffset); err != nil {
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
		if err = t.mayUpdatedLastParentKey(leaf, idx-1); err != nil {
			return err
		}
	}

	// if satisfied len
	if len(leaf.Keys) >= order/2 {
		return t.flushNodesAndPutNodesPool(leaf)
	}

	if leaf.Next != InvalidOffset {
		if nextLeaf, err = t.newMappingNodeFromPool(leaf.Next); err != nil {
			return err
		}
		// lease from next leaf
		if len(nextLeaf.Keys) > order/2 {
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
		if leaf.Prev != InvalidOffset {
			if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
				return err
			}
			prevLeaf.Next = nextLeaf.Self
			nextLeaf.Prev = prevLeaf.Self
			if err = t.flushNodesAndPutNodesPool(prevLeaf); err != nil {
				return err
			}
		} else {
			nextLeaf.Prev = InvalidOffset
		}

		nextLeaf.Keys = append(leaf.Keys, nextLeaf.Keys...)
		nextLeaf.Records = append(leaf.Records, nextLeaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushNodesAndPutNodesPool(leaf, nextLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-1])
	}

	// come here because leaf.Next = INVALID_OFFSET
	if leaf.Prev != InvalidOffset {
		if prevLeaf, err = t.newMappingNodeFromPool(leaf.Prev); err != nil {
			return err
		}
		// lease from prev leaf
		if len(prevLeaf.Keys) > order/2 {
			key := prevLeaf.Keys[len(prevLeaf.Keys)-1]
			rec := prevLeaf.Records[len(prevLeaf.Records)-1]
			removeKeyFromLeaf(prevLeaf, len(prevLeaf.Keys)-1)
			if idx, err = insertKeyValIntoLeaf(leaf, key, rec); err != nil {
				return err
			}
			// update the last key of parent's if necessy
			if err = t.mayUpdatedLastParentKey(prevLeaf, len(prevLeaf.Keys)-1); err != nil {
				return err
			}
			return t.flushNodesAndPutNodesPool(prevLeaf, leaf)
		}
		// merge prevleaf and curleaf
		prevLeaf.Next = InvalidOffset
		prevLeaf.Keys = append(prevLeaf.Keys, leaf.Keys...)
		prevLeaf.Records = append(prevLeaf.Records, leaf.Records...)

		leaf.IsActive = false
		t.putFreeBlocks(leaf.Self)

		if err = t.flushNodesAndPutNodesPool(leaf, prevLeaf); err != nil {
			return err
		}

		return t.deleteKeyFromNode(leaf.Parent, leaf.Keys[len(leaf.Keys)-2])
	}

	return nil
}

func (t *Tree) ScanTreePrint() error {
	if t.rootOff == InvalidOffset {
		return fmt.Errorf("root = nil")
	}
	Q := make([]OFFTYPE, 0)
	Q = append(Q, t.rootOff)

	floor := 0
	var (
		curNode *Node
		err     error
	)
	for 0 != len(Q) {
		floor++

		l := len(Q)
		fmt.Printf("floor %3d:", floor)
		for i := 0; i < l; i++ {
			if curNode, err = t.newMappingNodeFromPool(Q[i]); err != nil {
				return err
			}
			defer t.putNodePool(curNode)

			// print keys
			if i == l-1 {
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
