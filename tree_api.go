package bptree

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"sync"
	"syscall"
)

func NewTree(openpath string) (*Tree, error) {
	var (
		stat  syscall.Statfs_t
		fstat os.FileInfo
		err   error
	)

	// Ensure our path exists
	if err = os.MkdirAll(openpath, 0755); err != nil && !os.IsExist(err) {
		return nil, err
	}

	if _, err := ioutil.ReadDir(openpath); err != nil {
		return nil, err
	}

	t := &Tree{}

	t.rootOff = InvalidOffset
	t.nodePool = &sync.Pool{
		New: func() interface{} {
			return &Node{}
		},
	}
	t.freeBlocks = make([]OFFTYPE, 0, MaxFreeBlocks)

	dbAbsFilename := path.Join(openpath, DBNAME)
	metaAbsFilename := path.Join(openpath, META)
	walAbsFilename := path.Join(openpath, WAL)
	// close all file if any err occur
	defer func() {
		if err != nil {
			t.Close()
		}
	}()
	if t.dbFile, err = os.OpenFile(dbAbsFilename, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}
	if t.metaFile, err = os.OpenFile(metaAbsFilename, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}
	if t.walFile, err = os.OpenFile(walAbsFilename, os.O_CREATE|os.O_RDWR, 0644); err != nil {
		return nil, err
	}

	if err = syscall.Statfs(metaAbsFilename, &stat); err != nil {
		return nil, err
	}
	t.blockSize = uint64(stat.Bsize)
	if t.blockSize == 0 {
		return nil, errors.New("blockSize should be zero")
	}
	if fstat, err = t.dbFile.Stat(); err != nil {
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

func (t *Tree) Close() error {
	if t.dbFile != nil {
		t.dbFile.Sync()
		t.dbFile.Close()
		t.dbFile = nil
	}
	if t.metaFile != nil {
		t.metaFile.Sync()
		t.metaFile.Close()
		t.metaFile = nil
	}
	if t.walFile != nil {
		t.walFile.Sync()
		t.walFile.Close()
		t.walFile = nil
	}
	return nil
}

func (t *Tree) Insert(key uint64, val string) error {
	var (
		err  error
		node *Node
	)

	t.Lock()
	defer t.Unlock()

	// wal
	if err = WriteWalTxStart(t.walFile, t.txid+1, INSERT_OP); err != nil {
		fmt.Fprintf(os.Stdout, "[failed]walfile:%s,rootOff:%d,txid:%d,err:%s", t.walFile.Name(), t.rootOff, t.txid+1, err)
		return InnerWalError
	}

	// rollback if any err
	defer func() {
		if err != nil {
			rollback(t.txid, t)
		}
	}()

	// ensure wal and meta Persist
	defer func() {
		if err == nil {
			if err = WriteWalTxEnd(t.walFile, t.txid+1, INSERT_OP); err != nil {
				return
			}
			// 这个地方可能需要double write
			metaStr := fmt.Sprintf("rootOff:%020d,txid:%020d", t.rootOff, t.txid+1)
			if _, err = t.metaFile.WriteAt([]byte(metaStr), 0); err != nil {
				fmt.Fprintf(os.Stdout, "[failed]metafile:%s,rootOff:%d,txid:%d,err:%s", t.metaFile.Name(), t.rootOff, t.txid+1, err)
				err = InnerMetaError
				return
			}
			t.txid++
		} else {
			if _, err1 := t.metaFile.Seek(0, io.SeekStart); err1 != nil {
				panic(err1)
			}
			if _, err1 := fmt.Fscanf(t.metaFile, "rootOff:%020d,txid:%020d", &t.rootOff, &t.txid); err1 != nil {
				panic(err1)
			}
		}
	}()

	if t.rootOff == InvalidOffset {
		if node, err = t.newNodeFromDisk(); err != nil {
			return err
		}
		t.rootOff = node.Self
		node.IsActive = true
		node.Keys = append(node.Keys, key)
		node.Records = append(node.Records, val)
		node.IsLeaf = true
		return t.flushNodesAndPutNodesPool(node)
	}

	err = t.insertIntoLeaf(key, val)
	return err
}

func (t *Tree) Delete(key uint64) error {
	var (
		err error
	)

	t.Lock()
	defer t.Unlock()

	// wal
	if err = WriteWalTxStart(t.walFile, t.txid+1, DELETE_OP); err != nil {
		fmt.Fprintf(os.Stdout, "[failed]walfile:%s,rootOff:%d,txid:%d,err:%s", t.walFile.Name(), t.rootOff, t.txid+1, err)
		return InnerWalError
	}

	// rollback if any err
	defer func() {
		if err != nil {
			rollback(t.txid, t)
		}
	}()

	// ensure wal and meta Persist
	defer func() {
		if err == nil {
			if err = WriteWalTxEnd(t.walFile, t.txid+1, DELETE_OP); err != nil {
				return
			}
			// 这个地方可能需要double write
			metaStr := fmt.Sprintf("rootOff:%020d,txid:%020d", t.rootOff, t.txid+1)
			if _, err = t.metaFile.WriteAt([]byte(metaStr), 0); err != nil {
				fmt.Fprintf(os.Stdout, "[failed]metafile:%s,rootOff:%d,txid:%d,err:%s", t.metaFile.Name(), t.rootOff, t.txid+1, err)
				err = InnerMetaError
				return
			}
			t.txid++
		} else {
			if _, err1 := t.metaFile.Seek(0, io.SeekStart); err1 != nil {
				panic(err1)
			}
			if _, err1 := fmt.Fscanf(t.metaFile, "rootOff:%020d,txid:%020d", &t.rootOff, &t.txid); err1 != nil {
				panic(err1)
			}
		}
	}()

	if t.rootOff == InvalidOffset {
		return fmt.Errorf("not found key:%d", key)
	}
	err = t.deleteKeyFromLeaf(key)
	return err
}

func (t *Tree) Find(key uint64) (string, error) {
	var (
		node *Node
		err  error
	)

	t.RLock()
	defer t.RUnlock()

	if t.rootOff == InvalidOffset {
		return "", nil
	}

	if node, err = t.newMappingNodeFromPool(InvalidOffset); err != nil {
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

func (t *Tree) Update(key uint64, val string) error {
	var (
		node *Node
		err  error
	)
	t.Lock()
	defer t.Unlock()

	// wal
	if err = WriteWalTxStart(t.walFile, t.txid+1, UPDATE_OP); err != nil {
		return err
	}

	// rollback if any err
	defer func() {
		if err != nil {
			rollback(t.txid, t)
		}
	}()

	// ensure wal and meta Persist
	defer func() {
		if err == nil {
			if err = WriteWalTxEnd(t.walFile, t.txid+1, UPDATE_OP); err != nil {
				return
			}
			// 这个地方可能需要double write
			metaStr := fmt.Sprintf("rootOff:%020d,txid:%020d", t.rootOff, t.txid+1)
			if _, err = t.metaFile.WriteAt([]byte(metaStr), 0); err != nil {
				fmt.Fprintf(os.Stdout, "[failed]metafile:%s,rootOff:%d,txid:%d,err:%s", t.metaFile.Name(), t.rootOff, t.txid+1, err)
				err = InnerMetaError
				return
			}
			t.txid++
		} else {
			if _, err1 := t.metaFile.Seek(0, io.SeekStart); err1 != nil {
				panic(err1)
			}
			if _, err1 := fmt.Fscanf(t.metaFile, "rootOff:%020d,txid:%020d", &t.rootOff, &t.txid); err1 != nil {
				panic(err1)
			}
		}
	}()

	if t.rootOff == InvalidOffset {
		return NotFoundKey
	}

	if node, err = t.newMappingNodeFromPool(InvalidOffset); err != nil {
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
