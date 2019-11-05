package bptree

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
)

const (
	TXSTART = iota
	TXEND
	UNDONODE
	REDONODE
)

const (
	INSERT_OP = "insert"
	UPDATE_OP = "update"
	DELETE_OP = "delete"
)

func WriteWalTxStart(writer *os.File, txPoint uint64, op string) error {
	if _, err := fmt.Fprintf(writer, "txstart %d %s\n", txPoint, op); err != nil {
		return err
	}
	return writer.Sync()
}

func WriteWalTxEnd(writer *os.File, txPoint uint64, op string) error {
	if _, err := fmt.Fprintf(writer, "txend   %d %s\n", txPoint, op); err != nil {
		return err
	}
	return writer.Sync()
}

func WriteWalUndoNode(writer *os.File, node *Node) error {
	var (
		data []byte
		err  error
	)
	if data, err = node.EncodeJson(); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(writer, "undo    %s\n", data); err != nil {
		return err
	}
	return writer.Sync()
}

func WriteWalRedoNode(writer *os.File, node *Node) error {
	var (
		data []byte
		err  error
	)
	if data, err = node.EncodeJson(); err != nil {
		return err
	}
	if _, err = fmt.Fprintf(writer, "redo    %s\n", data); err != nil {
		return err
	}
	return writer.Sync()
}

func rollback(txPoint uint64, t *Tree) {
	var (
		data        []byte
		err         error
		rollBacking bool
		isWritOpWal bool
		tmpFile     *os.File
		pos         int64
	)
	fileName := t.walFile.Name()
	if tmpFile, err = os.Open(fileName); err != nil {
		panic(err)
	}
	reader := bufio.NewReader(tmpFile)
	rollBacking = false
	isWritOpWal = false
	stack := make([]*Node, 0)
	for {
		// 一行不能超过 65536
		if data, err = reader.ReadSlice('\n'); err != nil {
			if err == io.EOF {
				err = nil
				break
			}
			panic(fmt.Errorf("rollback txPoint:%d failed, err:%s", txPoint, err))
		}
		if len(data) <= int(8) {
			panic(fmt.Errorf("rollback txPoint:%d failed, len(data):%d <= int(8)", txPoint, len(data)))
		}
		if !rollBacking {
			pos += int64(len(data))
		}
		if bytes.Compare(data[:8], []byte("txstart ")) == 0 {
			if !rollBacking {
				continue
			} else {
				isWritOpWal = true
				continue
			}
		} else if bytes.Compare(data[:8], []byte("txend   ")) == 0 {
			if !rollBacking {
				curTxId := uint64(0)
				if _, err := fmt.Fscanf(bytes.NewBuffer(data), "txend   %d", &curTxId); err != nil {
					panic(fmt.Errorf("err %s, data %s", err, data))
				}
				if curTxId == txPoint {
					rollBacking = true
				}
			} else {
				isWritOpWal = false
				continue
			}
		} else if bytes.Compare(data[:8], []byte("undo    ")) == 0 {
			if !rollBacking {
				continue
			} else {
				if isWritOpWal {
					dataStr := ""
					if _, err := fmt.Fscanf(bytes.NewBuffer(data), "undo    %s", &dataStr); err != nil {
						panic(err)
					}
					var (
						node *Node
						err  error
					)
					if node, err = DecodeJson([]byte(dataStr)); err != nil {
						panic(err)
					}
					stack = append(stack, node)
				}
			}
		} else if bytes.Compare(data[:8], []byte("redo    ")) == 0 {
			continue
		} else {
			panic(fmt.Errorf("invalid wal format: data:%s", data))
		}
	}
	if !rollBacking {
		panic(fmt.Errorf("rollback txPoint:%d failed, wal not found", txPoint))
	}
	for 0 != len(stack) {
		node := stack[len(stack)-1]
		if node.Self != InvalidOffset {
			if err = t.flushNode(node); err != nil {
				panic(err)
			}
		}
		stack = stack[:len(stack)-1]
	}
	if _, err = t.walFile.Seek(pos, io.SeekStart); err != nil {
		panic(err)
	}
	if err = os.Truncate(fileName, pos); err != nil {
		panic(err)
	}
	fmt.Fprintf(os.Stdout, "[rollback]rootOff:%d,txid:%d\n", t.rootOff, t.txid)
}
