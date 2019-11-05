package bptree

import "encoding/json"

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

func (n *Node) EncodeJson() ([]byte, error) {
	return json.Marshal(n)
}

func DecodeJson(data []byte) (*Node, error) {
	node := &Node{}
	return node, json.Unmarshal(data, node)
}