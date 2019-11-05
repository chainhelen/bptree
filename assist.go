package bptree

import "sort"

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