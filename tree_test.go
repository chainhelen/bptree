package bptree

import (
	"errors"
	"fmt"
	"os"
	"testing"
)

func TestBptree(t *testing.T) {
	var (
		tree *Tree
		err  error
	)
	if tree, err = NewTree("./tmp_data"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./tmp_data")
	defer tree.Close()

	// insert
	for i := 0; i < 20; i++ {
		val := fmt.Sprintf("%d", i)
		if err = tree.Insert(uint64(i), val); err != nil {
			t.Fatal(err)
		}
	}

	// insert same key repeatedly
	for i := 0; i < 20; i++ {
		val := fmt.Sprintf("%d", i)
		if err = tree.Insert(uint64(i), val); err != HasExistedKeyError {
			t.Fatal(err)
		}
	}

	// find key
	for i := 0; i < 20; i++ {
		oval := fmt.Sprintf("%d", i)
		if val, err := tree.Find(uint64(i)); err != nil {
			t.Fatal(err)
		} else {
			if oval != val {
				t.Fatal(fmt.Sprintf("not equal key:%d oval:%s, found val:%s", i, oval, val))
			}
		}
	}

	// first print
	tree.ScanTreePrint()

	// delete two keys
	if err := tree.Delete(0); err != nil {
		t.Fatal(err)
	}
	if err := tree.Delete(2); err != nil {
		t.Fatal(err)
	}

	if _, err := tree.Find(2); err != NotFoundKey {
		t.Fatal(err)
	}

	// close tree
	tree.Close()

	//repoen tree
	if tree, err = NewTree("./tmp_data"); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("./tmp_data")
	defer tree.Close()

	// find
	if _, err := tree.Find(2); err != NotFoundKey {
		t.Fatal(err)
	}

	// update {key: 18, val : "19"}
	if err := tree.Update(18, "19"); err != nil {
		t.Fatal(err)
	}

	// find {key: 18, val : "19"}
	if val, err := tree.Find(18); err != nil {
		t.Fatal(err)
	} else if "19" != val {
		t.Fatal(fmt.Errorf("expect %s, but get %s", "19", val))
	}

	// second print
	tree.ScanTreePrint()
}

func TestBptreeTranscation(t *testing.T) {
	var (
		tree *Tree
		err  error
	)
	if tree, err = NewTree("./tmp2_data"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./tmp2_data")
	defer tree.Close()

	// 模拟刷node入磁盘出现错误
	opCount := 0
	userError := errors.New("user_error")
	tree.flushNodeHookHandle = func(*Node) error {
		opCount++
		if opCount%20 == 0 {
			return userError
		}
		return nil
	}

	failedKeyArr := make([]uint64, 0)
	sucessKeyArr := make([]uint64, 0)
	// insert
	for i := 0; i < 30; i++ {
		val := fmt.Sprintf("%d", i)
		if err = tree.Insert(uint64(i), val); err != nil {
			if err == userError {
				failedKeyArr = append(failedKeyArr, uint64(i))
			} else {
				t.Fatal(err)
			}
		} else {
			sucessKeyArr = append(sucessKeyArr, uint64(i))
		}
	}

	// expect failed key
	fmt.Printf("expect failed keys %+v\n", failedKeyArr)
	// first print tree
	tree.ScanTreePrint()

	tree.Close()
	if tree, err = NewTree("./tmp2_data"); err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll("./tmp2_data")
	defer tree.Close()

	for _, key := range failedKeyArr {
		if _, err := tree.Find(key); err != NotFoundKey {
			panic(fmt.Errorf("expect err :%s, but get %s", NotFoundKey, err))
		}
	}

	for _, key := range sucessKeyArr {
		if _, err := tree.Find(key); err != nil {
			panic(fmt.Errorf("export nil, but get err %s", err))
		}
	}
}
