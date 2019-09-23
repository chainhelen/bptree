package bptree

import (
	"fmt"
	"os"
	"testing"
)

func TestBptree(t *testing.T) {
	var (
		tree *Tree
		err error
	)
	if tree, err = NewTree("./data.db"); err != nil {
		t.Fatal(err)
	}

	// insert
	for i := 0;i < 20;i++ {
		val := fmt.Sprintf("%d", i)
		if err = tree.Insert(uint64(i), val); err != nil {
			t.Fatal(err)
		}
	}

	// insert same key repeatedly
	for i := 0;i < 20;i++ {
		val := fmt.Sprintf("%d", i)
		if err = tree.Insert(uint64(i), val); err != HasExistedKeyError {
			t.Fatal(err)
		}
	}

	// find key
	for i := 0;i < 20;i++ {
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
	if tree, err = NewTree("./data.db"); err != nil {
		t.Fatal(err)
	}
	defer os.Remove("./data.db")
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
 		t.Fatal(fmt.Errorf("Expect %s, but get %s", "19", val))
	}

	// second print
	tree.ScanTreePrint()
}
