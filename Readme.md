## bplustree
Implementing bplustree base on disk(kv store).The type of key is `uint64` and value is `string`.      
And all actions are `Concurrent security`(db will be locked when operators of `write`).  
  
Data will be rollback by `wal.log` when nodes be writed partially. It keep data Consistency.  

#### InitTree
```go 
if tree, err = NewTree("./data"); err != nil {
	panic(err)
}
defer tree.Close()
```
`NewTree` will create directory `./data` if not existed, or will read data by bplustree format.

#### Insert
```go 
if err = tree.Insert(20, "2001"); err != nil {
    panic(err)
}
```


#### Find
```go  
if val, err = tree.Find(20); err != nil {
    panic(err)
} 

```

#### Delete
```go  
if err = tree.Delete(20); err != nil {
	panic(err)
}
```

#### Update
```golang
if err = tree.Update(20, "2002"); err != nil {
    panic(err)
}
```

#### all error
```go  
var HasExistedKeyError = errors.New("hasExistedKey")
var NotFoundKey = errors.New("notFoundKey")
var InvalidDBFormat = errors.New("invalid db format")
var InnerWalError = errors.New("inner wal file error")
var InnerMetaError = errors.New("inner meta file error")
```


#### test
```go test -v``` 


## LICENSE
`MIT`