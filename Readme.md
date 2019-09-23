## bplustree
Implementing bplustree base on disk(kv store).The type of key is `uint64` and value is `string`.    
And all actions are not `Concurrent security`.

#### InitTree
```go 
if tree, err = NewTree("./data.db"); err != nil {
	panic(err)
}
defer tree.Close()
```
`NewTree` will create new file named `./data.db` if not existed, or will read data by bplustree format.

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
```


#### test
```go test -v``` 


## LICENSE
`MIT`