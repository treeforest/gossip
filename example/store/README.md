# Store

这是一个分布式存储的示例，在一个节点中写入数据后将广播到网络中的其它节点

## Usage
在这个示例中，将启动6个节点，步骤如下：
* 启动节点1
```go
go run main.go -port 6010
```
* 启动节点2
```go
go run main.go -port 6020 -member localhost:6010
```
* 启动节点3
```go
go run main.go -port 6030 -member localhost:6010
```
* 启动节点4
```go
go run main.go -port 6040 -member localhost:6010
```
* 启动节点5
```go
go run main.go -port 6050 -member localhost:6010
```
* 启动节点6
```go
go run main.go -port 6060 -member localhost:6010
```

