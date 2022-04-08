# chat

基于gossip实现的群聊示例

## Usage
 * 启动节点1
```
go run main.go -port 8000
```
 * 启动节点2
```
go run main.go -port 8001 -member localhost:8000
```
 * 启动节点3
```
go run main.go -port 8002 -member localhost:8000,localhost:8001
```

* 效果
```
...
> hello world
> hello gossip
> 
```