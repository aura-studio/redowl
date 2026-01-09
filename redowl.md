\# redowl

用 Redis 的 List / Hash / ZSet 模拟一套「接近 SQS 语义」的队列：支持 Ack、可见性超时、DLQ（死信队列）、DLQ redrive，以及可选的 PubSub 事件（用于触发/动态发现队列）。

本仓库 Go module：`github.com/aura-studio/redowl`（Go 1.21）。Redis 客户端使用 `github.com/redis/go-redis/v9`。

---

## 特性概览

- SQS-like：发送、接收、Ack（基于 ReceiptHandle）。
- 可见性超时（Visibility Timeout）：未 Ack 的消息到期可再次投递。
- DLQ：超过最大投递次数（ReceiveCount）自动进入死信队列。
- DLQ redrive：把 DLQ 消息批量移回 ready。
- 事件（PubSub，可选）：发送/接收/重投递/进入 DLQ/从 DLQ 接收/从 DLQ redrive/Ack。
- WorkerPool：用少量 worker 消费大量动态队列（通过订阅 `{prefix}:events` 发现队列）。
- 兼容 Redis 单机与 Cluster：Queue 构造使用 `redis.Cmdable`；WorkerPool 使用 `redis.UniversalClient`。

---

## 安装

```bash
go get github.com/aura-studio/redowl
```

---

## 核心概念

### Message

`Message` 是一次“投递”的载体：

- `ID`：消息 ID。
- `Body`：消息体（`[]byte`）。
- `Attributes`：属性（`map[string]string`）。
- `ReceiptHandle`：Ack 必需的句柄（一次投递对应一个 receipt）。
- `ReceiveCount`：累计投递次数（每次 receive 会自增）。
- `VisibleAt`：本次投递的可见性到期时间。
- `CreatedAt`：创建时间。

### Queue

`Queue` 提供：

- `Send(ctx, body, attrs)`
- `Receive(ctx)` / `ReceiveWithWait(ctx, wait)`
- `Ack(ctx, receiptHandle)`
- `ChangeVisibility(ctx, receiptHandle, d)`
- `RequeueExpiredOnce(ctx, batch)`
- `ReceiveDLQ(ctx)` / `ReceiveDLQWithWait(ctx, wait)`
- `RedriveDLQ(ctx, n)`
- `Subscribe(ctx, handler)`（需要配置 triggers）
- `StartReaper()` / `StopReaper()`（后台回收器，需配置 `WithReaperInterval`）

---

## 快速开始：发送 / 接收 / Ack

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/aura-studio/redowl"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})

    q, err := redowl.New(
        rdb,
        "orders",
        redowl.WithVisibilityTimeout(30*time.Second),
        redowl.WithMaxReceiveCount(3),
    )
    if err != nil {
        panic(err)
    }

    _, _ = q.Send(ctx, []byte("hello"), map[string]string{"trace_id": "t-1"})

    msg, err := q.ReceiveWithWait(ctx, 5*time.Second)
    if err != nil {
        panic(err)
    }
    if msg == nil {
        return
    }

    fmt.Println("recv:", msg.ID, string(msg.Body), msg.Attributes["trace_id"], msg.ReceiveCount)
    _ = q.Ack(ctx, msg.ReceiptHandle)
}
```

---

## Queue Options

构造队列：

```go
q, err := redowl.New(client, "name",
    redowl.WithPrefix("myapp"),
    redowl.WithVisibilityTimeout(10*time.Second),
    redowl.WithMaxReceiveCount(5),
    redowl.WithReaperInterval(2*time.Second),
    redowl.WithTriggerClient(client),
)
```

### WithPrefix(prefix)

- Redis key 的前缀，默认 `redowl`。

### WithVisibilityTimeout(d)

- 可见性超时，默认 `30s`。
- 每次 `Receive/ReceiveWithWait` 会生成新的 `ReceiptHandle`，并写入 inflight ZSet 的到期时间。

### WithMaxReceiveCount(n)

- 最大投递次数；默认 `0` 表示关闭 DLQ 行为。
- 当 `ReceiveCount` 自增后满足 `rc > n`，该消息会被移动到 DLQ，并且本次 `Receive*` 返回 `(nil, nil)`（即不会把它作为正常消息交给业务）。

### WithReaperInterval(d)

- 开启后台回收器：定期调用 `RequeueExpiredOnce(ctx, 500)`，把过期的 inflight 重新放回 ready。
- `d <= 0` 时不启动后台回收器（仍可手动调用 `RequeueExpiredOnce`）。

### WithTriggerClient(c)

- 启用 PubSub 事件：`Send/Receive/RequeueExpiredOnce/DLQ/Redrive/Ack` 等会 publish 事件。
- 若你没有显式配置该 option，但传入的 `cmd` 本身是 `redis.UniversalClient`（例如 `*redis.Client` / `*redis.ClusterClient`），会自动复用它作为触发器客户端。

---

## 事件（PubSub Trigger）

事件结构：

```go
type Event struct {
    Type      redowl.EventType
    Queue     string
    MessageID string
    AtUnixMs  int64
    Extra     map[string]string
}
```

事件类型：

- `sent`
- `received`
- `requeued`
- `to_dlq`
- `dlq_received`
- `dlq_redriven`
- `acked`

发布通道：

- per-queue：`{prefix}:{queue}:events`
- namespace：`{prefix}:events`

说明：redowl 会把同一个事件同时 publish 到上述两个通道；这样消费端可以只订阅 namespace 通道就能“动态发现队列”。

订阅 per-queue 事件：

```go
unsub, err := q.Subscribe(ctx, func(e redowl.Event) {
    // e.Queue / e.Type / e.MessageID ...
})
if err != nil {
    // 可能是 redowl.ErrTriggersNotConfigured
    panic(err)
}
defer func() { _ = unsub() }()
```

---

## 可见性超时与回收

### 重要行为

- 如果消费者在处理过程中崩溃或忘记 Ack，消息不会丢失：它会在 `VisibilityTimeout` 到期后变为可再次投递。
- `ReceiveWithWait` 内部会做一次 best-effort 的回收：每次调用都会先尝试 `RequeueExpiredOnce(ctx, 100)`。

### 手动回收：RequeueExpiredOnce

```go
requeued, err := q.RequeueExpiredOnce(ctx, 100)
_ = requeued
_ = err
```

### 后台回收器：WithReaperInterval

```go
q, _ := redowl.New(client, "orders",
    redowl.WithReaperInterval(2*time.Second),
)
defer q.StopReaper() // 建议在退出时停止
```

---

## DLQ（死信队列）

启用：

```go
q, _ := redowl.New(client, "orders", redowl.WithMaxReceiveCount(3))
```

### ReceiveDLQ / ReceiveDLQWithWait

- DLQ 接收会把消息 ID 从 DLQ list 弹出（类似消费 ready）。
- DLQ 也会生成 `ReceiptHandle`，用于调用 `Ack` 删除消息。
- DLQ 接收不应用可见性超时；一旦从 DLQ list 取出，就不会自动回到 DLQ。

```go
dlqMsg, err := q.ReceiveDLQWithWait(ctx, 5*time.Second)
if err != nil {
    panic(err)
}
if dlqMsg != nil {
    // 记录/报警/人工处理...
    _ = q.Ack(ctx, dlqMsg.ReceiptHandle)
}
```

### RedriveDLQ

把 DLQ 中最多 n 条消息移回 ready：

```go
moved, err := q.RedriveDLQ(ctx, 100)
_ = moved
_ = err
```

实现备注：redrive 仅移动“消息 ID”，消息体仍在 msg hash 中；并会把 ReceiveCount 复位为 0，以便重新投递。

---

## WorkerPool：用少量 worker 处理大量队列

当队列数非常大且动态变化时，「每队列一个 goroutine」会浪费资源。WorkerPool 通过订阅 `{prefix}:events`，在收到 `EventSent` 时把队列加入调度，并用有限数量的 worker 串行处理单个队列、并发处理多个队列。

### 创建与启动

```go
pool := redowl.NewWorkerPool(
    client,          // redis.UniversalClient
    "myapp",         // prefix（同时也是订阅的 namespace 事件 channel）
    func(ctx context.Context, queueName string, msg *redowl.Message) error {
        // 业务处理...
        return nil // 返回 nil 将自动 Ack；返回 error 不 Ack（等待可见性超时后重投）
    },
    redowl.WithMaxWorkers(10),
    redowl.WithMinWorkers(0),
    redowl.WithQueueIdleTimeout(5*time.Minute),
    redowl.WithWorkerIdleTimeout(30*time.Second),
    redowl.WithPollInterval(500*time.Millisecond),
)

if err := pool.Start(ctx); err != nil {
    panic(err)
}
defer pool.Stop()
```

### PoolOption 说明

- `WithMaxWorkers(n)`：最大 worker 数，默认 10。
- `WithMinWorkers(n)`：最小常驻 worker 数，默认 0。
- `WithQueueIdleTimeout(d)`：队列“无消息”持续多久后，worker 停止处理该队列；并且该队列的统计元数据在后续清理周期中会被删除，默认 5 分钟。
- `WithWorkerIdleTimeout(d)`：worker 空闲多久后允许退出（但不会低于 MinWorkers），默认 30 秒。
- `WithPollInterval(d)`：单个队列上的 `ReceiveWithWait` 等待/轮询间隔，默认 500ms。

### 顺序与并发语义

- 同一队列：单个 worker 在 `processQueue` 中循环 receive + handler +（成功则 ack），因此队列内是串行处理。
- 不同队列：多个 worker 可并发处理不同队列。

### 统计

```go
stats := pool.Stats()
// map[queueName]processedMessageCount
```

---

## Redis Key 结构（实现细节）

默认 `prefix=redowl`、队列名为 `name`：

- Ready（List）：`{prefix}:{name}:ready`（RPUSH 入队，LPOP/BLPOP 出队）
- DLQ（List）：`{prefix}:{name}:dlq`
- Message（Hash）：`{prefix}:{name}:msg:{id}`
  - `body`：base64 编码
  - `attrs`：JSON 字符串（map）
  - `rc`：接收次数
  - `created_at_ms`：Unix 毫秒
- ReceiptMap（Hash）：`{prefix}:{name}:receipt`（receiptHandle -> messageID）
- Inflight（ZSet）：`{prefix}:{name}:inflight`（member=receiptHandle，score=visibleAtUnixMs）
- Events（PubSub）：`{prefix}:{name}:events` 与 `{prefix}:events`

提示：这些 key 命名来自当前实现（见 `queue.go`），若未来实现调整，文档可能需要同步更新。

---

## Redis Cluster / 集成测试

- Queue 侧：`redowl.New` 接收 `redis.Cmdable`，所以可直接传 `*redis.ClusterClient`。
- WorkerPool 侧：`redowl.NewWorkerPool` 需要 `redis.UniversalClient`，`*redis.ClusterClient` 满足。

本仓库的单测使用 `miniredis`（不模拟完整 cluster）；cluster 场景的用例放在测试里，通过环境变量开关运行：

- `REDIS_CLUSTER_ADDRS`：逗号分隔的 cluster 节点地址。

示例（PowerShell）：

```powershell
$env:REDIS_CLUSTER_ADDRS = "127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002"
go test ./... -run ClusterClient
```

示例（bash）：

```bash
REDIS_CLUSTER_ADDRS=127.0.0.1:7000,127.0.0.1:7001,127.0.0.1:7002 \
  go test ./... -run ClusterClient
```

---

## 常见坑与建议

- `Ack` 依赖 `ReceiptHandle`；重复 Ack 或过期 receipt 会返回 `redowl.ErrInvalidReceiptHandle`。
- `ReceiveWithWait` 的 wait >= 1s 时会用 Redis `BLPOP`（秒级精度）；小于 1 秒则用轮询 + sleep。
- 生产环境建议开启 `WithReaperInterval` 或由外部定时任务调用 `RequeueExpiredOnce`，以便更及时回收超时消息。

```go
// ❌ 不推荐：1000 个队列 = 1000 个 goroutine
for i := 0; i < 1000; i++ {
    q, _ := redowl.New(client, fmt.Sprintf("queue_%d", i))
    go func(q *Queue) {
        for {
            msg, _ := q.ReceiveWithWait(ctx, 5*time.Second)
            // 大部分时间在等待，浪费资源
        }
    }(q)
}
```

### 解决方案：Worker 池

```go
// ✅ 推荐：10 个 worker 处理 1000 个队列
pool := redowl.NewWorkerPool(
    client,
    "myapp",
    func(ctx context.Context, queueName string, msg *redowl.Message) error {
        fmt.Printf("[%s] %s\n", queueName, string(msg.Body))
        return nil // 返回 nil 自动 Ack
    },
	redowl.WithMaxWorkers(10),             // 最多 10 个 worker
	redowl.WithQueueIdleTimeout(5*time.Minute), // 5 分钟无消息自动清理队列
    redowl.WithPollInterval(500*time.Millisecond),
)

pool.Start(ctx)
defer pool.Stop()

// 动态创建 1000 个队列
for i := 0; i < 1000; i++ {
    q, _ := redowl.New(client, fmt.Sprintf("queue_%d", i),
        redowl.WithPrefix("myapp"))
    q.Send(ctx, []byte("message"), nil)
}

// Worker 池会自动：
// 1. 监听 "myapp:events" 发现新队列
// 2. 动态分配 worker 处理消息
// 3. 空闲队列自动回收，释放资源
```

### Worker 池特性

1. **动态队列发现**
   - 自动订阅 `{prefix}:events` 频道
   - 收到 `EventSent` 时自动处理该队列

2. **资源高效 + 动态释放**
   - 固定数量的 worker goroutine
   - **按需分配**：worker 只在处理消息时占用队列
   - **主动释放**：连续 3 次空闲后自动释放，可处理其他队列
   - 空闲队列元数据自动清理（可配置超时时间）

3. **负载均衡**
   - Worker 通过 channel 竞争获取队列
   - 有消息的队列优先处理
   - Worker 动态切换队列，适应滚动负载

4. **统计信息**
   ```go
   stats := pool.Stats()
   // map[string]int64{"queue_1": 100, "queue_2": 50, ...}
   ```

### 完整示例

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/aura-studio/boost/redowl"
    "github.com/redis/go-redis/v9"
)

func main() {
    client := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })
    defer client.Close()

    ctx := context.Background()

    // 创建 worker 池
    pool := redowl.NewWorkerPool(
        client,
        "game",
        func(ctx context.Context, queueName string, msg *redowl.Message) error {
            // 处理消息
            fmt.Printf("[%s] Player: %s\n", queueName, string(msg.Body))
            time.Sleep(10 * time.Millisecond) // 模拟处理
            return nil // 返回 nil 自动 Ack，返回 error 不 Ack
        },
		redowl.WithMaxWorkers(10),
		redowl.WithQueueIdleTimeout(5*time.Minute),
    )

    if err := pool.Start(ctx); err != nil {
        panic(err)
    }
    defer pool.Stop()

    // 模拟 100 个游戏房间，每个房间是一个队列
    for i := 0; i < 100; i++ {
        roomQueue, _ := redowl.New(client, fmt.Sprintf("room_%d", i),
            redowl.WithPrefix("game"),
            redowl.WithVisibilityTimeout(30*time.Second),
        )

        // 玩家加入房间
        roomQueue.Send(ctx, []byte("player_joined"), nil)
    }

    // 等待处理完成
    time.Sleep(5 * time.Second)

    // 查看统计
    stats := pool.Stats()
    fmt.Printf("Processed %d queues\n", len(stats))
}
```

### 性能对比

| 方案 | 队列数 | Goroutine 数 | 内存占用 | 适用场景 |
|------|--------|--------------|----------|----------|
| 每队列一个 Worker | 1000 | 1000+ | 高 | 队列数固定且少 |
| Worker 池 | 1000 | 10-50 | 低 | 队列数动态扩展 |

### 配置选项

```go
redowl.WithMaxWorkers(n int)               // Worker 最大数量（默认 10）
redowl.WithQueueIdleTimeout(d time.Duration) // 空闲队列清理时间（默认 5 分钟）
redowl.WithPollInterval(d time.Duration) // 轮询间隔（默认 500ms）
```

### 注意事项

1. **队列内顺序保证**：同一队列的消息按顺序处理（worker 处理完一条再取下一条）
2. **队列间并发**：不同队列的消息可以并发处理
3. **自动 Ack**：handler 返回 `nil` 时自动 Ack，返回 `error` 时不 Ack
4. **动态释放机制**：
   - Worker 级别：连续 3 次空闲后释放队列，可处理其他队列
   - 元数据级别：超过 `IdleTimeout` 的队列统计信息被清理

### 动态释放工作原理

```
时间线：
t0: Queue1 有消息 → Worker1 处理 Queue1
t1: Queue1 空闲 (1/3)
t2: Queue1 空闲 (2/3)
t3: Queue1 空闲 (3/3) → Worker1 释放，可处理其他队列
t4: Queue2 有消息 → Worker1 处理 Queue2
```

**优势**：
- 10 个 worker 可以处理 1000+ 个滚动队列
- Worker 不会被空闲队列占用
- 自动适应负载变化

### 何时使用 Worker 池

- ✅ 队列数量动态变化（如游戏房间、用户会话）
- ✅ 大部分队列消息量较少
- ✅ 需要节省资源（内存、goroutine）
- ❌ 队列数量固定且少（< 10 个）
- ❌ 每个队列都需要独立的状态管理
