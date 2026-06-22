# Burberry 框架文档

Burberry 是 [paradigm/artemis](https://github.com/paradigmxyz/artemis/) 的 alloy 移植版：一个事件驱动的链上机器人框架。本文档覆盖全部公开 API 与运行时行为，目标是不读源码就能上手。

> 本仓库已升级到 **alloy 2.x**。所有示例在 `examples/` 下，每个用例一个独立文件夹。

---

## 目录

1. [设计总览](#1-设计总览)
2. [快速开始：four.meme 新币监听](#2-快速开始fourmeme-新币监听)
3. [Engine：泛型与运行时行为](#3-engine泛型与运行时行为)
4. [Collector：事件来源](#4-collector事件来源)
5. [Strategy：业务逻辑](#5-strategy业务逻辑)
6. [Executor：动作执行](#6-executor动作执行)
7. [类型适配：Map 适配器与宏](#7-类型适配map-适配器与宏)
8. [ActionSubmitter 一览](#8-actionsubmitter-一览)
9. [Feature flags](#9-feature-flags)
10. [常见陷阱 FAQ](#10-常见陷阱-faq)

---

## 1. 设计总览

整个框架只有一句话：**Collector 产生事件，Strategy 消费事件并产生动作，Executor 执行动作**。

```
┌───────────┐   事件 E    ┌──────────┐   动作 A    ┌──────────┐
│ Collector │ ──────────> │ Strategy │ ──────────> │ Executor │
│  (耳朵)    │  broadcast  │  (大脑)   │  broadcast  │  (手)     │
└───────────┘   channel   └──────────┘   channel   └──────────┘
```

三个角色通过两条 tokio broadcast channel 连接，互相完全不知道对方存在，只通过两个类型见面：

- `E`（Event）：事件管道里跑的类型，如 `Header`、`Log`、`Transaction`，或自定义枚举。
- `A`（Action）：动作管道里跑的类型，如 `TransactionRequest`、TG `Message`，或自定义枚举。

`Engine<E, A>` 上的两个泛型就是它们。**设计一个机器人，先想清楚 E 和 A 是什么，剩下都是填空。**

三个核心 trait（定义在 `src/types.rs`）：

```rust
#[async_trait]
pub trait Collector<E>: Send + Sync {
    fn name(&self) -> &str { "Unnamed" }
    /// 返回一个不断吐出事件的异步流
    async fn get_event_stream(&self) -> Result<CollectorStream<'_, E>>;
}

#[async_trait]
pub trait Strategy<E, A>: Send + Sync {
    fn name(&self) -> &str { "Unnamed" }
    /// 引擎启动时调用一次（在事件开始流动之前），用于初始化状态
    async fn sync_state(&mut self, submitter: Arc<dyn ActionSubmitter<A>>) -> Result<()> { Ok(()) }
    /// 每个事件调用一次
    async fn process_event(&mut self, event: E, submitter: Arc<dyn ActionSubmitter<A>>);
}

#[async_trait]
pub trait Executor<A>: Send + Sync {
    fn name(&self) -> &str { "Unnamed" }
    async fn execute(&self, action: A) -> Result<()>;
}
```

类型约束（为什么到处都是泛型 bound）：

- `E: Send + Sync + Clone + 'static` —— broadcast 是广播语义，每个订阅者拿到一份克隆，所以必须 `Clone`。
- `A: Send + Sync + Clone + Debug + 'static` —— 同上，外加 `Debug` 用于报错日志。

事件大、策略多时 `Clone` 有开销，可以把 `E` 定义成 `Arc<T>` 或字段里套 `Arc` 来规避。

---

## 2. 快速开始：four.meme 新币监听

完整代码见 `examples/four_meme_monitor/main.rs`：监听 BSC 上 four.meme 的 `TokenCreate` 事件并推送 Telegram。骨架如下：

```rust
// 1. 定事件/动作类型：E = Log，A = Message（TG 消息）
let mut engine: Engine<Log, Message> = Engine::new();

// 2. Collector：在节点端按合约地址 + topic0 过滤，只订阅目标事件
let filter = Filter::new()
    .address(TOKEN_MANAGER2)
    .event_signature(TokenCreate::SIGNATURE_HASH);
engine.add_collector(Box::new(LogCollector::new(provider, filter)));

// 3. Strategy：解码日志 -> 组装通知 -> submit
engine.add_strategy(Box::new(TokenCreateNotifier { bot_token, chat_id }));

// 4. Executor：调 Telegram Bot API
engine.add_executor(Box::new(TelegramMessageDispatcher::default()));

// 5. 跑起来（阻塞直到所有任务退出）
engine.run_and_join().await.unwrap()
```

Strategy 的核心实现：

```rust
#[async_trait::async_trait]
impl Strategy<Log, Message> for TokenCreateNotifier {
    async fn process_event(&mut self, log: Log, submitter: Arc<dyn ActionSubmitter<Message>>) {
        let event = TokenCreate::decode_log(&log.inner).unwrap();  // sol! 宏生成的解码器

        let message = MessageBuilder::new()
            .bot_token(&self.bot_token)
            .chat_id(&self.chat_id)
            .text(format!("{} ({})\ntoken: `{:#x}`", escape(&event.name), escape(&event.symbol), event.token))
            .build();

        submitter.submit(message);   // 非阻塞，扔进动作管道即返回
    }
}
```

运行：

```bash
cp .env.example .env   # 填 BSC_WSS / TG_BOT_TOKEN / TG_CHAT_ID
cargo run --example four_meme_monitor
```

其余示例：

| 示例 | 演示内容 |
|---|---|
| `examples/full_block` | 单 collector 单 executor 的最小引擎链路 |
| `examples/subscribe_mempool` | 不用引擎、直接消费 collector 的事件流 |
| `examples/map` | 多 collector / 多 executor 的枚举多路复用（见第 7 节） |
| `examples/four_meme_monitor` | 合约事件监听 + TG 通知的完整实战 |

---

## 3. Engine：泛型与运行时行为

### 构建 API

```rust
let mut engine: Engine<E, A> = Engine::new();   // 或 Engine::default()

engine.add_collector(Box<dyn Collector<E>>);
engine.add_strategy(Box<dyn Strategy<E, A>>);
engine.add_executor(Box<dyn Executor<A>>);

// 可选：调 channel 容量（默认各 512）
let engine = Engine::new()
    .with_event_channel_capacity(1024)
    .with_action_channel_capacity(1024);
```

三类组件都可以加任意多个。

### 启动流程（`run` / `run_and_join`）

```rust
// 返回 JoinSet<()>，自己管理任务生命周期
let join_set = engine.run().await?;

// 或者直接阻塞等待所有任务结束（常用）
engine.run_and_join().await?;
```

`run()` 内部按固定顺序执行：

1. 校验三类组件都非空，任一为空返回错误。
2. **先 spawn 所有 Executor**，各自订阅动作 channel。
3. **逐个调用每个 Strategy 的 `sync_state()`**（串行、在 spawn 之前）。任何一个失败，`run()` 直接报错退出——所以初始化失败会阻止引擎启动，而不是带病运行。然后 spawn 该 Strategy。
4. **最后 spawn 所有 Collector**。

这个顺序保证：collector 开始产生事件时，所有 strategy 已经订阅完毕，**不会丢启动瞬间的事件**。

### 广播语义（重要）

两条 channel 都是 `tokio::sync::broadcast`：

- **每个 Strategy 收到所有事件的克隆**；**每个 Executor 收到所有动作的克隆**。
- 不是任务队列（不是"谁抢到谁处理"），是广播。如果挂了两个相同的 `TransactionSender`，同一笔交易会被发两次。
- 需要"只有某类 executor 处理某类动作"时，用枚举 + `map_executor!`（第 7 节）。

### 背压与丢消息

broadcast channel 容量满时，**最旧的消息被丢弃**，消费者下次 `recv` 得到 `Lagged(n)`，引擎记一条 warn 日志后继续：

```
event channel lagged by 37
```

看到这条日志说明策略处理速度跟不上事件产生速度，已经丢事件了。对套利来说这是关键信号：要么优化策略耗时，要么加大容量（治标），要么把重活挪出 `process_event`。

### 错误与退出行为

| 情况 | 行为 |
|---|---|
| Executor `execute()` 返回 Err | 记 error 日志，继续处理下一个动作，**不会挂掉** |
| Strategy `sync_state()` 返回 Err | 引擎启动失败，`run()` 返回 Err |
| Collector 事件流结束（如 ws 断开） | 该 collector 任务退出并记 error 日志，**其他任务继续跑**，引擎不会自动重连 |
| channel 关闭 | 对应任务退出 |

注意：**collector 断流不会让 `run_and_join` 返回**（其他任务还在）。生产环境要自己监控"事件停止流动"的情况，或在外层做进程级重启。

---

## 4. Collector：事件来源

`CollectorStream<'a, E>` 就是 `Pin<Box<dyn Stream<Item = E> + Send + 'a>>`——一个装箱的异步流。

### 内置 Collector 总览

| Collector | 事件类型 `E` | 节点要求 | 说明 |
|---|---|---|---|
| `BlockCollector` | `Header` | ws/ipc | 订阅新块头 |
| `FullBlockCollector` | `Block` | ws/ipc | 订阅块头后再拉完整区块（含交易） |
| `PollFullBlockCollector` | `Block` | **http 可用** | 轮询 latest 完整区块，按块号去重 |
| `LogCollector` | `Log` | ws/ipc | 按 `Filter` 订阅日志（节点端过滤） |
| `LogsInBlockCollector` | `(Header, Vec<Log>)` | ws/ipc | 每出一个块，拉该块内匹配 filter 的全部日志 |
| `MempoolCollector` | `Transaction` | ws/ipc | 订阅 pending 交易哈希并并发拉全量交易体 |
| `IntervalCollector` | `Instant` | 无 | 定时器，每隔 interval 吐一个事件（驱动周期性任务） |

构造函数都很直白，`provider` 统一是 `Arc<dyn Provider>`：

```rust
BlockCollector::new(provider)
FullBlockCollector::new(provider)                          // 重试间隔默认 50ms
FullBlockCollector::new_with_config(provider, Duration::from_millis(100))
PollFullBlockCollector::new(provider, Duration::from_millis(500))
LogCollector::new(provider, filter)
LogsInBlockCollector::new(provider, filter)
MempoolCollector::new(provider)
IntervalCollector::new(Duration::from_secs(60))
```

### 各自的行为细节

- **`FullBlockCollector`**：收到块头后调 `get_block_by_number(n).full()`。节点可能还没索引好该块（返回 None），此时按 `retry_interval` 无限重试直到拿到；RPC 报错则跳过该块。
- **`PollFullBlockCollector`**：内部用 `AtomicU64::fetch_max` 去重，只对块号**严格递增**的块产生事件。轮询间隔内出了多个块时，中间的块会被跳过（只拿到 latest）。
- **`LogsInBlockCollector`**：以块哈希为锚点调 `get_logs`（不会拿错重组分叉上的日志）；`get_logs` 失败则跳过该块。
- **`MempoolCollector`**：节点只推哈希，框架内部用 `TransactionStream` 以最多 **256 并发**回查 `eth_getTransactionByHash` 补全交易体；查不到的（已上链/被丢弃）静默跳过。

### 订阅类 collector 必须用 ws/ipc

所有 `subscribe_*` 系列在 http provider 上会在启动时直接报错。http 节点只能用 `PollFullBlockCollector` / `IntervalCollector`。

```rust
let provider = ProviderBuilder::new()
    .connect_ws(WsConnect::new("wss://..."))
    .await?;
let provider: Arc<dyn Provider> = Arc::new(provider);
```

### 自定义 Collector

实现一个方法即可，配合 `async_stream::stream!` 很顺手：

```rust
struct MyCollector;

#[async_trait]
impl Collector<MyEvent> for MyCollector {
    fn name(&self) -> &str { "MyCollector" }

    async fn get_event_stream(&self) -> eyre::Result<CollectorStream<'_, MyEvent>> {
        let stream = async_stream::stream! {
            loop {
                // 任何事件源：ws 订阅、轮询 API、读队列……
                yield MyEvent { /* ... */ };
            }
        };
        Ok(Box::pin(stream))
    }
}
```

---

## 5. Strategy：业务逻辑

机器人 90% 的代码都在这里。两个要点：

### 有状态：`&mut self`

`process_event(&mut self, ...)` 拿的是可变引用，Strategy 天然是个状态机。套利场景下这里存池子储备、nonce 管理、在途交易等。每个 Strategy 在独立任务里串行处理自己收到的事件，**自身不需要加锁**。

### `sync_state`：启动时初始化

在事件流动之前被调用一次，典型用法是拉初始状态：

```rust
async fn sync_state(&mut self, _submitter: Arc<dyn ActionSubmitter<A>>) -> eyre::Result<()> {
    self.reserves = fetch_all_pool_reserves(&self.provider).await?;
    Ok(())
}
```

返回 Err 会让整个引擎拒绝启动（fail-fast）。

### 提交动作

`submitter.submit(action)` 是**同步非阻塞**的（往 channel 里 send），决策和执行解耦：发交易慢不会卡住下一个事件的处理。一次 `process_event` 里可以 submit 零个、一个或多个动作。

---

## 6. Executor：动作执行

### 内置 Executor 总览

| Executor | 动作类型 `A` | feature | 说明 |
|---|---|---|---|
| `Dummy` | 任意 | - | 丢弃动作，占位/测试用 |
| `RawTransactionSender` | `Bytes` | ethereum | 广播已签名的裸交易 |
| `TransactionSender` | `TransactionRequest` | ethereum | 补 nonce → 签名 → 广播 |
| `TelegramMessageDispatcher` | `Message` | telegram | 调 TG Bot API 发消息 |

### `RawTransactionSender`

交易在策略里已经签好，executor 只管 `eth_sendRawTransaction`：

```rust
RawTransactionSender::new(provider)
RawTransactionSender::new_http("https://...")        // 用独立 http 端点广播
// 一批预置的私有/加速端点：
RawTransactionSender::new_with_flashbots()           // rpc.flashbots.net/fast
RawTransactionSender::new_with_bsc_bloxroute()       // bsc.rpc.blxrbdn.com
RawTransactionSender::new_with_48club()              // rpc-bsc.48.club
RawTransactionSender::new_with_polygon_bloxroute()
RawTransactionSender::new_with_arbitrum_sequencer()
```

### `TransactionSender`

管理一组签名私钥，按 `from` 字段路由：

```rust
TransactionSender::new(provider, vec![signer1, signer2])
// 读链用 provider、广播走独立端点：
TransactionSender::new_http_dedicated(provider, "https://rpc.flashbots.net/fast", signers)
TransactionSender::new_with_flashbots(provider, signers)   // 以及同上一批预置端点
```

执行流程与**约定**（不满足时记 error 日志并跳过该动作，不会崩）：

1. `action.from` 必须已设置，且对应的 signer 已注册——用它选私钥。
2. `nonce` 未设置时自动补：`get_transaction_count(from)`（**latest**，非 pending，见 FAQ）。
3. **`gas_limit` / gas price（或 EIP-1559 两个 fee）/ `chain_id` 必须由策略填好**，缺了 `build()` 会失败。框架不做 gas 估算——对低延迟场景这是刻意的。
4. 签名后通过专用广播端点（若配置）或主 provider 发送。

### `TelegramMessageDispatcher`

```rust
TelegramMessageDispatcher::default()   // 或 ::new_without_error_report()
// 发送失败时把失败详情推到另一个"报错频道"：
TelegramMessageDispatcher::new(Some(bot_token), Some(chat_id), Some(thread_id))
```

消息用 `MessageBuilder` 构造：

```rust
let msg = MessageBuilder::new()
    .bot_token(token).chat_id(chat_id)
    .text(text)
    .thread_id(tid)                  // 可选：话题群的 topic
    .disable_link_preview(true)      // 可选
    .disable_notification(true)      // 可选：静默推送
    .parse_mode("HTML")              // 可选：默认 MarkdownV2
    .build();
```

默认 `parse_mode` 是 **MarkdownV2**，动态内容（代币名、用户输入等）必须用 `executor::telegram_message::escape()` 转义，否则带特殊字符的文本会 400（见 FAQ）。

### 自定义 Executor

```rust
struct Beeper;

#[async_trait]
impl Executor<MyAction> for Beeper {
    fn name(&self) -> &str { "Beeper" }
    async fn execute(&self, action: MyAction) -> eyre::Result<()> {
        // 返回 Err 只会被记日志，不会让引擎挂掉
        Ok(())
    }
}
```

---

## 7. 类型适配：Map 适配器与宏

引擎只有一对 `(E, A)`，多种事件/动作并存时用枚举收拢，再用适配器把"具体类型的组件"包装成"枚举类型的组件"。这是框架最容易被泛型劝退、但其实最简单的部分。

### 模式：枚举 + map 宏

```rust
#[derive(Debug, Clone)]
enum Event {
    Block(Header),
    Tx(Transaction),
}

#[derive(Debug, Clone)]
enum Action {
    SendTx(Bytes),
    Notify(Message),
}

let mut engine: Engine<Event, Action> = Engine::new();

// Collector<Header> -> Collector<Event>：输出套上枚举变体
engine.add_collector(map_collector!(BlockCollector::new(p.clone()), Event::Block));
engine.add_collector(map_collector!(MempoolCollector::new(p.clone()), Event::Tx));

// Executor<Bytes> -> Executor<Action>：只认自己的变体，其他变体直接忽略
engine.add_executor(map_executor!(RawTransactionSender::new(p.clone()), Action::SendTx));
engine.add_executor(map_executor!(TelegramMessageDispatcher::default(), Action::Notify));
```

Strategy 里 match 事件、用 `submit_action!` 提交：

```rust
async fn process_event(&mut self, event: Event, submitter: Arc<dyn ActionSubmitter<Action>>) {
    match event {
        Event::Block(header) => { /* ... */ }
        Event::Tx(tx) => {
            submit_action!(submitter, Action::SendTx, raw_bytes);
            // 等价于 submitter.submit(Action::SendTx(raw_bytes));
        }
    }
}
```

### 宏与底层适配器对照

| 宏 | 展开为 | 方向 |
|---|---|---|
| `map_collector!(c, Event::X)` | `Box::new(CollectorMap::new(Box::new(c), Event::X))` | `Collector<T>` → `Collector<Event>`（包装） |
| `map_boxed_collector!(boxed, Event::X)` | 同上，但 `c` 已经是 `Box<dyn Collector<T>>` | 同上 |
| `map_executor!(e, Action::X)` | `Box::new(ExecutorMap::new(Box::new(e), 匹配则Some))` | `Executor<T>` → `Executor<Action>`（拆包） |
| `map_boxed_executor!(boxed, Action::X)` | 同上 | 同上 |
| `submit_action!(s, Action::X, v)` | `s.submit(Action::X(v))` | 语法糖 |

底层适配器也可以直接用闭包构造，做比"套枚举"更复杂的转换/过滤：

- `CollectorMap::new(c, f)` —— `f: Fn(E1) -> E2`，逐个转换事件。
- `CollectorFilterMap::new(c, f)` —— `f: Fn(E1) -> Option<E2>`，返回 None 的事件被丢弃（转换 + 预过滤一步到位）。
- `ExecutorMap::new(e, f)` —— `f: Fn(A1) -> Option<A2>`，None 表示该动作不归这个 executor 管。
- `ActionSubmitterMap::new(s, f)` —— 同理作用于 submitter，常用于给子模块一个"窄类型"的提交口。

---

## 8. ActionSubmitter 一览

`ActionSubmitter<A>` 是策略提交动作的出口抽象（`fn submit(&self, action: A)`，同步、非阻塞）。引擎注入的实现是 `ActionChannelSubmitter`（往动作 channel send）。其他实现主要用于测试和脱离引擎的场景：

| 实现 | 行为 |
|---|---|
| `ActionChannelSubmitter` | 引擎内部使用，写 broadcast channel |
| `ActionPrinter` | 只把动作打到日志（单测策略时注入它即可，无需起引擎） |
| `ActionSubmitterMap` | 类型适配（见第 7 节） |
| `TelegramSubmitter` | 同步直发 TG 消息；`new_with_redirect` 可强制改写收件人。**注意：内部起线程并 join，会阻塞调用线程**，只适合脚本/调试，不要放进异步热路径 |

测试策略的套路：

```rust
let submitter = Arc::new(ActionPrinter::<Action>::default());
strategy.process_event(fake_event, submitter).await;   // 不需要引擎和节点
```

---

## 9. Feature flags

```toml
[features]
default = ["ethereum", "telegram"]
ethereum = ["dep:alloy", "dep:thiserror"]   # 全部链上 collector + 交易 executor
telegram = ["dep:reqwest", "dep:serde_json"] # TG executor/submitter
```

引擎核心（Engine / 三个 trait / 适配器 / `IntervalCollector` / `Dummy`）不在任何 feature 后面，永远可用。只做离线处理可以 `default-features = false` 把 alloy 都裁掉。

引入方式（本 fork）：

```toml
[dependencies]
burberry = { git = "https://github.com/naixiWeb3/burberry" }
```

---

## 10. 常见陷阱 FAQ

**Q: 启动就报错 `Transport(...)` / subscribe 失败？**
订阅类 collector 需要 ws/ipc 节点，http 不支持 `eth_subscribe`。要么换 `wss://` 端点，要么改用 `PollFullBlockCollector`。

**Q: 日志里出现 `event channel lagged by N`？**
策略消费速度跟不上，broadcast 丢掉了 N 条最旧的事件。优化 `process_event` 耗时（把网络 IO 挪到 executor 或后台任务）、或调大 `with_event_channel_capacity`。

**Q: 同一笔交易被发了两次？**
你挂了两个能处理同一动作类型的 executor。broadcast 是广播：每个 executor 收到所有动作。用枚举 + `map_executor!` 把动作路由到唯一的 executor。

**Q: `TransactionSender` 连发多笔交易 nonce 冲突？**
它对每笔缺 nonce 的交易查 `get_transaction_count`（**latest**）：上一笔还没上链时，下一笔会拿到相同 nonce。高频场景在策略里自己维护 nonce 计数器，显式 `set_nonce` 后再提交。

**Q: 交易构建失败 `failed to build tx`？**
`TransactionSender` 只补 nonce。`from`、`gas_limit`、gas price（或 1559 的 `max_fee_per_gas` + `max_priority_fee_per_gas`）、`chain_id` 都要策略自己填。

**Q: TG 消息发送 400？**
默认 parse_mode 是 MarkdownV2，`. - ( ) !` 等都是保留字符。所有动态文本过一遍 `escape()`，或者 `parse_mode("HTML")` 自己转义 `< > &`。

**Q: ws 断了引擎还"活着"但没有事件？**
collector 断流只会终止它自己的任务（error 日志 `event stream ended!`），引擎不自动重连。生产环境用进程管理器（systemd/pm2）配合"无事件超时告警"做重启。

**Q: 事件类型很大，担心 Clone 开销？**
`E` 直接定义成 `Arc<BigEvent>`，或枚举变体里装 `Arc<T>`。broadcast 克隆的就只是指针。
