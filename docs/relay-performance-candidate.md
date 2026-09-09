# Relay 接收分发与流量控制

本说明描述 Go SDK 的 Relay 接收、发送屏障与关闭语义。协议仍为 `client-v1alpha5` 和现有 RelayEnvelope，未增加 wire 字段或要求核心变更。

## 接收与预算

- 共享 WebSocket reader 不等待业务 Receive、Relay RPC 或用户关闭回调。每 Relay 一个有界 FIFO 分发 worker；DATA 仍按可靠序号重排。
- 入站 ACK 推进反向发送窗口，允许越过正向慢 DATA。首次入队 CLOSE/ERROR 后关闭该快速路径，后续 ACK 留在 FIFO，不能越过终止帧。CLOSE/ERROR 仍等待此前 DATA 交付及接收 ACK 受理。
- `recvCh` 保持 64；发送窗口公开范围 1..256，默认 16，不限制只能使用 16。首次 DATA 与重传共享 `min(WindowSize, 16)` 个受理槽。
- 接收 DATA 的保守预算仍为 `cap(recvCh) + 2*WindowSize`，默认 96 帧，包含排队、活动、重排存储及尚未投递的 ready 批次。交接会重复计数，因此实际可容纳数量可能略低。队列额外保留 8 个控制槽，不允许无限排队。
- 本地重排窗口不是远端发送窗口，没有窗口协商。可靠有序 DATA 超出本地窗口时忽略、不 ACK，等待发送方重传；不得当作协议错误关闭。入队之前也执行窗口过滤，考虑已排队连续序号，避免分发 worker 阻塞时把较大远端窗口误判为 overflow。
- 尚在 inbox、重排或 ready 批次内的重复可靠序号不占新预算。历史重传只可重发已经完整交付的累计 ACK，不能使用提前推进的 `expectedSeq` 确认尚未交付的 ready 批次。
- 可靠有序 DATA 预算不足时丢弃该次未确认帧，保留连接和已存数据，由重传恢复。其他可靠性模式的 DATA 预算不足、控制队列耗尽仍显式 `receive_overflow`，只关闭该 Relay，不断开共享 WS。
- ACK 仍仅在连续批次全部进入 recvCh 后生成，没有因后台入队或丢弃超窗帧释放远端发送窗口。

## ACK 与关闭

接收 ACK 使用一个固定 worker 和一个合并累计槽，至多一个 ACK RPC 等待受理，不为每帧创建 goroutine。受理失败关闭本 Relay 并保留原始错误。远端 CLOSE/ERROR 等待先前接收 ACK 的 RPC 受理；Abort/断线可取消等待。

本地 Close 等待发送队列、DATA RPC 受理、可靠端到端 ACK，以及已入站受理 DATA 对应的 ACK 生成和受理，再发 CLOSE 并等待其受理。接收屏障在 DATA 进入分发队列之前登记，因此应用刚读取最后一帧、分发 worker 尚未生成 ACK 时，Close 也不能越过它。ACK1 在途、ACK2 位于合并槽时，必须连 ACK2 的受理也完成才能发送 CLOSE。CloseTimeout 从等待发送串行锁之前开始，统一约束调用返回；Abort、ACK 受理失败保留首次关闭原因。

CLOSE 的单个发送任务与调用方等待分离，结果通道有界；共享写锁或正在进行的 WS 帧写入可以晚于 Relay 的关闭预算结束，但不会继续阻塞 Close 调用，也不会为取消单个 Relay 而中断共享帧。取消时异步注销 pending，不必等待共享写锁释放；正在写的帧仍可能到达远端，取消不代表撤销。底层发送任务仍受客户端连接生命周期与写超时约束，Close 不承诺返回时所有后台任务均已退出。

这是完整关闭，不是半关闭或新增 EOF 协议；本地 Close 不承诺继续消费应用尚未读取的任意入站数据。无限慢读和有限 MaxRetransmits 仍不能保证最终恢复。

重传共享 DATA 受理额度。16 槽满时 timer 重传等待额度，部分释放后可继续；每轮重传后重新启动 ACK timer，持续丢 ACK 最终触发 MaxRetransmits。过期或未来 ACK 不推进窗口，删除仅遍历实际 unacked map。

OnConnection 每入站 Relay 一次异步调用。断线批量关闭、入队溢出、主动 Close 的成功或失败、Close 总超时先关闭状态、取消及移除，再异步通知 OnClose；阻塞回调不阻塞共享 reader 或主动 Close。直接 Abort 和其他单 Relay worker 的关闭仍可能同步通知，已关闭后注册 OnClose 仍同步通知注册者。

## 显式 Flush 屏障

`RelayConnection.Flush(ctx)` 将独立的 typed barrier 放入原有有界发送队列，等待此前 DATA 的 RPC 受理及可靠模式的端到端 ACK；BestEffort 无 ACK，只等待受理。屏障不发送 wire 控制帧、不改变连接状态、不等待未来 Send，也不等待应用消费 recvCh。重复、并发 Flush 各自持有独立的容量 1 完成通道，不复用 Close 的一次性 flushCh。

Send、Flush、Close 共用支持 context 的入队锁。Flush 的 context 覆盖等待锁、入队、受理和 ACK；取消只终止本次等待，不撤销已入队数据或关闭 Relay。入队后的已取消屏障仍会由唯一 sendLoop 消费，不创建每屏障后台 goroutine。屏障在 sendLoop 中等待先前 DATA 受理，记录 nextSeq 上界后继续处理未来发送；调用方只观察低于该上界的 unacked，使用 1ms ticker 等待 ACK。断线、Abort 和受理错误保留连接的首个关闭错误。

Close 仍从等待入队锁之前启动原有 CloseTimeout 总预算（默认 5 秒），仍等待终止 DATA 受理和接收 ACK 生成/受理屏障；没有改成活动超时。应用文件流应在其 half_close 入队后使用父 context Flush，再让 Close 承担最后的关闭阶段。应用整体取消可以 Abort，单次 Flush 超时本身不 Abort。

仅当协议正常 CLOSE 是首次关闭原因时，`ReceiveTimeout`（timeout 为 0 时无限等待）会先排空已经接受的缓冲 DATA，再返回原终止错误。Abort、断线和协议错误不启用该排空行为，即使 Abort 携带 remote_close 类型也不例外。原始 Receive 通道不关闭；应用必须通过接收方法处理终止状态。ACK 与 Flush 成功仍只表示 DATA 已进入远端 recvCh，不表示远端应用已经消费或持久化。

## 验证与边界

回归测试覆盖真实 WebSocket 下的 32→16、16→1、256→16 窗口，强制逆序、暂停读、历史重传及恢复；双向各 96 帧暂停读，验证反向 ACK 不依赖正向恢复。正常关闭排空、Abort 来源隔离、Flush 前缀与取消以及并发 Send/Flush/Close 另有针对性测试。

帧数有界不等于跨 Relay 总内存有配额。默认 32KiB 数据约 3MiB 接收 payload 预算；接近既有 1MiB WS 限制的大帧会显著增加内存。未增加跨 Relay 总连接或内存配额，未验证长期丢包和任意大小窗口差异下的重传次数预算。

验证不覆盖所有网络故障、生产 WSS、长期丢包、任意慢消费者或所有连接迁移竞态。关闭排空与慢 TCP 消费采用分层回归，不能视作覆盖全部真实 WebSocket 终止帧故障组合。使用方升级 SDK 后需要重新构建应用，并按自身网络与负载进行验证。
