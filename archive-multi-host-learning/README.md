# Aeron Archive Multi-host Learning

这个模块用于分阶段独立编写最小 Multi-host Archive 示例，不复制官方示例中的 Agent、状态机和 Listener。

## 当前阶段：Host 录制流程

Host 对象骨架已经完成。当前打开 `MinimalArchiveHost.java`，按顺序完成两个留白：

1. `publishMessages`：发送少量已知消息，返回最终 `publication.position()`。
2. `awaitRecordingPosition`：找到 `RecordingPos` counter，等待 Archive 录制进度追上目标 Position。

`listRecordingsForUri` 暂时不属于 Host 这一阶段，它会放到后续 Client 查询/Replay 流程中。

完成后运行：

```bash
./gradlew :archive-multi-host-learning:compileJava
./gradlew :archive-multi-host-learning:run
```

本阶段成功标准：程序输出 Recording ID，并在发送消息后确认 Recording Position 追上 Publication Position。

## 下一阶段：Client Replay 骨架

`MinimalArchiveClient.java` 与 Host 分开，按以下顺序填写：

```text
Client MediaDriver
→ Client Aeron
→ AeronArchive Client
→ listRecordingsForUri(fromRecordingId=0, ...)
→ Replay Subscription(endpoint=localhost:0)
→ tryResolveChannelEndpointPort()
→ startReplay(..., replayStreamId=200)
→ Subscription.poll()
```

`fromRecordingId=0` 只用于首次扫描；分页时从上一页最后返回的 `recordingId + 1` 开始。
