# SmartDriver Android 监视端

这是桌面端的只读、前台监视版本。它直接连接现场 Redis，并直接接收 UDP 组播；不依赖桌面端转发。

## 已实现

- 每 100 ms 通过 Redis `MGET` 读取六个固定 Unity 键；权限不允许时降级为逐键 `GET`。
- 非零 DB 只额外发送 `SELECT`，不发送任何写命令。
- 显示键是否存在、字节数、最后观察、内容未变化时长、SHA-256、protobuf 解析状态与摘要。
- 获取 Android Wi-Fi 组播锁，监听默认 `239.255.76.21:7621`，支持 `auto` 或指定网卡 IPv4。
- 显示 UDP 来源、长度、相邻间隔、协议字段、重复、乱序、尾随字节和解析失败。
- 点击条目查看解析详情与最多前 4096 字节原始十六进制。
- UI 事件队列和 UDP 列表均有上限；总计数与 UI 丢弃数单独显示。

## 明确不做

- 不录制、不打包、不申请存储权限；只用应用私有设置保存连接参数。
- 不在后台运行；Activity 不可见时关闭连接并释放组播锁。
- 不扫描 Redis 全库，只看桌面端当前定义的六个 Unity 键。
- 不支持 Redis 密码、TLS、Cluster 或 LCM 解包。
- 不包含语音识别、TTS、麦克风权限或相关第三方运行时库。

## 构建

需要 JDK 17、Android SDK 35 和网络可访问的首次 Gradle 依赖缓存：

```bash
./gradlew :app:testDebugUnitTest :app:assembleDebug :app:lintDebug
```

APK：`app/build/outputs/apk/debug/app-debug.apk`

安装到已连接设备：

```bash
adb install -r app/build/outputs/apk/debug/app-debug.apk
```

## 现场检查

三端统一步骤和通过标准见 [Windows、Ubuntu、Android 现场测试](../docs/FIELD_TEST.md)。Android 端快速检查：

1. 手机和 Redis/UDP 发送端接入同一可互访网络，关闭 AP 客户端隔离。
2. 保持应用在前台，核对 Redis 与 UDP 状态均不是“错误”。
3. Redis 行的“观察”持续更新，证明轮询仍在进行；“内容未变化”持续增加只说明返回字节相同。
4. 发送端确实发送 UDP 时，UDP 总数应增加。为零时指定 Wi-Fi 网卡 IPv4，并用电脑抓包确认 AP 是否转发组播。
5. 切到后台再返回，应用会重新连接；后台期间没有监视记录可供补回。
