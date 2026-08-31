# SmartDriver Live Monitor

[![Cross-platform self-test](https://github.com/weathour/smartdriver-live-monitor/actions/workflows/self-test.yml/badge.svg)](https://github.com/weathour/smartdriver-live-monitor/actions/workflows/self-test.yml)

SmartDriver Live Monitor 是一套 Redis 与 UDP 现场监看工具。Windows/Ubuntu 桌面端还负责录制与校验；Android 端只负责前台实时监视。它用于回答三个具体问题：

1. Unity 依赖的 Redis 键当前是否存在，内容是否更新，能否按已知格式解析。
2. 当前接收端是否收到了指定组播地址上的 UDP 数据报，是否有重复、乱序或尾随字节。
3. 桌面端的一次录制能否完整停止、校验并打包成便于机器读取和跨电脑转移的文件。

同一份 `smartdriver_monitor.py` 同时支持 Windows 与 Linux 图形界面，也支持两端的无界面录制。`android/` 是不额外引入 Redis、网络或 UI 库的原生 Kotlin 监视端，参考仓库现有安卓语音应用的裸 RESP 客户端与原生界面方式实现，但没有复制语音、TTS 或后台麦克风服务。

当前版本：桌面端 `1.3`；Android 端 `1.0`

## 桌面端能录到什么

### Redis

默认连接 `192.168.2.112:6379/db0`，每 100 ms 轮询一次。工具会重点关注 Unity 当前使用的六个键：

- `Sensor_Location`
- `Sensor_Mmobstacles`
- `Sensor_TrafficLightlist`
- `Sensor_Lanelist`
- `Sensor_SAM`
- `Sensor_CVP`

工具也会定期执行 `SCAN`，把发现的其他字符串键纳入录制，并记录键名、Redis 类型、是否由扫描发现、是否属于 Unity 当前键或旧版候选键。每次轮询都会保留一条时间线记录；相同内容只保存一份原始二进制文件，通过 SHA-256 引用，避免重复占用空间。

Redis 兼容策略以 Unity 的实际读取权限为下限：DB 0 不发送 `SELECT`；`SCAN` 或 `TYPE` 不可用时仍读取六个固定键；`MGET` 被权限规则拒绝或服务器不支持时，当前连接自动改用逐键 `GET`。`discovery_method` 和 `read_method` 会把实际模式写入 JSONL；降级和单键读取错误也会写入运行日志，不会把“无权读取”伪装成“键不存在”。

### UDP

默认监听组播 `239.255.76.21:7621`。Python 套接字收到的每个 UDP 数据报都会保存，包括：

- UTC 时间和单调时钟时间
- 数据报序号、来源 IP 和端口
- 与上一包的到达间隔
- 长度、SHA-256、前 24 字节十六进制摘要
- 原始二进制内容
- 已知协议的解析结果、重复和乱序判断

解析失败、重复、乱序或含尾随字节的数据报不会被丢弃。

### 录制结果

一次正常结束的录制包含：

- `manifest.json`：版本、配置、环境、计数和结束状态
- 四个 JSON Lines 时间线文件，适合逐行流式读取
- 按 SHA-256 命名的 Redis 和 UDP 原始二进制内容
- `SHA256SUMS`：录制目录内所有其他文件的完整性清单
- 可选 ZIP 包和旁置的 `.zip.sha256` 摘要

字段定义见 [录制格式说明](docs/CAPTURE_FORMAT.md)。

## 边界说明

这不是 PCAP 抓包器。它记录应用层实际收到的内容：UDP 以 Python 套接字收到的数据报为边界，Redis 以客户端查询得到的值为边界。因此：

- 工具能证明某条数据已经到达 Python 应用。
- 工具不能证明网卡或内核曾收到但没有交给 Python 的数据包。
- Redis 是 100 ms 轮询快照，不是 Redis 服务端推送事件。
- 桌面图形界面只保留最近 500 条显示记录，磁盘录制不受这个显示上限影响。
- Android 界面只保留最近 50 个 UDP datagram，不把监视载荷写入磁盘；事件过快时会明确显示 UI 丢弃数。

如果 UDP 计数为零，需要结合 Windows `pktmon`、Wireshark 或 Linux `tcpdump` 检查网卡、组播路由和防火墙。不要把零计数直接解释成发送端没有发包。

## 运行要求

- Windows 10/11 或常见 Linux 发行版
- Python 3.10 或更高版本
- 不需要 `pip install`
- Windows 图形界面需要 Python 自带的 Tcl/Tk
- Linux 图形界面需要图形桌面、Tkinter 和可显示中文的系统字体
- Linux 服务器可以使用无界面模式，不需要桌面环境或 Tcl/Tk
- Android 8.0（API 26）或更高版本；手机需与 Redis/UDP 发送端网络可达
- 构建 Android APK 需要 JDK 17 和 Android SDK 35；已安装 APK 不需要 Python

项目的持续集成会在 Windows 和 Ubuntu 上分别使用 Python 3.10 与 3.12 运行解析、录制与本机 TCP/UDP socket 自检，并实际创建一次 Tk 窗口；Android job 会运行单元测试、lint、构建 APK 并上传 artifact。

## 下载

最简单的方式是从 [Releases](https://github.com/weathour/smartdriver-live-monitor/releases) 下载版本 ZIP，解压后运行。也可以克隆仓库：

```bash
git clone https://github.com/weathour/smartdriver-live-monitor.git
cd smartdriver-live-monitor
```

从 `v1.3.0` 起，Release 同时提供 Android 1.0 debug APK；也可从成功的 GitHub Actions 运行中下载 `smartdriver-monitor-android-debug` artifact。

## Windows 使用方法

### 第一次运行

1. 安装 Python 3，并勾选将 Python Launcher 加入系统。
2. 双击 `Self-Test.cmd`。
3. 看到 `SELF_TEST=OK` 后关闭窗口。
4. 双击 `Start-SmartDriver-Monitor.cmd` 启动图形界面。

图形界面中先核对 Redis、UDP 和输出目录，再点击开始录制。结束时点击停止录制，等待界面报告录制已完成。不要直接关机或强制结束 Python 进程。

### 无界面录制

录制 60 秒并自动校验：

```powershell
py -3 smartdriver_monitor.py --headless --duration-seconds 60
```

持续录制，按 `Ctrl+C` 停止：

```powershell
py -3 smartdriver_monitor.py --headless
```

录制完成后自动生成 ZIP：

```powershell
py -3 smartdriver_monitor.py --headless --duration-seconds 60 --archive
```

## Linux 使用方法

### 图形界面

Ubuntu 或 Debian 先安装 Tkinter 和中文字体：

```bash
sudo apt install python3-tk fonts-noto-cjk
```

其他发行版请安装其 Python Tk/Tkinter 软件包和中文字体，然后在图形桌面中运行：

```bash
./Start-SmartDriver-Monitor.sh
```

也可以直接运行：

```bash
python3 smartdriver_monitor.py
```

启动参数会作为图形界面的初始值，例如连接旧版开发环境：

```bash
./Start-SmartDriver-Monitor.sh --redis-host 192.168.0.108
```

界面、操作步骤和录制结果与 Windows 相同。

### 无界面录制

先运行离线自检：

```bash
python3 smartdriver_monitor.py --self-test
```

录制 60 秒并自动校验：

```bash
python3 smartdriver_monitor.py --headless --duration-seconds 60
```

持续录制并在 `Ctrl+C` 后生成 ZIP：

```bash
python3 smartdriver_monitor.py --headless --archive
```

如果主机有多块网卡，明确指定接收组播的本机 IPv4 地址：

```bash
python3 smartdriver_monitor.py --headless --udp-interface 192.168.2.50
```

也可以用逗号指定多个地址。`auto` 会尝试通配地址和工具发现的本机 IPv4 地址。

## Android 使用方法

Android 端在应用可见时每 100 ms 读取六个固定 Unity Redis 键，并直接加入 `239.255.76.21:7621` 组播。界面显示连接状态、观察与变化计数、内容未变化时长、解析摘要、SHA-256、UDP 重复/乱序/尾随标签；点击 Redis 或 UDP 条目可查看解析详情和原始十六进制。

构建并运行单元检查：

```bash
cd android
./gradlew :app:testDebugUnitTest :app:lintDebug :app:assembleDebug
```

Windows 使用 `gradlew.bat`。生成的 APK 位于：

```text
android/app/build/outputs/apk/debug/app-debug.apk
```

安装后填写 Redis、UDP 和可选网卡 IPv4，点击“应用并重连”。`auto` 会尝试手机当前可用的 IPv4 网卡；多网络设备收不到组播时，填写 Wi-Fi 网卡的具体 IPv4。

Android 端没有录制按钮、监视数据文件、后台服务、Redis `SCAN`、Redis 密码/TLS 或 Redis Cluster 支持；只在本机保存连接设置。切到后台时会立即关闭 Redis/UDP 套接字并释放组播锁。完整说明见 [Android 监视端说明](android/README.md)。

三类设备的统一准备、执行、通过标准和故障定位见 [现场测试清单](docs/FIELD_TEST.md)。

## 录制前检查

先做一次 10 秒短录制：

```bash
python3 smartdriver_monitor.py --headless --duration-seconds 10
```

正常停止时应看到：

```text
SELF_TEST=OK
CAPTURE_DIR=...
VERIFY_CAPTURE=OK
```

然后检查输出目录中的 `manifest.json`：

- `status` 必须是 `complete`。
- `counts.redis` 大于零，表示 Redis 轮询记录已经写入。
- `counts.inventory` 大于零，表示 Redis 键清单已经写入。
- 发送端确实在发 UDP 时，`counts.udp` 应大于零。
- `last_error` 应为 `null`。

同时查看控制台或 `events.jsonl`：如果出现 `using configured Unity keys` 或 `using individual GET commands`，说明工具已进入兼容降级，但固定 Unity 键仍会继续读取。某个键出现 `Redis GET failed` 则表示该键未被伪装为缺失，需要检查它的 ACL 或数据类型。

`VERIFY_CAPTURE=OK` 只说明已有文件结构、摘要、行数和引用关系一致，不说明现场一定存在 Redis 或 UDP 流量。流量是否满足要求仍要看计数和内容。

## 常用参数

| 参数 | 默认值 | 作用 |
| --- | --- | --- |
| `--self-test` | 无 | 离线检查解析器、录制器、校验器和打包器 |
| `--network-self-test` | 无 | 用本机临时端口检查真实 Redis TCP/RESP 与 UDP socket 读取链路 |
| `--headless` | 无 | 不启动 Tk，适用于 Windows 和 Linux |
| `--redis-host` | `192.168.2.112` | Redis 地址；也作为图形界面的初始值 |
| `--redis-port` | `6379` | Redis 端口 |
| `--redis-database` | `0` | Redis 数据库编号 |
| `--udp-group` | `239.255.76.21` | IPv4 组播地址 |
| `--udp-port` | `7621` | UDP 端口 |
| `--udp-interface` | `auto` | 接收组播的本机 IPv4 地址，可用逗号分隔 |
| `--output-root` | 用户文档目录下的 `SmartDriverCaptures` | 所有录制目录的父目录 |
| `--duration-seconds` | `0` | 无界面录制时长；`0` 表示等待 `Ctrl+C` |
| `--archive` | 关闭 | 无界面录制结束并校验后生成 ZIP |
| `--verify-capture DIR` | 无 | 校验一个已完成的录制目录 |
| `--pack-capture DIR` | 无 | 校验目录后生成 ZIP 和 SHA-256 旁置文件 |

查看本机的完整帮助：

```bash
python3 smartdriver_monitor.py --help
```

## 校验和转移

校验录制目录：

```bash
python3 smartdriver_monitor.py --verify-capture /path/to/SmartDriverLiveCapture-时间戳
```

成功时会输出 `VERIFY_CAPTURE=OK` 和四类记录计数。

将已完成的目录打包：

```bash
python3 smartdriver_monitor.py --pack-capture /path/to/SmartDriverLiveCapture-时间戳
```

命令会先做完整校验，再在录制目录旁生成：

```text
SmartDriverLiveCapture-时间戳.zip
SmartDriverLiveCapture-时间戳.zip.sha256
```

ZIP 内保留顶层录制目录，便于解压多个批次而不互相覆盖。目标 ZIP 或旁置摘要已存在时，工具会拒绝覆盖。

## 常见问题

### Redis 一直连接失败

检查目标地址、端口、数据库编号、路由、防火墙和 Redis 服务状态。DB 0 不需要 `SELECT` 权限；非零 DB 仍需要。工具不支持 Redis 密码或 TLS；如果现场 Redis 启用了其中任一项，需要先明确接口方案。

当前版本也不处理 Redis Cluster 的 `MOVED/ASK` 重定向，不应把 `CROSSSLOT` 错误解释为已经兼容集群。

### Redis 已连接，但某个键一直为空

查看 `redis-inventory.jsonl`，确认键名、大小写和类型。扫描可用时，只有 Redis 类型为 `string` 或 `none` 的键进入读取；扫描不可用时，工具直接逐个尝试六个固定键，并在 `events.jsonl` 记录失败。

### UDP 状态为监听中，但计数为零

先确认发送端地址和端口，再尝试用 `--udp-interface` 指定有线网卡的 IPv4 地址。继续为零时，用系统级抓包工具判断数据是否到达网卡。虚拟网卡、VPN、防火墙和组播路由都会影响接收。

Android 上还需确认手机使用 Wi-Fi 而非蜂窝网络、路由器/AP 没有启用客户端隔离，并尝试在应用中填写 Wi-Fi 网卡的具体 IPv4。应用会申请 Wi-Fi 组播锁，但系统或 AP 仍可能过滤组播。

### Linux 报告 Tkinter 不可用

Ubuntu/Debian 安装 `python3-tk fonts-noto-cjk`，并从图形桌面运行。如果机器没有显示服务（例如纯 SSH 服务器），使用 `--headless`；无界面录制不依赖 Tk。

### 目录无法通过校验

保留原目录，不要手工补摘要或改计数。先根据错误定位缺失、额外或摘要不符的文件。`status` 为 `recording` 或 `failed` 的目录不属于可交付的完整录制。

## 数据安全

录制内容可能包含位置、车辆标识、轨迹、障碍物和信号灯信息。公开上传、发送给第三方或放入问题单前，应先确认数据允许外发。完整性校验用于发现文件变化，不提供加密或访问控制。

## 项目原则

- 原始字节先落盘，再执行协议解析。
- 解析错误、重复、乱序和尾随数据都保留。
- 磁盘时间线与界面显示相互独立。
- Windows 和 Linux 共用同一个图形界面与录制核心。
- 仅使用 Python 标准库，降低现场安装风险。
- Android 端只执行只读 Redis 命令并接收 UDP，不录制也不在后台持续运行。
- 状态文案区分已验证事实和需要系统级工具确认的边界。
- 格式变化必须更新版本、校验器和格式文档。

开发和提交要求见 [CONTRIBUTING.md](CONTRIBUTING.md)。

## 许可

本仓库当前没有附加开源许可证。公开可见不等于获得复制、修改或再分发授权。如需采用、集成或发布衍生版本，请先联系仓库所有者。
