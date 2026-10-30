# SmartDriver Live Monitor

[![Cross-platform self-test](https://github.com/weathour/smartdriver-live-monitor/actions/workflows/self-test.yml/badge.svg)](https://github.com/weathour/smartdriver-live-monitor/actions/workflows/self-test.yml)

SmartDriver Live Monitor 在 Windows 和 Linux 电脑上显示 Redis 与 UDP 的最新数据，并将一次录制保存为一个 `.sdlog` 文件，便于回看和转移。

当前版本：桌面端 `1.5`，Android 端 `1.0`。页面徽标反映对应 GitHub Actions 工作流的远端运行结果；桌面交付包不含新 APK。

## 快速开始

### Windows

1. 安装 Python 3.10 或更高版本，安装时启用 Python Launcher；图形界面需要 Tcl/Tk。
2. 解压本次桌面交付包，双击 `Self-Test.cmd`。等待 `SELF_TEST=OK`、`NETWORK_SELF_TEST=OK` 和单元测试最终的 `OK`；如有失败，先排查错误。
3. 双击 `Start-SmartDriver-Monitor.cmd`。

### Linux

需要 Python 3.10 或更高版本。Ubuntu/Debian 的图形桌面可先安装 Tkinter 和中文字体：

```bash
sudo apt install python3-tk fonts-noto-cjk
```

解压本次桌面交付包后，在其目录内运行：

```bash
python3 smartdriver_monitor.py --self-test
python3 smartdriver_monitor.py --network-self-test
python3 -m unittest discover -s tests -v
./Start-SmartDriver-Monitor.sh
```

工具仅使用 Python 标准库，不需要 `pip install`。没有图形桌面的电脑可以使用下文的无界面模式。

## 现场操作

1. 在“现场监看与录制”窗口核对 Redis、UDP 和输出目录；修改连接参数后点击“应用并重连”。录制中不能更换连接，未应用的连接改动也不能用于开始录制。
2. 在“Redis 最新值”和“UDP 最新消息”查看各路最新数据、接收时间及状态。没有数据时检查连接；很久未更新的值要结合距上次接收的时间判断。
3. 点击“开始录制”。录制期间仍可查看最新值。
4. 点击“停止录制”，等待停止完成。如果出现写盘错误，保留现场文件并排查，不要把它当作完整录制。
5. 点击“查看最近录制”或“打开录制文件”回看。查看器每页显示 200 条，原始内容预览最多 2048 字节；更长的载荷仍保存在文件中。
6. 将正常停止后的 `.sdlog` 文件复制或交付即可。录制过程中 SQLite 可能产生临时日志文件，请等录制停止后再复制。

默认 Redis 地址是 `192.168.2.112:6379/db0`，默认 UDP 组播地址是 `239.255.76.21:7621`。多网卡电脑收不到 UDP 时，尝试在界面中填写实际接入现场网络的本机 IPv4。

## 录制内容和采样

- **Redis**：以每 1000 ms 一轮为目标读取，包括六个 Unity 键和扫描发现的其他字符串键。每次成功读取都记录，即使值没有变化；请求较慢时实际间隔会更长。未知 schema 的原始内容仍保留。
- **UDP**：按来源 IP、端口及话题区分数据流，目标每秒保存每路最近收到的可接受完整消息。一个采样间隔内的其他消息会合并；没有新消息时不补造记录。停止时会写出未满一轮的待保存消息。计数用于区分采样、合并、重复和迟到情况；LCM 迟到判断依据传输序号，而非业务时间。这是快照录制，不是逐包抓取。
- **LCM**：LC02 去除传输封装后保存消息载荷；LC03 仅在分片完整重组后保存载荷。不完整分片和超限情况作为诊断处理。未获得经核实的业务 schema 时，不生成业务解码或消息级乱序判断。直接小端 `VehicleIntendInfo` 的原有解析路径保留。
- **文件**：选中的原始载荷以 SHA-256 去重，连同观察、清单、事件、配置和结束状态写入 SQLite `.sdlog`。每次成功停止后交付一个文件。

分片处理有上限：单条消息最多 4 MiB，待处理消息、话题和分片共用 16 MiB，最多 64 个分片组和 128 条流；分片超时为 2 秒。超限或不完整的数据不能当成完整消息。

录制事务以约 1 秒为目标批量提交。进程被强制结束或发生存储故障时，尚未提交的最后一批数据可能丢失；负载或存储延迟可能使这段时间更长。未正常收尾的文件不能视为完整录制。

工具记录应用实际收到并选中的内容，不能证明网卡或内核曾收到但没有交给程序的数据。需要逐包检查时，请结合 Wireshark、Windows `pktmon` 或 Linux `tcpdump`。

## 无界面录制

Windows：

```powershell
py -3 smartdriver_monitor.py --headless --duration-seconds 60
```

Linux：

```bash
python3 smartdriver_monitor.py --headless --duration-seconds 60
```

省略 `--duration-seconds` 可持续录制，按 `Ctrl+C` 停止；添加 `--archive` 可在停止并校验后生成 ZIP。需要指定组播网卡时可增加 `--udp-interface 本机IPv4`。完整参数可运行 `python3 smartdriver_monitor.py --help` 查看，Windows 可将 `python3` 换为 `py -3`。

正常停止后，控制台显示录制文件路径和 `VERIFY_CAPTURE=OK`。在 Linux 或 Windows 现场开始长时间录制前，建议先做一次短录制，回看最新值和采样记录。

## 校验、回看和转移

用界面打开 `.sdlog` 可以分页查看记录和有限长度的原始内容预览；打开查看器不会校验文件完整性。需要校验时运行：

```bash
python3 smartdriver_monitor.py --verify-capture /path/to/SmartDriverLiveCapture-时间戳.sdlog
```

Windows 使用 `py -3 smartdriver_monitor.py --verify-capture "录制文件路径"`。成功时输出 `VERIFY_CAPTURE=OK` 和记录计数。校验检查完成状态、SQLite 完整性、记录和原始载荷引用及摘要；它不证明现场网络一定有数据。

通常直接复制 `.sdlog` 即可。如需压缩包，可运行：

```bash
python3 smartdriver_monitor.py --pack-capture /path/to/SmartDriverLiveCapture-时间戳.sdlog
```

命令先校验，再生成 `.sdlog.zip` 和旁置 `.zip.sha256`。旧版 v2/v3 录制目录仍可用同一组 `--verify-capture` 和 `--pack-capture` 命令处理。格式细节见 [录制格式说明](docs/CAPTURE_FORMAT.md)。

## 常见问题

### Redis 连接失败或键为空

检查地址、端口、数据库编号、网络和 Redis ACL。DB 0 不发送 `SELECT`；`SCAN` 或 `TYPE` 不可用时仍尝试六个固定键，`MGET` 受限时降级为逐键 `GET`。单键读取失败不会伪装成键不存在。工具不支持 Redis 密码、TLS 或 Cluster 重定向。

### UDP 监听中但没有数据

核对组播地址、端口和网卡 IPv4，再检查防火墙、VPN 与组播路由。必要时使用系统级抓包工具判断数据是否到达网卡。UDP 计数为零不能直接证明发送端没有发包。

### 文件未通过校验

保留原文件和报错，不要修改内部状态或手工补写摘要。异常中断或写盘失败后的文件可能仍为 `recording` 或 `failed`；这类文件不能作为完整录制交付。

现场准备和通过标准见 [现场测试清单](docs/FIELD_TEST.md)，开发检查见 [CONTRIBUTING.md](CONTRIBUTING.md)。原始录制可能包含位置、车辆标识、轨迹和道路环境信息，外发前请确认数据允许共享。

## Android

Android 1.0 仍是仅在前台运行的实时监视端，不录制文件；本次桌面端的快照和回看能力没有加入 Android。详见 [Android 1.0 监视端说明](https://github.com/weathour/smartdriver-live-monitor/blob/v1.3.0/android/README.md)。

## 许可

本仓库当前没有附加开源许可证。公开可见不等于获得复制、修改或再分发授权。如需采用、集成或发布衍生版本，请先联系仓库所有者。
