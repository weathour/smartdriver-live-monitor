# 现场测试清单

本清单用于现场验证桌面端 1.5。当前本地自动测试使用伪数据和回环网络；尚无本次变更的 Windows GUI 真机或现场网络验证。

## 接口基线

| 数据源 | 默认端点 | 检查内容 |
| --- | --- | --- |
| Redis | `192.168.2.112:6379/db0` | 六个 Unity 键和扫描发现的字符串键；目标每秒一轮 |
| UDP | `239.255.76.21:7621` | 各来源和话题的最新完整消息；目标每秒记录一次 |

如果现场端点不同，在图形界面修改后点击“应用并重连”。需要比较不同设备时，应填写同一组端点。

## 出发前

1. Windows 双击 `Self-Test.cmd`，等待 `SELF_TEST=OK`、`NETWORK_SELF_TEST=OK` 和单元测试最终的 `OK` 后再关闭窗口；在实际 Windows 电脑上检查窗口启动、应用设置、录制、停止和回看。
2. Ubuntu 运行：

   ```bash
   python3 smartdriver_monitor.py --self-test
   python3 smartdriver_monitor.py --network-self-test
   python3 -m unittest discover -s tests -v
   ```

3. 解压本次桌面交付包，确认输出目录可写且有足够空间。回环网络自检只连接本机临时端口，不访问现场 Redis 或组播。

## 现场准备

- 电脑接入能够访问 Redis 和 UDP 发送端的网络；核对地址、端口、数据库和 Redis ACL。
- 关闭会改写路由的 VPN，确认防火墙允许接收 UDP。多网卡电脑收不到组播时填写现场网卡的本机 IPv4。
- 如需同时用 Android 监视，使用已有的 Android 1.0 APK，保持应用前台并连接现场 Wi-Fi；本次交付包不含新 APK。

## 现场执行

1. Windows 双击 `Start-SmartDriver-Monitor.cmd`；Ubuntu 运行 `./Start-SmartDriver-Monitor.sh`。
2. 核对连接和输出目录；修改连接参数后先点击“应用并重连”。在“Redis 最新值”查看最后观察时间和内容未变化时长；在“UDP 最新消息”查看来源、接收时间和距上次接收的时间。
3. 点击“开始录制”，让发送端按现场工况持续工作。界面按流显示最新值；UDP 会按每路每秒保留最新完整消息，中间消息会合并。
4. 点击“停止录制”，等待完成。点击“查看最近录制”或“打开录制文件”，分页检查记录和原始载荷预览，再复制停止后的 `.sdlog` 文件。
5. 需要命令行确认时，在交付包目录运行以下命令；Windows 用 `py -3` 替换 `python3`：

   ```bash
   python3 smartdriver_monitor.py --verify-capture /path/to/录制文件.sdlog
   ```

也可以执行 60 秒无界面测试：

```bash
python3 smartdriver_monitor.py --headless --duration-seconds 60
```

多网卡时增加 `--udp-interface 本机IPv4`。

## 通过标准

- Redis 能连接，具备读取权限的键持续产生观察；发送端工作且组播加入成功时，UDP 最新消息持续更新。较慢的 Redis 请求可能使实际观察间隔超过 1 秒。
- UDP 数据不再到达时，界面显示上次接收时间和经过时长，不把旧值当作新接收的数据，也不补造快照。Redis 的内容未变化时长应与最后观察时间分开判断。
- 正常停止后得到一个 `.sdlog`，可在窗口中打开并分页回看；命令行校验输出 `VERIFY_CAPTURE=OK`。
- UDP 高频发送时，采样与合并计数应能反映快照行为；重复、迟到、超限或未完成分片的诊断不应被当作完整消息。
- 已知小端直传数据可检查解析摘要；LCM 的完整载荷可记录，但未核实业务 schema 时不能将其视为业务解码成功。

## 失败定位

- Redis 失败：核对地址、端口、DB 0、路由、防火墙和 ACL。`MGET` 不可用时工具会降级为逐键 `GET`；单键错误不会显示成“键不存在”。
- UDP 为零：核对组播地址、端口、网卡 IPv4、VPN、IGMP/组播路由和防火墙。用 Wireshark、Windows `pktmon` 或 Linux `tcpdump` 判断数据是否到达网卡。
- 保存或校验失败：保留 `.sdlog` 和报错。强制退出或磁盘故障可能丢失尚未提交的一批记录；未正常收尾的文件不应作为完整录制。
- `VERIFY_CAPTURE=OK` 证明文件通过完整性校验，不证明发送端确实产生了数据；还需检查各路的观察时间、计数和内容。
