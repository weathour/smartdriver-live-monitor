# Windows、Ubuntu、Android 现场测试

## 接口基线

| 数据源 | 默认端点 | 读取内容 |
| --- | --- | --- |
| Redis | `192.168.2.112:6379/db0` | `Sensor_Location`、`Sensor_Mmobstacles`、`Sensor_TrafficLightlist`、`Sensor_Lanelist`、`Sensor_SAM`、`Sensor_CVP` |
| UDP | `239.255.76.21:7621` | Unity `VehicleIntendInfo` 小端直接二进制数据报 |

三端都允许在界面或命令行修改端点。修改现场参数时，三台设备必须填写同一组值。

## 出发前

1. 确认 GitHub Actions 的 `Cross-platform self-test` 成功。该流程会在 Windows、Ubuntu 上运行解析、录制、回环 TCP/UDP 与 Tk 检查，并构建、测试、lint Android。
2. 从成功流程的 `smartdriver-monitor-android-debug` artifact 下载 APK，或在 `android/` 运行 `./gradlew :app:testDebugUnitTest :app:lintDebug :app:assembleDebug`。
3. Windows 双击 `Self-Test.cmd`，应依次看到 `SELF_TEST=OK` 和 `NETWORK_SELF_TEST=OK`。
4. Ubuntu 运行：

   ```bash
   python3 smartdriver_monitor.py --self-test
   python3 smartdriver_monitor.py --network-self-test
   ```

回环网络自检只使用 `127.0.0.1` 临时端口，用于验证真实 TCP/UDP socket、RESP 请求和 UDP 解析链路；它不会访问现场 Redis 或加入组播，也不能替代现场组播检查。

## 现场网络准备

- 三台设备接入能访问发送端的同一现场网络；Android 使用 Wi-Fi。
- 关闭会改写路由的 VPN；确认 AP 未启用客户端隔离。
- Windows/Ubuntu 防火墙允许 Python 接收 UDP 7621；Android 应用保持前台。
- 多网卡电脑和手机先使用 `auto`；收不到 UDP 时填写实际接入现场网络的本机 IPv4。

## 三端执行

### Windows

双击 `Start-SmartDriver-Monitor.cmd`，核对端点后开始录制。也可执行 60 秒无界面测试：

```powershell
py -3 smartdriver_monitor.py --headless --duration-seconds 60
```

### Ubuntu

运行 `./Start-SmartDriver-Monitor.sh`，或执行：

```bash
python3 smartdriver_monitor.py --headless --duration-seconds 60
```

多网卡时增加 `--udp-interface 本机IPv4`。

### Android

安装 `android/app/build/outputs/apk/debug/app-debug.apk`，打开“SmartDriver 数据监看”，核对端点并点击“应用并重连”。测试期间保持应用可见；切到后台会按设计关闭连接且不补录数据。

## 通过标准

- Windows 与 Ubuntu：Redis 状态已连接，六个键持续产生观察；发送端工作时 UDP 计数持续增加，UDP 状态不是 `port-only`。
- Windows 与 Ubuntu：正常停止后输出 `VERIFY_CAPTURE=OK`；`manifest.json` 中 `status=complete`、`counts.redis>0`，发送端工作时 `counts.udp>0`，且 `last_error=null`。
- Android：Redis 显示已连接且观察数持续增加；UDP 显示监听中并列出已加入网卡，发送端工作时 UDP 总数持续增加。
- 三端都能看到合理的解析摘要；解析失败、重复、乱序或尾随字节必须显示为诊断状态，不能被当作无数据。

## 失败定位

- Redis 失败：先核对地址、6379、DB 0、路由、防火墙与 ACL。`MGET` 无权限时工具会降级为逐键 `GET`；单键错误不会显示成“键不存在”。
- UDP 为零：核对 239.255.76.21、7621 和网卡 IPv4，再检查 AP 客户端隔离、VPN、IGMP/组播路由与系统防火墙。用 Wireshark、Windows `pktmon` 或 Linux `tcpdump` 判断数据是否到达网卡。
- Android UDP 为零：确认 Wi-Fi 仍连接现场网络，尝试填写手机 Wi-Fi IPv4；系统或 AP 仍可能在应用已取得组播锁后过滤组播。
- `VERIFY_CAPTURE=OK` 只证明录制文件完整，不证明现场发送端产生了数据；最终以三端观察计数和解析内容为准。
