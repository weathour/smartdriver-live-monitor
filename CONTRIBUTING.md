# 参与开发

桌面端刻意保持为一个 Python 标准库脚本，以及 Windows 和 Linux 的薄启动脚本；Android 端刻意保持为原生 Kotlin/Android API。提交改动前先确认问题不能通过现有代码解决，不要为单一实现增加接口层、插件系统或依赖管理框架。

## 本地检查

最低检查命令：

```bash
python3 smartdriver_monitor.py --self-test
python3 smartdriver_monitor.py --network-self-test
python3 -m py_compile smartdriver_monitor.py
```

Windows 上也应运行：

```powershell
py -3 smartdriver_monitor.py --self-test
py -3 smartdriver_monitor.py --network-self-test
```

涉及图形界面的改动还应在 Windows 和 Linux 各创建一次 Tk 窗口。Linux 可在安装 `python3-tk`、`xvfb` 和中文字体后使用 `xvfb-run`；仓库 CI 已执行这项检查。

Android 改动的最低检查命令：

```bash
cd android
./gradlew :app:testDebugUnitTest :app:assembleDebug :app:lintDebug
```

涉及组播或生命周期的改动还应在 Android 8.0 以上真机确认：应用前台时可接收，切到后台后 Redis/UDP 套接字和组播锁均已释放。

网络自检只连接 `127.0.0.1` 临时端口，不连接现场 Redis 或加入现场组播。涉及组播、网卡选择或防火墙的改动还需要按 [现场测试清单](docs/FIELD_TEST.md) 在授权网络中验证。

## 不可破坏的行为

- 原始字节必须在解析前持久化。
- 解析失败、重复、乱序或含尾随字节的数据报不能被过滤掉。
- Redis 每次轮询记录不能因为内容相同而从时间线消失。
- Redis 兼容降级不能把命令被拒绝伪装成键不存在，也不能吞掉连接或 RESP 协议错误。
- 图形界面和无界面模式必须调用同一套录制、校验和打包逻辑。
- Android 不得写 Redis、录制载荷或用后台服务伪装持续监视；界面丢弃必须有可见计数。
- 校验器必须拒绝摘要不符、额外文件、缺失文件、越界路径、坏 JSONL 和计数不符。
- 新依赖需要说明为什么 Python 标准库和现有实现不能满足需求。

如果录制字段或含义变化，请同步更新：

1. `format_version` 或 `recorder_version`
2. 自检样例和断言
3. `docs/CAPTURE_FORMAT.md`
4. `README.md` 中受影响的使用说明

## 提交问题

问题单请包含操作系统、Python 版本、完整命令、控制台错误和 `manifest.json` 中的 `config`、`counts`、`last_error`。原始录包可能包含敏感信息，确认允许外发后再上传。

不要只写 UDP 没数据。请说明系统级抓包是否看到目标组播、使用了哪块网卡，以及防火墙或 VPN 是否启用。
