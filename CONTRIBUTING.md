# 参与开发

桌面端由主脚本和 `desktop_log.py`、`udp_latest.py`、`log_viewer.py` 三个模块组成，仅使用 Python 标准库，并配有 Windows 和 Linux 薄启动脚本；Android 端保持为原生 Kotlin/Android API。提交改动前先确认问题不能通过现有代码解决，不要为单一实现增加接口层、插件系统或依赖管理框架。

## 本地检查

最低检查命令：

```bash
python3 smartdriver_monitor.py --self-test
python3 smartdriver_monitor.py --network-self-test
python3 -m unittest discover -s tests -v
python3 -m py_compile smartdriver_monitor.py desktop_log.py udp_latest.py log_viewer.py
```

Windows 上也应运行：

```powershell
py -3 smartdriver_monitor.py --self-test
py -3 smartdriver_monitor.py --network-self-test
py -3 -m unittest discover -s tests -v
```

涉及图形界面的改动还应运行 `python3 tests/gui_smoke.py`，检查录制、最新值及回看；Windows 可使用 `py -3 tests/gui_smoke.py`。无图形桌面的 Linux 可安装 `python3-tk`、`xvfb` 和中文字体后运行 `xvfb-run -a python3 tests/gui_smoke.py`。

Android 改动的最低检查命令：

```bash
cd android
./gradlew :app:testDebugUnitTest :app:assembleDebug :app:lintDebug
```

涉及组播或生命周期的改动还应在 Android 8.0 以上真机确认：应用前台时可接收，切到后台后 Redis/UDP 套接字和组播锁均已释放。

自动测试使用本地伪数据和回环网络；网络自检只连接 `127.0.0.1` 临时端口，不连接现场 Redis 或加入现场组播。涉及组播、网卡选择或防火墙的改动还需要按 [现场测试清单](docs/FIELD_TEST.md) 在授权网络中验证。

## 不可破坏的行为

- 选中的原始载荷应在业务解析前写入录制事务；批量提交期间尚未提交的数据在异常退出时可能丢失。
- UDP 按来源和话题保留每秒最新的完整消息；中间消息会合并，不得把录制声称为逐包抓取。重复、迟到及未完成分片应有诊断，不能伪装成完整业务消息。
- Redis 成功读取不能因为内容相同而从时间线消失，也不能在没有成功读取时补造新观察。
- Redis 兼容降级不能把命令被拒绝伪装成键不存在，也不能吞掉连接或 RESP 协议错误；每个成功的 `GET` 应及时写入事务，后续请求失败不能抹去同轮已读值。
- LC02 剥去传输头后保存消息载荷；LC03 仅在完整重组后保存消息载荷。分片处理及待处理流必须有上限；没有经核实的 schema 时，不生成业务 `decoded` 或消息级乱序判断。
- 图形界面和无界面模式必须调用同一套录制、校验和打包逻辑。
- 图形界面只能用已应用的连接配置开始录制，录制期间不能更换连接；收尾失败不能标记为完整录制。
- Android 不得写 Redis、录制载荷或用后台服务伪装持续监视；界面丢弃必须有可见计数。
- v4 单文件校验必须检查完成状态、数据库结构、计数和原始载荷摘要。旧 v2/v3 目录的校验与打包仍须使用一致的文件集合，并拒绝 `last_error` 非空、摘要不符、额外或缺失文件、越界路径、坏 JSONL 和计数不符；子目录同名 `SHA256SUMS` 也不能跳过。
- 新依赖需要说明为什么 Python 标准库和现有实现不能满足需求。

如果录制字段或含义变化，请同步更新：

1. `format_version` 或 `recorder_version`
2. 自检样例和断言
3. `docs/CAPTURE_FORMAT.md`
4. `README.md` 中受影响的使用说明

## 提交问题

问题单请包含操作系统、Python 版本、完整命令、控制台错误，以及录制中的配置、计数、结束状态和错误信息。原始录制可能包含敏感信息，确认允许外发后再上传。

不要只写 UDP 没数据。请说明系统级抓包是否看到目标组播、使用了哪块网卡，以及防火墙或 VPN 是否启用。
