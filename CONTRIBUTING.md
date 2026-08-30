# 参与开发

这个项目刻意保持为一个 Python 标准库脚本和两个 Windows 启动文件。提交改动前先确认问题不能通过现有代码解决，不要为单一实现增加接口层、插件系统或依赖管理框架。

## 本地检查

最低检查命令：

```bash
python3 smartdriver_monitor.py --self-test
python3 -m py_compile smartdriver_monitor.py
```

Windows 上也应运行：

```powershell
py -3 smartdriver_monitor.py --self-test
```

自检不连接现场 Redis，也不加入现场 UDP 组播。涉及套接字行为的改动还需要在授权网络中做短录制，并运行 `--verify-capture`。

## 不可破坏的行为

- 原始字节必须在解析前持久化。
- 解析失败、重复、乱序或含尾随字节的数据报不能被过滤掉。
- Redis 每次轮询记录不能因为内容相同而从时间线消失。
- 图形界面和无界面模式必须调用同一套录制、校验和打包逻辑。
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
