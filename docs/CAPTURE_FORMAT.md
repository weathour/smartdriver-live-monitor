# SmartDriver 录制格式 v2

本文说明 SmartDriver Live Monitor `1.1` 写出的 `format_version: 2`。该格式把时间线、原始内容、运行环境和完整性信息分开保存，便于脚本逐行读取，也便于整包转移。

## 目录结构

```text
SmartDriverLiveCapture-YYYYMMDD-HHMMSS-mmm/
├── manifest.json
├── SHA256SUMS
├── redis-inventory.jsonl
├── redis-observations.jsonl
├── udp-observations.jsonl
├── events.jsonl
└── payloads/
    ├── redis/
    │   └── <sha256>.bin
    └── udp/
        └── <sha256>.bin
```

所有 JSON 和 JSON Lines 文件使用 UTF-8。二进制文件保持工具实际收到的字节，不做转码。所有相对路径统一使用 `/`。

## manifest.json

`manifest.json` 是本次录制的入口。主要字段如下：

| 字段 | 类型 | 含义 |
| --- | --- | --- |
| `format_version` | integer | 当前固定为 `2` |
| `recorder` | string | 当前为 `SmartDriverLiveMonitor` |
| `recorder_version` | string | 写出该目录的软件版本 |
| `status` | string | `recording`、`complete` 或 `failed` |
| `started_at_utc` | string | UTC 开始时间 |
| `ended_at_utc` | string 或 null | UTC 结束时间 |
| `capture_layer` | string | 当前为 `application_socket` |
| `config` | object | Redis、UDP 和轮询配置 |
| `counts` | object | 四个 JSONL 文件的行数 |
| `last_error` | string 或 null | 最后一次录制错误 |
| `environment` | object | 操作系统、CPU 架构和 Python 信息 |
| `files` | object | 文件用途说明 |
| `semantics` | object | Redis、UDP 和捕获边界说明 |

可交付录制必须满足：

```json
{
  "format_version": 2,
  "status": "complete",
  "last_error": null
}
```

`counts` 使用四个键：

```json
{
  "events": 4,
  "inventory": 1,
  "redis": 600,
  "udp": 125
}
```

计数可以是零。零表示对应 JSONL 文件没有记录，不能单独用来判断网络上是否曾经存在数据。

## JSON Lines 文件

JSON Lines 文件每行是一个独立 JSON 对象，行尾为换行符。读取方可以流式处理，不必一次载入整个文件。合法文件不含空行。

### redis-inventory.jsonl

Redis 键清单只在录制开始或清单发生变化时写入。

| 字段 | 含义 |
| --- | --- |
| `type` | `redis_inventory` |
| `observed_at_utc` | 观察时间 |
| `endpoint` | `host:port/dbN` |
| `keys` | 键条目数组 |

每个 `keys` 条目包含：

- `key`：Redis 键名
- `type`：Redis 返回的类型
- `seen_by_scan`：是否由 `SCAN` 发现
- `unity_key`：是否属于当前六个 Unity 键
- `legacy_candidate`：是否属于已知旧版候选键

### redis-observations.jsonl

每个已纳入读取的键在每轮轮询中写一行。

| 字段 | 含义 |
| --- | --- |
| `kind` | `redis_observation` |
| `sequence` | 轮询序号，同一轮的多个键共享序号 |
| `observed_at_utc` | UTC 观察时间 |
| `monotonic_ns` | 本机单调时钟纳秒值，只适合同一进程内排序和计算间隔 |
| `round_trip_ms` | 本轮 `MGET` 往返耗时 |
| `endpoint` | Redis 端点 |
| `key` | Redis 键名 |
| `redis_type` | 最近一次扫描得到的类型 |
| `present` | 值是否存在 |
| `changed` | 相比本进程对该键的上一条观察是否变化 |
| `length` | 原始值字节数 |
| `sha256` | 原始值摘要；不存在时为 null |
| `payload` | 原始值相对路径；不存在时没有此字段 |
| `parse` | 已知格式的解析结果或解析错误说明 |

`changed: false` 也可能表示这是进程首次观察该键，不应解释为录制前后内容一定没变。

### udp-observations.jsonl

Python 套接字每返回一个 UDP 数据报，就写一行。

| 字段 | 含义 |
| --- | --- |
| `kind` | `udp_datagram` |
| `sequence` | 本进程 UDP 接收序号 |
| `observed_at_utc` | UTC 到达时间 |
| `monotonic_ns` | 本机单调时钟纳秒值 |
| `inter_arrival_ms` | 与上一数据报的到达间隔；首包为 null |
| `source_ip`、`source_port` | 发送端地址 |
| `listen_endpoint` | 配置的组播地址和端口 |
| `length` | 数据报字节数 |
| `sha256` | 数据报摘要 |
| `prefix_hex` | 前 24 字节十六进制文本 |
| `wire_hint` | 对已知包封装的提示 |
| `payload` | 原始数据报相对路径 |
| `parse` | 解析结果和分析标记 |

`parse.analysis` 包含 `message_key`、`duplicate`、`out_of_order` 和 `accepted_for_capture`。当前工具对所有收到的数据报写出 `accepted_for_capture: true`。

### events.jsonl

记录连接、监听、停止和录制错误等运行事件。

| 字段 | 含义 |
| --- | --- |
| `kind` | `runtime_event` |
| `observed_at_utc` | UTC 时间 |
| `level` | `INFO`、`WARN` 或 `ERROR` |
| `message` | 事件文本 |

不要依赖 `message` 文本做稳定的协议判断。机器处理应优先使用结构化时间线和 `manifest.json`。

## 原始内容寻址

Redis 值和 UDP 数据报按内容的 SHA-256 保存：

```text
payloads/<redis-or-udp>/<sha256>.bin
```

同类内容重复出现时只保存一份 `.bin`，但每次观察仍保留独立 JSONL 行。读取方应通过行内 `payload` 找到文件，并核对：

1. 文件长度等于行内 `length`。
2. 文件 SHA-256 等于行内 `sha256`。
3. 路径符合 `payloads/<kind>/<sha256>.bin`。

## SHA256SUMS 和验证范围

录制正常停止后，`SHA256SUMS` 列出目录内除自身外的全部文件。每行格式为：

```text
<64位小写SHA-256><两个空格><相对路径>
```

`--verify-capture` 会检查：

- `SHA256SUMS` 的语法和路径边界
- 清单中的文件集合与目录内实际文件集合完全一致
- 每个文件的 SHA-256
- `manifest.json` 为完成状态的 v2 录制
- 四个 JSONL 文件逐行可解析为 JSON 对象且没有空行
- JSONL 实际行数与 `manifest.counts` 一致
- 每条原始内容引用的路径、长度和 SHA-256 一致

验证不检查业务数值是否合理，也不证明数据在进入 Python 之前没有丢失。

## ZIP 交付

`--pack-capture` 仅打包已经通过验证的目录。ZIP 外另有一个 `<archive>.sha256` 文件，用于传输后先核对整个压缩包。ZIP 内保留原录制目录名。

建议接收方按以下顺序处理：

1. 核对 ZIP 与旁置文件中的 SHA-256。
2. 解压到新目录。
3. 对解压出的录制目录运行 `--verify-capture`。
4. 再读取 `manifest.json` 和 JSONL。

## 读取示例

Python 标准库逐行读取 UDP 时间线：

```python
import json
from pathlib import Path

capture = Path("SmartDriverLiveCapture-YYYYMMDD-HHMMSS-mmm")
with (capture / "udp-observations.jsonl").open(encoding="utf-8") as rows:
    for line in rows:
        item = json.loads(line)
        raw = (capture / item["payload"]).read_bytes()
        print(item["observed_at_utc"], item["source_ip"], len(raw))
```

使用 `jq` 查看计数：

```bash
jq '.status, .counts, .environment' SmartDriverLiveCapture-*/manifest.json
```

筛选解析失败的 UDP 行时，应按实际 `parse` 结构选择条件。不要删除这些行，它们仍然是有效的原始观测证据。

## 兼容规则

- 读取方应先检查 `format_version`。
- 读取方应忽略不认识的新字段。
- 不应依赖 JSON 对象的字段顺序。
- 不应把 `monotonic_ns` 与其他进程或其他机器直接比较。
- `status` 不是 `complete` 时，不应把目录当作完整交付物。
- 格式含义发生不兼容变化时，写出端必须提升 `format_version`。

## 隐私

原始二进制内容可能包含位置、车辆标识、轨迹和道路环境信息。格式本身不加密。存储、转移和共享策略由数据所有者负责。
