# SmartDriver 桌面录制格式

桌面端 1.5 默认写出 `format_version: 4` 的单文件 `.sdlog`。文件是 SQLite 数据库，包含录制状态、观察、运行事件及按内容去重的原始载荷。正常停止后，一个 `.sdlog` 即可回看和转移；录制期间 SQLite 可能使用临时日志文件。

校验器和打包器仍支持 1.5 以前的 v2/v3 录制目录。它们与 v4 在记录粒度和保存方式上不同，不能当作逐包兼容格式。

## v4：单文件录制

文件名形如 `SmartDriverLiveCapture-时间戳-标识.sdlog`。主要表如下：

| 表 | 内容 |
| --- | --- |
| `manifest` | 一条 JSON 记录，包含版本、状态、配置、环境、计数和错误信息 |
| `records` | 按 `id` 排序的 Redis、UDP、键清单和运行事件记录 |
| `payloads` | 以 SHA-256 为键保存的原始二进制载荷及长度 |

`records.kind` 为 `redis`、`udp`、`inventory` 或 `events`。记录含 `observed_at_utc`、`stream_key`、`metadata_json` 和可选的 `payload_sha256`；实际采样、解析及诊断信息在元数据中。`payload_sha256` 指向 `payloads.sha256`，相同内容只存一份载荷，观察记录仍分别保留。

`manifest` 中 `format_version` 为 `4`，`status` 为 `recording`、`complete` 或 `failed`，`counts` 包含上述四类记录的行数。正常交付的文件须为 `complete` 且 `last_error` 为 null。

### Redis

目标轮询间隔为 1000 ms；请求慢时实际间隔会更长，不补造样本。成功读取的每个键都记录，包括内容未变化的值。未知 schema 的值仍保存原始内容。读命令出错不伪装成键不存在。

同一轮多个键共享 `sequence`。`MGET` 的 `round_trip_ms` 是整条请求耗时；降级为逐键 `GET` 时，每个成功请求及时写入事务，`observed_at_utc` 和 `round_trip_ms` 对应该次请求。后续单键失败不应抹去前面成功读取的记录。

### UDP

每路按来源 IP、端口及话题区分，以 1 Hz 为目标保存最近收到的可接受完整消息。同一采样间隔内的其他消息会合并；静默时不补造新记录，停止时会写出未满一轮的待保存消息。采样、合并、重复和迟到等诊断不能解释为逐包录制。

LC02 仅剥去 LCM 传输封装，保存完整消息载荷；LC03 的分片只有完整重组后才保存消息载荷。上限为单消息 4 MiB、待处理消息与话题及分片共用 16 MiB、最多 64 个分片组、最多 128 条流，分片超时 2 秒。未完成或超限的分片按诊断处理，不作为完整消息。直接小端 `VehicleIntendInfo` 和其他非 LCM 数据保留选中的原始数据报。

每条 UDP 记录的 `interval_counts` 给出该流在采样间隔内的 `received`、`complete`、`duplicate`、`coalesced` 和 `out_of_order`。其中 `received` 仅计入组成完整消息的数据报，重组完成后才计入所有分片；全局接收数则包含未完成和被拒绝的数据报。`complete` 包含迟到消息，`coalesced` 只计入替换待保存消息的可接受消息。LCM 的 `out_of_order` 依据传输序号，不是业务时间上的乱序。

未取得经核实的 schema 时，LCM 载荷不生成业务 `decoded` 或消息级乱序判断。话题等封装信息不证明业务解码成功。

### 提交与中断

记录先进入 SQLite 事务，目标约每 1 秒提交一次，并在正常停止时提交和写出完成状态。异常退出或存储故障可能丢失尚未提交的最后一批数据；负载或 I/O 延迟可能使这段时间超过 1 秒。被中断或收尾失败的文件不能当作完整录制；磁盘无法继续写入时，失败状态也可能无法写出。

## 校验、查看与打包

`--verify-capture 文件.sdlog` 检查 SQLite 完整性、外键、完成状态、错误信息、记录计数，以及原始载荷的引用、长度和 SHA-256。成功输出 `VERIFY_CAPTURE=OK` 和计数；校验不证明业务值合理或现场网络没有丢包。

桌面界面的查看器每页读取 200 条，原始内容预览最多 2048 字节。预览截断不改变文件中的完整已选载荷；查看器只读取内容和状态，不执行完整性校验。

`--pack-capture 文件.sdlog` 先校验，再生成 `.sdlog.zip` 和旁置 `.zip.sha256`；已存在同名输出时不会覆盖。无需压缩时，可直接复制正常停止后的 `.sdlog`。

## 旧 v2/v3 目录

旧格式使用录制目录：`manifest.json`、`SHA256SUMS`、`redis-inventory.jsonl`、`redis-observations.jsonl`、`udp-observations.jsonl`、`events.jsonl` 和 `payloads/redis`、`payloads/udp` 中按 SHA-256 命名的原始二进制文件。

旧目录以数据报为 UDP 记录边界；v3 只识别 LC02/LC03 封装，不重组 LCM 分片。v2 的逐键 `GET` 观察使用整轮读取结束时的共同时间，`round_trip_ms` 为该轮读取阶段总耗时；v3 的逐键 `GET` 使用各次请求完成时间及该次耗时。读取时应先检查 `manifest.format_version`。

对旧目录执行 `--verify-capture` 或 `--pack-capture` 时，校验器要求完成状态且 `last_error` 为 null，并检查清单与实际文件集合、SHA-256、JSONL、计数和载荷引用。子目录中同名的 `SHA256SUMS` 按普通文件处理。只有通过校验的目录才能打包；ZIP 保留原目录名。

## 读取示例

Python 标准库读取 v4 的前 200 条记录：

```python
import json
import sqlite3
from pathlib import Path

path = Path("SmartDriverLiveCapture-时间戳-标识.sdlog").resolve()
connection = sqlite3.connect(path.as_uri() + "?mode=ro", uri=True)
try:
    rows = connection.execute(
        "SELECT id, kind, observed_at_utc, stream_key, metadata_json "
        "FROM records ORDER BY id LIMIT ?", (200,)
    )
    for row_id, kind, observed_at, stream, metadata_json in rows:
        print(row_id, kind, observed_at, stream, json.loads(metadata_json))
finally:
    connection.close()
```

读取方应检查格式版本，忽略不认识的字段，也不应把不同进程或不同机器的单调时钟值直接比较。

## 隐私

原始载荷可能包含位置、车辆标识、轨迹和道路环境信息。格式本身不加密，存储、转移和共享策略由数据所有者负责。
