# 容量与多窗口监控

容量用例使用真实 Takler 服务、Python 客户端、gRPC 和 Linux 本地 Shell。先按[环境准备](baseline.md)设置 `TAKLER_PYTHON`，在套件仓库根目录执行以下命令。

```bash
runs_dir="$HOME/.local/state/takler-test-suite/runs/capacity"
"$TAKLER_PYTHON" -Bu run.py --runs-dir "$runs_dir" run CAP-001 --profile local
"$TAKLER_PYTHON" -Bu run.py --runs-dir "$runs_dir" run CAP-002 --profile local
"$TAKLER_PYTHON" -Bu run.py --runs-dir "$runs_dir" run CAP-003 --profile local
```

## CAP-001 至 CAP-003

| 用例 | 默认负载与判定 |
| --- | --- |
| CAP-001 | 逐级加载 1,000、10,000 和 100,000 个叶任务。检查摘要、五组完整字段及宽字段分块的完整性。服务保持暂停，业务作业数为零 |
| CAP-002 | 1,000 个背景任务，4 个本地 Shell 作业并发，两位独立读者持续查询摘要和完整视图。默认负载为 120 秒 |
| CAP-003 | 连续混合负载至少 7,200 秒，包含慢读者取消。负载结束后观察 135 秒，检查内存、文件描述符、线程和资源回收 |

默认参数见 [`assets/capacity.toml`](../../assets/capacity.toml)。用 `SCENARIO_CAPACITY_CONFIG` 指向另一份完整 TOML，可调整负载或运行时间。运行器保存所用配置和 SHA256。修改负载后按实际配置解读结果。

默认 checkpoint 每 20 秒保存，资源每秒采样一次。单次查询 RPC 上限为 60 秒，完整读取的总上限为 300 秒。自有进程 RSS 或证据超过 8 GiB、可用内存低于 2 GiB，或自有进程超过 128 个时，运行器报告失败并收尾。精确阈值以本轮配置为准。

## CAP-004 独立监控窗口

CAP-004 需要 Takler 的 TUI extra 和提供 pidfd API 的 `/usr/bin/python3`。默认使用 34 节点小树、1 个真实 Shell 作业、1 个真实 Textual TUI 和 3 个无界面读者。每个窗口有独立连接和凭据。用例检查首开、同步与错峰刷新、详情、预期的越权拒绝以及从旧水位重连，并在周期保存后验证恢复。

```bash
SCENARIO_MONITOR_CONFIG="$PWD/assets/monitoring.toml" \
    "$TAKLER_PYTHON" -Bu run.py --runs-dir "$HOME/.local/state/takler-test-suite/runs/monitor" \
    run CAP-004 --profile local
```

`SCENARIO_MONITOR_CONFIG` 选择整份 TOML。复制相近配置后可用 `windows` 设置读者数，用 `job_concurrency` 设置实际作业并发。`headless_mode = "thread"`（默认）在运行器的不同线程中执行无界面读者，`"process"` 为每个读者创建独立进程。第一窗始终运行真实 TUI。

| 配置 | 默认场景 |
| --- | --- |
| [`monitoring.toml`](../../assets/monitoring.toml) | 34 节点，4 窗，1 个作业 |
| [`monitoring-small-ten-thread.toml`](../../assets/monitoring-small-ten-thread.toml) | 34 节点，10 窗，9 个无界面读者采用线程 |
| [`monitoring-small-ten-process.toml`](../../assets/monitoring-small-ten-process.toml) | 34 节点，10 窗，各读者使用独立进程 |
| [`monitoring-operations.toml`](../../assets/monitoring-operations.toml) | 模拟四个业务时次，9,959 节点，4 窗，4 个作业 |
| [`monitoring-100k.toml`](../../assets/monitoring-100k.toml) | 100,000 个叶任务、104,728 节点，4 窗，4 个作业 |

启动前至少需要 4 GiB 可用内存和 20 GiB 证据盘余量。运行中检查可用内存、磁盘余量、swap 增量和自有进程 RSS。共享文件系统上的 child CLI 和控制文件等待可按[控制超时](control-timeouts.md)设置。

### 专项测量

- **保存期间首开。** 设置 `checkpoint_cold_open = true` 和 `headless_mode = "process"`。运行器在周期保存临时文件出现后放行新窗口，并在 `checkpoint-cold-open.json` 中记录每窗是否完整落入保存区间及是否在 5 秒内读完摘要。
- **保存期间同步刷新。** 设置 `poll_cycles = 0` 和 `checkpoint_sync_waves = 3`。每个同步波次使用独立的保存周期，记录在 `checkpoint-sync.json`。成功要求每个窗口读到新标记，并且该次增量读取的所有 RPC 都落在观测到的写入区间内。该配置可以与保存期间首开一起运行。
- **慢读与租约过期。** 在业务树配置中使用 `headless_mode = "thread"`、`poll_cycles = 0` 和 `checkpoint_sync_waves = 0`，按需启用 `slow_reader` 和 `scope_rebuild`。慢读观察其他窗口在一窗等待下一页时能否继续推进。范围重建需要根和每个业务时次各有一个读者，默认四个时次至少需要 5 窗。结果见 `slow-reader.json` 和 `scope-rebuild/`。

## 查看容量证据

| 证据 | 用途 |
| --- | --- |
| `*.queries.jsonl`、`*.rpc.jsonl` | 用户读取耗时与各次 RPC 的耗时、状态及回复 JSON 字节数 |
| `windows.jsonl`、`monitor-summary.json` | CAP-004 的逐窗结果、计划时刻、数据就绪时刻、TUI 首屏和波次汇总 |
| `cold-inventory.json`、`events.jsonl` | CAP-004 首开后的节点清点、读者 PID 及连接归属 |
| `child-receipts.jsonl`、`checkpoint-events.jsonl` | 实际上报与周期保存的外部文件事件 |
| `resources.jsonl`、`host-resources.jsonl` | 运行进程和主机的资源样本 |
| `report.json`、`process-intervals.json` | 用例结果、作业退出和服务清理 |

checkpoint 的文件创建和重命名事件给出从外部观测到的写入区间。测量时区分摘要可用、根详情可用和 TUI 首屏。共享主机上的响应时间受同机负载影响，RSS 汇总也可能重复计算共享页。归档前按[排错与清理](troubleshooting.md)检查本轮运行证据。
