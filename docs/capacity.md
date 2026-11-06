# 容量、混合负载与耐久测量

CAP-001–003 使用统一 run.py、独立服务和私有目录，第一轮固定为 **Python/gRPC、WSL 本地真实 Shell**。不选择 Go/HTTP 或真实 HPC，不将该机器的观察当作生产容量保证。原 SCN-11 选择文件仍为 37 用例/46 次自动运行，不自动执行新的长时用例。

使用已准备的 Takler 环境运行：

```bash
suite=/absolute/path/to/takler-test-suite
python=/absolute/path/to/takler-env/bin/python
"$python" -Bu "$suite/run.py" --runs-dir '/tmp/capacity runs' run CAP-001 --profile local
"$python" -Bu "$suite/run.py" --runs-dir '/tmp/capacity runs' run CAP-002 --profile local
"$python" -Bu "$suite/run.py" --runs-dir '/tmp/capacity runs' run CAP-003 --profile local
```

默认配置为 [assets/capacity.toml](../assets/capacity.toml)。`SCENARIO_CAPACITY_CONFIG` 可显式指定另一个 TOML；每次在启动服务前记录实际配置、原件 SHA256、冻结时间和环境。修改时长或负载的 smoke 运行仅证明测量设施，不计作默认 2 小时基线通过。不要在运行中改配置或门槛。

## 第一轮冻结条件

| 项目 | 配置与判据 |
| --- | --- |
| 硬件/存储 | 记录 WSL 内核、CPU affinity、内存、磁盘和文件系统；使用本次独立目录，机器为共享开发主机 |
| 资源采样 | 1 秒，观察 runner 及其存活后代，保留 PID/启动刻度、RSS、FD、线程和 CPU ticks |
| 停止条件 | 自有进程 RSS 合计超过 8 GiB、证据超过 8 GiB、可用内存低于 2 GiB 或自有进程超过 128 时失败收尾 |
| checkpoint | 始终开启，周期 20 秒；不得为了读取达标而关闭 |
| 请求/收尾 | 单次查询 RPC 60 秒，加载与正常服务收尾各 180 秒，均不自动重试 |
| 查询总等待 | 摘要每次不超过 60 秒，完整读取每次不超过 300 秒；保留失败和完整等待，不只统计成功 RPC |
| 耐久趋势 | 预热 600 秒后，首/尾各 10 分钟窗口的 RSS 中位数增长率不超过 64 MiB/小时，FD/线程中位数增长各不超过 32 |

这些是本机首轮的验收/停止条件，尚不构成目标业务的性能 SLO。RSS 是采样的进程 RSS 合计，可能重复计算共享页；短进程的 CPU/峰值可能落在采样间隔内，不能解释为精确物理内存峰值或全部 CPU 使用量。

## CAP-001 大树和宽字段

按 1k、10k、100k **叶任务数**逐级构建，另外计入根、Flow 和 Container。100k 配置实际为 101,051 节点。为使用普通 gRPC 加载入口，每个 Flow 最多 2,000 个任务、每个 Container 最多 100 个；不会发送一个超过传输包络的巨大定义，也不修改产品传输限制。

每任务一个 256 字节参数，每棵树最后一个任务另含 90,000 字节字段。普通参数、宽字段和节点数分别记录。服务显式保持 manual halted，每棵树的任务为 queued/无 attempt；实际业务作业数量必须为 0。

每规模 3 次摘要、3 次完整读取。完整读取始终请求 summary、definition、parameters、runtime、artifacts 五组；使用公开 QueryClient 完成页/分块和 SHA 校验，再独立核对路径、类型计数、层级、参数和宽字段。每阶段等待两次加载后的周期发布，避免将尚在途的旧捕获误作整棵新树的保存。

## CAP-002 混合负载

固定 1,000 个未开始的背景任务、4 个真实 Shell 作业并发、30 秒一次作业轮次。作业实际计算 0–999 的和，生成含轮次和 attempt 的独立输出，每任务以真实 child 上报 init、2 次 meter 和 complete；没有合成 child 替代真实任务。

两个独立查询客户端每 2 秒各发起一次读取，一个读摘要、一个完整读取；每 30 次读取关闭并重新建立连接。已完成的工作 Flow 使用公开 replace 创建下一代，验证新 attempt、try=1、业务结果、meter、占用和最终释放，树规模保持固定。CAP-002 默认运行 120 秒。

## CAP-003 耐久与回收

复用相同真实混合负载，持续至少 7,200 秒。每 5 轮引入一次慢消费者：获取首条后实际等待 2 秒，再关闭迭代器取消读取，不产生完成标记。工作期间要求至少每 60 秒完成一轮，周期保存持续推进，两个查询客户端没有遗漏节点或失败。

负载结束后停止读者、确认所有实际作业退出，再观察 135 秒回收窗口，超过 120 秒查询租约；随后触发一次新的公开读取并记录资源。报告包含全程峰值、稳定窗口趋势、FD/线程变化和回收阶段，不能用 smoke 的通过替代该 2 小时运行。

## 测量和证据

`*.rpc.jsonl` 记录原始 QueryDocument 请求类型、回复 JSON 字节、RPC 等待、错误和零重试；字节不包含 protobuf/HTTP2 framing。`*.queries.jsonl` 记录整个用户读取，包括能力发现、续页/分块、解码校验与取消收尾。完整读取使用产品公开客户端及可用的原生摘要接收优化，不关闭产品限制或简化字段。

`resources.jsonl` 保留外部进程采样；`checkpoint-events.jsonl` 用 inotify 记录临时文件创建至主文件重命名的观察包络。该包络含事件读取间隔的不确定性，只表示可能与 checkpoint 文件 I/O 重叠，不当作精确内部捕获/序列化耗时。全程和可能重叠窗口分别统计；没有重叠样本时明确报 0。

`rounds.jsonl`、阶段快照、进程区间、输出和最终 checks 证明真实推进。原始 checkpoint、生成作业含 child 口令，保留在私有目录，不直接公开。出错时先释放本轮屏障并关闭读者，再停掉自有服务。未完成窗口、环境阻塞、判据错误或清理失败均非零。

2026-11-06 的首轮 WSL/Python/gRPC 正式三项均已通过：CAP-003 完成 7,200 秒、240 轮、960 个真实作业和 135 秒回收观察。实际复跑、冻结配置、原始采样、独立验收、资源角色拆分和失败批次见工作区 [SCN-10 记录](../../../doc/2026-10-10-scenario-testing/records/scn-10/scn-10-wsl-capacity.md)。该机器共享其他会话，未控制其他 CPU/内存/磁盘负载；端到端耗时按当次共享主机观察解释，不作独占性能比较。
