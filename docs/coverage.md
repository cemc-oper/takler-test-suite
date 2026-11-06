# 用例、交付选择与证据索引

当前 catalog 共 41 个可执行用例。SCN-11 固定选择包括 37 项基线运行和 9 个 Go/HTTP 代表组合，共 46 次自动运行。offline 与 online 分开验收。CAP-001–003 的容量基线与 CAP-004 的独立监控窗口场景各有专用入口和记录。[SCN-11 选择清单](../selections/wsl-delivery.toml)

| 用例 | 名称与实现 | 本轮选择 | 历史证据 |
| --- | --- | --- | --- |
| BAS-001 | [服务配置、启动与停止](../cases/service/lifecycle/bas_001_config_lifecycle/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| BAS-002 | [传输选择与连接失败](../cases/service/transports/bas_002_connections/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| BAS-003 | [客户端配置与环境隔离](../cases/service/client_config/bas_003_precedence/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| CTL-001 | [节点与服务暂停](../cases/operations/controls/ctl_001_pause/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |
| CTL-002 | [重排与旧实例隔离](../cases/operations/controls/ctl_002_requeue/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |
| CTL-003 | [force 与批量控制](../cases/operations/controls/ctl_003_force_batch/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |
| DEF-001 | [离线流程树构建](../cases/definition/structure/def_001_flow_tree/case.py) | offline | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) |
| DEF-002 | [离线非法定义检查](../cases/definition/structure/def_002_invalid_definition/case.py) | offline | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) |
| DEF-003 | [定义导出与离线重建](../cases/definition/serialization/def_003_round_trip/case.py) | offline | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) |
| DEP-001 | [顺序依赖与汇合](../cases/scheduling/dependencies/dep_001_fork_join/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| DEP-002 | [事件放行](../cases/scheduling/dependencies/dep_002_event_gate/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| DEP-003 | [进度阈值与组合条件](../cases/scheduling/dependencies/dep_003_event_meter/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| DPL-001 | [流程加载与部署核对](../cases/execution/deployment/dpl_001_load/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| DPL-002 | [非法与重复部署](../cases/execution/deployment/dpl_002_invalid_load/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| DPL-003 | [流程更新与替换保护](../cases/execution/deployment/dpl_003_replace/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) |
| EXE-001 | [单任务真实执行](../cases/execution/submission/exe_001_local_shell/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| EXE-002 | [失败与提交结果未知](../cases/execution/submission/exe_002_uncertain/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |
| EXE-003 | [takflow、Takler 与 orvix 业务链路](../cases/execution/submission/exe_003_workflow/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-09/scn-09-wsl-workflow-exercise.md) |
| PAR-001 | [参数继承与覆盖](../cases/definition/parameters/par_001_inheritance/case.py) | offline | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) |
| PAR-002 | [模板、include 与脚本准备](../cases/definition/templates/par_002_render/case.py) | offline | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) |
| QRY-001 | [范围摘要与实时详情](../cases/operations/queries/qry_001_scopes/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| QRY-002 | [增量、长会话与重连](../cases/operations/queries/qry_002_sessions/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| QRY-003 | [同版本完整详细读取](../cases/operations/queries/qry_003_full_snapshot/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| QRY-004 | [CLI 展示与独立 TUI 观察](../cases/operations/presentation/qry_004_cli/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| QRY-005 | [实例输出定位与本地查看](../cases/operations/presentation/qry_005_outputs/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| RCV-001 | [正常停机与同地址恢复](../cases/recovery/restoring/rcv_001_restart/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-08/scn-08-wsl-recovery-faults.md) |
| RCV-002 | [主备损坏与启动拒绝](../cases/recovery/restoring/rcv_002_backup/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-08/scn-08-wsl-recovery-faults.md) |
| RCV-003 | [SIGKILL 丢失窗口与外部进程](../cases/recovery/crash_windows/rcv_003_sigkill/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-08/scn-08-wsl-recovery-faults.md) |
| RCV-004 | [周期写失败与组合暂停](../cases/recovery/saving/rcv_004_write_failure/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-08/scn-08-wsl-recovery-faults.md) |
| RES-001 | [单资源并发限制](../cases/scheduling/resources/res_001_shared_limit/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| RES-002 | [跨流程资源竞争](../cases/scheduling/resources/res_002_cross_flow/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md) |
| SEC-001 | [操作凭据与授权](../cases/security/operators/sec_001_operator/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| SEC-002 | [TLS 与证书验证](../cases/security/protection/sec_002_tls/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| SEC-003 | [敏感信息与非法访问](../cases/security/protection/sec_003_redaction/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| SEC-004 | [child 身份与实例凭据](../cases/security/children/sec_004_child_identity/case.py) | Python/gRPC；Go/HTTP | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md) |
| TIM-001 | [当前时间条件](../cases/scheduling/time_repeat/tim_001_wall_time/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |
| TIM-002 | [当前循环与轮次身份](../cases/scheduling/time_repeat/tim_002_repeat_date/case.py) | Python/gRPC | [记录](../../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md) |

## SCN-10 容量补充

| 用例 | 名称与实现 | 第一轮组合 | 验证状态和证据 |
| --- | --- | --- | --- |
| CAP-001 | [大树与宽字段读取](../cases/capacity/large_trees/cap_001_queries/case.py) | Python/gRPC；真实服务，0 个业务作业 | 通过：1k/10k/100k 叶任务、五组完整字段、18 次读取 |
| CAP-002 | [活跃混合负载](../cases/capacity/mixed_load/cap_002_mixed/case.py) | Python/gRPC；本地真实 Shell | 通过：120 秒、4 并发、16 个真实作业、双读者 |
| CAP-003 | [耐久与资源回收](../cases/capacity/endurance/cap_003_endurance/case.py) | Python/gRPC；本地真实 Shell | 通过：7,200 秒、240 轮、960 个真实作业、135 秒回收和冻结趋势门槛；保留快照上限补充通过 |

负载、资源门槛和测量口径见 [容量运行说明](capacity.md)。三项采用共享开发主机，未隔离其他任务，不作独占性能基准；内部 worker 队列精确上限未测。独立验收、失败批次、源码哈希、复跑命令和公开证据见 [SCN-10 记录](../../../doc/2026-10-10-scenario-testing/records/scn-10/scn-10-wsl-capacity.md)。

## 独立监控窗口

| 用例 | 默认组合 | 验证范围 |
| --- | --- | --- |
| [CAP-004 独立监控窗口](../cases/capacity/mixed_load/cap_004_monitors/case.py) | Python/gRPC，34 节点，1 个真实 Textual TUI 和 3 个无界面读者 | 四套独立身份的查询、变更可见性、拒绝归属、有效水位重连，期间有真实 Shell 作业和周期保存 |

2026-11-06 小树验收通过，运行编号 `cap-004-sncxuw2n`，20/20 正常读操作首试成功，受控越权拒绝单列。保存两次，作业和服务正常回收。[小树实测及此前的收尾失败](../../../doc/2026-11-06-multi-window-monitoring/t5-small-tree.md)

同一小树的十窗配置支持线程组和独立进程组。两组各 50/50 次正常读取首试成功，10/10 沿原水位重连。独立进程组逐窗核对了 10 个 PID 和各自的新连接。另在错峰刷新前让第 10 窗退出，其余 9 窗首次成功，所有读者和服务均已回收。[T11 十窗驱动验收与复跑](../../../doc/2026-11-06-multi-window-monitoring/t11-small-tree-process-windows.md)

CAP-004 默认运行用于验收工具。9,959 节点业务替身在 4、10、20 窗分别完成 60/60、150/150、300/300 次正常读取。20 窗另验证了 3 次保存早段的同步刷新、1 人慢读时其余 19 人继续完成读取，以及根和四个业务子树上 20 人的真实租约过期与重建。窗口数来自测试配置。100K 叶任务余量树有 104,728 个节点。四窗 4/4 在 5 秒内取得完整摘要。十窗线程组两轮均为 1/10，独立进程组两轮均为 10/10。二十窗独立进程组在一次正式波次中 20/20 在 5 秒内取得完整摘要，p95 为 4.292 秒，最慢为 4.644 秒，实际 TUI 首屏为 5.105 秒。全程正常读取 300/300 首试成功。二十窗冷开及同步稀疏刷新均缺少完整落入保存文件写入区间的样本。定向保存复核和 HPC 业务树复核另行安排。[100K 二十窗进程实测](../../../doc/2026-11-06-multi-window-monitoring/t14-100k-twenty-process-windows.md) · [100K 十窗进程对照](../../../doc/2026-11-06-multi-window-monitoring/t12-100k-ten-window-process-comparison.md) · [100K 十窗实测](../../../doc/2026-11-06-multi-window-monitoring/t10-100k-ten-windows.md) · [100K 四窗实测](../../../doc/2026-11-06-multi-window-monitoring/t9-100k-four-windows.md) · [4 窗实测](../../../doc/2026-11-06-multi-window-monitoring/t6-four-windows.md) · [10 窗实测](../../../doc/2026-11-06-multi-window-monitoring/t7-ten-windows.md) · [20 窗实测](../../../doc/2026-11-06-multi-window-monitoring/t8-twenty-windows.md) · [运行说明](capacity.md#cap-004-独立监控窗口)

## SCN-11 使用范围

- SCN-11 已交付：DEF/PAR、BAS/DPL、EXE、DEP/RES、CTL/TIM、QRY、SEC、RCV 共 37 项；当轮运行结果逐项见 [SCN-11 交付记录](../../../doc/2026-10-10-scenario-testing/records/scn-11/scn-11-wsl-delivery.md)。另新增的 CAP 3 项按上表和 SCN-10 单独验收。
- 自动集合中，所有在线用例复现 Python/gRPC；Go/HTTP 只复现表中 9 项。其他组合若有历史实跑，以上链接保持当轮范围，不推定全体 37 项都验证四组合。
- 人工必测：本次新建的 EXE-003 + QRY-004 组合，使用 Python TUI/gRPC、真实 PTY 键鼠、原生 Screenshot SVG/PNG，核对选择/展开、成功与失败、queued 未尝试、active 身份、stale 和恢复，退出及清理另有报告；由代理操作终端并查看截图，不表示独立真人签字。
- 历史人工：SCN-07 的 QRY-004 gRPC/HTTP 交互记录继续可追溯。SCN-09 的“未取得组合截图”保持原始记录，本轮 gRPC 新证据单独归档；新组合 HTTP 仍未核验，Go TUI 不适用。

## 未实现、未覆盖和失败的处理

| 分类 | 具体范围 | 本轮处理 |
| --- | --- | --- |
| 容量组合边界 | CAP-001–003 的 Go、HTTP 与生产 HPC | 首轮只验证 WSL/Python/gRPC，不作跨环境性能/容量承诺 |
| 未覆盖环境 | Windows 原生执行、远程/HPC 调度器、远程输出读取 | 仅 WSL 本机 Shell/orvix local 与本地文件系统，不外推 |
| 未覆盖组合 | 表外 Go/HTTP/Python HTTP/Go gRPC 的本轮重跑、新组合 HTTP TUI | 引用历史边界，标为本轮未复现或未核验 |
| 安装边界 | 空缓存机器的完整离线安装 | 本轮构建新 wheel/venv，依赖来自已准备基线，记录此限制 |
| 不适用 | offline 的服务/客户端/传输；Go 的 TUI | 不通过伪造入口补齐 |
| 环境阻塞/失败 | 选定必测项缺入口、断言错误、清理不完整 | 记录失败或阻塞，批量非零；保留原报告，修正后使用新目录重跑 |

初次复现暴露 EXE-002 含空格解释器 shebang 和 BAS-003 Python/Go 缺失配置诊断差异，修正范围限于场景设施。失败/中断批次保留，最终验收只采用修正后新目录中的完整批次，不能用挑出成功日志替代整批通过。
