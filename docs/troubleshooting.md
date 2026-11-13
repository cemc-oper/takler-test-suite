# 排错与清理

## 从运行报告定位问题

打开 `RUN_DIR` 中的 `report.json`，查看 `result`、`reason` 和清理字段。也可以用 `run.py inspect` 读取同一报告。批量运行应查看 `summary.json` 的 `expected_runs`、`passed_runs` 和各项退出码。[查看运行报告](quickstart.md#查看运行报告)

| `result` | 处理方法 |
| --- | --- |
| `environment_blocked` | 按 `reason` 补齐所选用例需要的组件或系统能力 |
| `failed` | 查看首个失败判据、异常和清理结果 |
| `interrupted` | 检查中断后的进程、监听端口和清理记录 |
| `passed` | 用例判据与清理均通过，可按需复查证据 |

| 文件或字段 | 用途 |
| --- | --- |
| `events.jsonl` | 按运行编号跟踪服务启停、客户端操作及判据 |
| 阶段 JSON、`job-outputs.json` | 核对阶段状态、attempt 和业务输出 |
| `process-intervals.json` | 按 PID、内核启动刻度和观察区间核对作业进程 |
| `server_history`、`cleanup_history` | 对齐恢复用例中的服务重启和清理代次 |
| `server*.log`、`client-error.log`、`error.log` | 定位服务或客户端错误及 Python 异常 |

## 常见问题

| 现象 | 核对与处理 |
| --- | --- |
| 找不到 `takler-server` 或 `takler-client-py` | 将 `TAKLER_PYTHON` 指向实际安装了 Takler 的解释器，确认两个入口位于它的 `bin` 目录 |
| Go、orvix 或查询探针缺失 | 按[环境准备](baseline.md)设置所需的绝对可执行路径 |
| HTTP 连接失败 | 检查 `http` extra 及本次 `connect.yaml` 中记录的 HTTP 端口。HTTP 运行同时使用一个内部 gRPC 端口 |
| 查询过期、定时或恢复场景等待较长 | 查看该阶段是否正在等待真实时钟、租约过期或周期保存。共享盘上的 child CLI 和控制文件等待可按[控制超时](control-timeouts.md)配置 |
| checkpoint 恢复后服务处于 `halted` | 核对 `restore_summary`、外部作业和暂停原因，按用例要求处理后再执行 `server-resume` |
| checkpoint 写入失败后服务拒绝 `server-resume` | 恢复写权限，等待周期保存成功，再核对暂停原因。`server-resume` 即使被 `checkpoint_failure` 拒绝，也会清除 `manual` 和 `recovery` |
| orvix 生成的文件多于作业数 | `.job<UUID>` 是作业脚本，`.submit`、`.submit.log`、`.info.yaml` 是相关产物 |
| TUI 按键或截图无响应 | 检查终端焦点和尺寸。可在启动 TUI 前执行 `stty -ixon`。退出前先关闭命令面板 |
| TUI 恢复后仍显示旧错误 | 查看最新刷新时间及 stale 是否撤除，通知区域可能仍保留之前的错误 |

## 清理与归档

- 自动用例在正常结束和 Ctrl+C 时清理本次启动的服务、受控作业和查询探针。交互入口按[操作演练](operator-exercise.md)结束。
- 中断或失败后，查看 `cleanup_error` 和服务代次，确认本次监听已释放。用 `process-intervals.json` 中的 PID 和内核启动刻度确认原作业身份。
- 运行目录默认保留。确认进程和端口已清理、所需证据已归档后，再删除明确选定的 `RUN_DIR`。
- 原始 checkpoint、`private-*`、`security/`、生成的作业脚本及 `.submit` 文件可能含测试凭据。将这些文件保留在权限为 `0700` 的私有运行目录中。共享报告前，检查选中的文件是否包含本轮生成的 operator、API 或 child 凭据。
