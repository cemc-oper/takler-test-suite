# CLI/TUI 操作演练

人工演练用于查看真实终端中的节点状态、详情和断线恢复。先按[环境准备](baseline.md)安装 TUI extra 并设置 `TAKLER_PYTHON`。以下命令在套件仓库根目录执行。

## 独立 TUI 演练

QRY-004 的自动用例检查 CLI。使用以下入口可以在同一类四状态流程上操作 TUI。

```bash
"$TAKLER_PYTHON" -B assets/tui_fixture.py --runs-dir "$PWD/runs/tui" --transport grpc
```

入口打印 `READY`、`CONTROL` 和 `TUI_COMMAND`，并在准备完成后保留服务至多 15 分钟。在另一终端执行打印的 `TUI_COMMAND`，检查 `/showcase` 下的四种状态。观察完成后写入 `finish`。可在启动演练的终端按 Ctrl+C 中断并清理。

## 跨组件流程演练

EXE-003 使用 takflow 导出流程定义，Takler 加载并调度流程，orvix `local` 后端提交真实 Shell 作业。请在待测 Python 环境安装 takflow，并将 `SCENARIO_ORVIX_BIN` 设为名为 `orvix` 的绝对可执行路径。

```bash
export SCENARIO_ORVIX_BIN=/absolute/path/to/bin/orvix
"$TAKLER_PYTHON" -B run.py --runs-dir ./runs run EXE-003 --profile local
```

自动用例执行成功和预期失败两条流程。组合演练再加载 `/showcase`，用 `--keep-service` 保留服务以供观察。

```bash
"$TAKLER_PYTHON" -B assets/workflow_exercise.py \
    --runs-dir ./runs/exercises --transport grpc --keep-service --max-seconds 900
```

选择 `--transport http` 时需要 HTTP extra，选择 `--client go` 时需要 `SCENARIO_GO_CLIENT`。`--max-seconds` 从就绪开始计时，范围为 1 至 900，默认 900 秒。超时后入口清理服务并以非零状态退出。省略 `--keep-service` 会在自动检查后关闭服务。

## 观察和结束

在 TUI 中展开流程、选中任务并按 `r` 刷新。查看任务的 info/artifacts，可用同次运行的 `connect.yaml` 通过 CLI 核对。

| 流程 | 预期结果 |
| --- | --- |
| `/showcase` | `done` 为 `complete`，`failed` 为 `aborted`，`running` 为 `active`，`blocked` 为 `queued` |
| `/workflow_ok`（组合演练） | 四个作业均为 `complete`。`merge` 的输出依次为 `left=10`、`right=31`、`total=41` |
| `/workflow_fail`（组合演练） | `prepare` 和 `left` 为 `complete`，`right` 因退出码 7 而 `aborted`，`merge` 仍为 `queued` 且没有 attempt |

在发送控制命令的终端，将 `control` 设为 `CONTROL` 打印的绝对路径。

```bash
control=/absolute/path/from/CONTROL
```

观察断连时先暂停本次演练的服务，等控制文件被消费且服务进入暂停状态后刷新，核对 stale。

```bash
printf 'pause-listener\n' > "$control"
```

恢复服务后再次刷新，核对状态更新。

```bash
printf 'resume-listener\n' > "$control"
```

退出 TUI 后结束演练。

```bash
printf 'finish\n' > "$control"
```

跨组件演练另支持 `stop`，会中断并以 `130` 退出。完成后核对 `report.json` 的清理字段、`process-intervals.json` 中的进程退出记录及监听端口。组合演练的 `exercise-ready.json` 将人工 TUI 检查记录为 `pending_observation`。另行保存本次运行编号、TUI 版本、观察项和实际结果。需要截图时，在 TUI 按 Ctrl+P 选择 Screenshot。
