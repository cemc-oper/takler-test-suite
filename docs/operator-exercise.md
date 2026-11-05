# 跨组件流程与 CLI/TUI 演练

EXE-003 使用真实 takflow `WorkflowEngine(TaklerBackend()).save_suite` 导出 DefinitionDocument v1，由 Takler 加载、begin，经 takflow 的 `orvix_job(engine, "local")` 提交真实本地 Shell。任务由所选 Python/Go child CLI 上报，orvix 默认 sidecar 和独立 `status` 查询用于核对进程。环境必须同时安装 takflow 和 Takler，并设置绝对路径 `SCENARIO_ORVIX_BIN`，文件名为 `orvix`。缺少入口会在服务启动前记录环境阻塞。

普通自动运行：

```bash
python=/absolute/path/to/takler-and-takflow-env/bin/python
suite=/absolute/path/to/takler-test-suite
export SCENARIO_ORVIX_BIN=/absolute/path/to/bin/orvix
"$python" -B "$suite/run.py" --runs-dir '/tmp/workflow runs' \
    run EXE-003 --profile local --client python --transport grpc
```

Go 时另设绝对路径 `SCENARIO_GO_CLIENT` 并传 `--client go`；HTTP 传 `--transport http`。固定版本的 WSL 构建、依赖复用方式、完整矩阵和实跑证据见工作区 [SCN-09 记录](../../../doc/2026-10-10-scenario-testing/records/scn-09/scn-09-wsl-workflow-exercise.md)。工程本身不读取该记录或依赖工作区布局。

每次自动运行均执行两个独立流程：

| 流程 | 预期结果 |
| --- | --- |
| workflow_ok | prepare 写入两组输入，left/right 并行计算 10 和 31；先完成 left 并观察至少 13 秒，merge 仍 queued；right 完成后 merge 输出 `left=10\nright=31\ntotal=41\n`。四个作业均 complete。 |
| workflow_fail | 同样真实准备和并行计算，随后让 right 的 Shell 退出 7，EXIT trap 用真实 child abort 上报；prepare/left 为 complete，right 为 aborted，merge 持续 queued、无 attempt/作业文件/汇总结果。三个实际作业，不使用 force-complete。 |

受控失败是明确预期，只有负向判据及清理通过，统一入口才返回 passed。`components.json`、`business-timeline.json`、`parallel-evidence.json`、各阶段快照、orvix 查询、进程区间、输出及清理记录均保留在本次私有目录。生成的作业和 checkpoint 可能含 child 口令，公开归档应排除这些原件。

## 组合演练入口

`assets/workflow_exercise.py` 复用 EXE-003 自动链路，再复用 QRY-004 的同一四状态 fixture，在同一服务内留下两个业务流程和 `/showcase`。默认自动放行演练任务并关闭服务；运行目录和连接配置始终作为证据保留。**只有 `--keep-service` 才保留活服务供观察**。

```bash
"$python" -B "$suite/assets/workflow_exercise.py" \
    --runs-dir '/tmp/operator exercises' --transport http \
    --keep-service --max-seconds 900
```

先等待两个业务流程完成既定检查及四状态准备，再出现 `READY`、`CONTROL` 和 `TUI_COMMAND`。从 READY 起最多保留 900 秒；超时、Ctrl-C 或 stop 都会有界清理，并以非零结果结束。该入口不用 `--profile`，与普通 run 使用相同组件预检和服务设施。

在另一 WSL 终端执行打印的 TUI_COMMAND，或使用 READY 目录内 `connect.yaml` 的端口运行 CLI。TUI 中选择根节点及各流程，展开节点、选择任务并按 `r` 刷新，核对状态和 info/artifacts；不要按 force-complete 来改变参考结果。

| 观察点 | 独立参考 |
| --- | --- |
| workflow_ok | 四项 complete；merge 的输出为固定三行；每任务一个 attempt，try_no=1。 |
| workflow_fail | prepare/left complete，right aborted、原因含 exit=7，merge queued 且未尝试。 |
| showcase | done complete、failed aborted、running active、blocked queued；running 的 PID 与四状态快照一致。 |
| 人工刷新和详情 | 选择节点后 info 对应本节点，按 r 后可对照最新 CLI snapshot；界面观察与 CLI 自动检查分别登记。 |
| 可选断连观察 | 向 CONTROL 写 pause-listener；等控制文件被消费且 `/proc/<服务PID>/stat` 为 T，再刷新观察 stale；写 resume-listener 后等待恢复并刷新。只暂停本次服务。 |

把打印的 CONTROL 路径设为变量，例如 `control='/tmp/operator exercises/exe-003-实际ID/control'`。完成观察后先退出 TUI，再选择一个清理动作：

```bash
printf 'finish\n' > "$control"  # 完成 remaining showcase 作业，检查输出后正常关闭；成功退出 0
# 或者：
printf 'stop\n' > "$control"    # 中断演练、释放受控作业并关闭；预期退出 130
```

若服务正因 pause-listener 停止，finish/stop/Ctrl-C 均先恢复该服务进程再清理。不要手工删除目录代替清理。退出后核对 `report.json` 的 cleanup、`process-intervals.json` 中全部进程退出，以及监听端口释放。若准备期间失败，服务同样由入口收尾。

`exercise-ready.json` 将 TUI 子检查标为 `pending_observation`。启动 TUI、CLI 自动通过或 finish 成功都不会自动把人工观察改成通过。观察者应单独保存运行 ID、客户端版本、观察项、实际结果与截图；本轮组合界面未完成截图核验，已有 QRY-004 人工记录仍在 SCN-07，不能替代新组合的逐项观察。
