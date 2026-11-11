# 测试驱动控制超时

共享文件系统上的客户端启动、控制文件确认可能偶发停顿。测试驱动提供三项独立的环境变量，在创建本次 `LocalService` 时读取；未设置时保持原默认值。

| 环境变量 | 默认秒数 | 作用范围 |
| --- | ---: | --- |
| `SCENARIO_CHILD_TIMEOUT_SECONDS` | 6 | `Jobs.flow()` 生成的共享 `controlled.takler` 作业中，每次 child CLI 的外部 `timeout` |
| `SCENARIO_JOB_TIMEOUT_SECONDS` | 10 | `Jobs.action()` 的属性确认和进程退出、上下文收尾的自有作业退出、`Jobs.finish()` 的最终退出等待 |
| `SCENARIO_CLIENT_TIMEOUT_SECONDS` | 8 | `LocalService.client()` 启动的普通客户端子进程，包括就绪检查和收尾状态查询 |

每项必须是正整数秒。空值、零、负数、小数和非数字值会在启动服务前以 `environment_blocked` 记录，不会启动该用例。仅设置一项时，其余两项保留默认值。`validate` 和 `list` 只检查场景目录，不创建服务，也不校验这些运行期环境变量。

有效值写入本次 `report.json` 的 `control_timeouts_seconds`，键为 `child`、`job`、`client`。同一服务的 `Jobs` 使用这份已冻结的配置，child 时限通过 Flow 参数写入作业脚本；无需将宿主环境变量透传给服务或作业。多服务案例应在整次运行期间保持环境变量不变。

这些值只控制测试驱动等待，不调整查询 RPC 预算、性能验收线、checkpoint 周期、服务停止时限或首轮 init 的启动时序。单独使用其他作业模板的案例仍遵循各自模板。`Jobs.phase()` 的状态等待也保持原配置。首轮 init 延迟诊断使用的 `sleep 30` 不属于此配置。

## 默认值与 HPC 配置

使用已安装 Takler/TUI 的 Python 环境；解释器旁须有 `takler-server`、`takler-client-py`，Linux/WSL 须有 `ss`。以下两次 CAP-004 均为 34 节点、4 窗、1 个真实作业的小树回归，各自使用新服务和私有运行目录。它们验证配置接入，不替代业务树或 100K 性能复核。

默认配置显式清除可能继承的覆盖值：

```bash
suite=/absolute/path/to/takler-test-suite
python=/absolute/path/to/takler-env/bin/python
env -u SCENARIO_CHILD_TIMEOUT_SECONDS \
    -u SCENARIO_JOB_TIMEOUT_SECONDS \
    -u SCENARIO_CLIENT_TIMEOUT_SECONDS \
    SCENARIO_MONITOR_CONFIG="$suite/assets/monitoring.toml" \
    "$python" -Bu "$suite/run.py" --runs-dir "$suite/runs" \
    run CAP-004 --profile local --client python --transport grpc
```

HPC 显式设为 30 秒；先用相同小树确认环境，再选择已冻结的完整业务或 100K TOML：

```bash
SCENARIO_CHILD_TIMEOUT_SECONDS=30 \
SCENARIO_JOB_TIMEOUT_SECONDS=30 \
SCENARIO_CLIENT_TIMEOUT_SECONDS=30 \
SCENARIO_MONITOR_CONFIG="$suite/assets/monitoring.toml" \
    "$python" -Bu "$suite/run.py" --runs-dir "$suite/runs" \
    run CAP-004 --profile local --client python --transport grpc
```

逐次核对 `report.json` 的有效超时、场景结果和清理状态，以及作业回执、周期文件恢复和逐窗账本。原始脚本和 checkpoint 保留在私有运行目录，不作为公开证据提交。CAP-004 的环境、资源停止线和测量口径见[容量运行说明](capacity.md#cap-004-独立监控窗口)。

## 设施回归

在测试环境就绪后，从套件根目录执行：

```bash
"$python" -B run.py validate
"$python" -B -m pytest -q -s \
    tests/test_control_timeouts.py tests/test_jobs.py tests/test_monitor_workload.py
```

新增回归覆盖默认值、逐项覆盖、全部 30 秒、运行期配置冻结、脚本渲染、客户端时限、四条作业等待路径、报告落盘及非法配置的启动前拒绝。
