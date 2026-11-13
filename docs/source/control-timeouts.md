# 控制超时

共享文件系统上的客户端启动和控制文件确认可能需要更长时间。以下变量在创建 `LocalService` 时读取。

| 环境变量 | 默认秒数 | 作用范围 |
| --- | ---: | --- |
| `SCENARIO_CHILD_TIMEOUT_SECONDS` | 6 | 共享 `controlled.takler` 模板中单次 child CLI 调用的外部时限 |
| `SCENARIO_JOB_TIMEOUT_SECONDS` | 10 | 共享 `Jobs` 设施的属性确认及作业进程退出等待 |
| `SCENARIO_CLIENT_TIMEOUT_SECONDS` | 8 | 普通客户端子进程，包括服务就绪检查和收尾状态查询 |

每项都接受正整数秒。省略某项时使用该项默认值。无效值会在启动服务前记为 `environment_blocked`。有效值在 `report.json` 的 `control_timeouts_seconds` 下分别记录为 `child`、`job`、`client`。

查询 RPC 预算、checkpoint 周期、服务停止时限和各用例的阶段等待由对应配置决定。使用专用作业模板的用例遵循该模板的时限。

## 默认值与 HPC 配置

在套件仓库根目录执行以下命令。第一条清除继承的覆盖值，第二条演示在 HPC 共享盘上把三项都设为 30 秒。两条命令均使用默认的 CAP-004 小树和新的运行目录。

```bash
env -u SCENARIO_CHILD_TIMEOUT_SECONDS \
    -u SCENARIO_JOB_TIMEOUT_SECONDS \
    -u SCENARIO_CLIENT_TIMEOUT_SECONDS \
    "$TAKLER_PYTHON" -Bu run.py --runs-dir ./runs/monitor \
    run CAP-004 --profile local

SCENARIO_CHILD_TIMEOUT_SECONDS=30 \
SCENARIO_JOB_TIMEOUT_SECONDS=30 \
SCENARIO_CLIENT_TIMEOUT_SECONDS=30 \
    "$TAKLER_PYTHON" -Bu run.py --runs-dir ./runs/monitor \
    run CAP-004 --profile local
```

开始前按[容量与多窗口监控](capacity.md#cap-004-独立监控窗口)准备环境。运行后检查 `report.json` 中的有效值、判据和清理状态。
