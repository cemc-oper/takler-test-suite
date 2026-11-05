# Takler Scenarios

Takler 的公开行为场景测试与操作演练项目，仓库名为 `takler-scenarios`。项目包含统一入口、场景目录、共享运行设施、任务模板和设施回归测试。

当前接入 23 个用例：DEF-001–003、PAR-001–002，BAS-001–003、DPL-001–003，EXE-001–002、DEP-001–003、RES-001–002，以及 CTL-001–003、TIM-001–002。使用 Python 3.11+ 和已安装 Takler 的 Python 环境，在线用例另需 Linux/WSL 的 `ss`，HTTP 运行需要 Takler 的 HTTP extra。解释器所在环境应同时包含 `takler-server` 和 `takler-client-py`。

在本工作区，先在 `repo/takler/` 准备开发虚拟环境，再从工作区根目录进入本仓库执行：

```bash
cd repo/takler-scenarios
runner="$PWD/run.py"
python="$(cd ../takler/.venv/bin && pwd)/python"
python3 -B "$runner" validate
python3 -B "$runner" list
"$python" -B "$runner" run DEF-001 --profile offline
run_output=$("$python" -B "$runner" run EXE-001 --profile local --client python --transport grpc)
printf '%s\n' "$run_output"
run_id=$(printf '%s\n' "$run_output" | sed -n 's/^RUN_ID=//p')
"$python" -B "$runner" inspect "$run_id"
"$python" -B "$runner" run BAS-003 --profile local --transport http
"$python" -B "$runner" run RES-002 --profile local --transport grpc
"$python" -B "$runner" run CTL-002 --profile local --transport http
```

在其他目录布局中，将 `python` 设为所选 Takler 环境中解释器的绝对路径即可。运行代码和场景资产按本仓库位置定位，不读取工作区任务文档。`validate` 和 `list` 只读取目录，使用标准库。默认输出根目录是本仓库下的 `runs/`，可在子命令前指定 `--runs-dir <目录>`。使用自定义根目录时，`inspect` 也指定同一路径。`local` 默认使用 Python CLI、gRPC 和直接 Shell，`--transport http` 选择 HTTP。服务使用动态端口、独立 HOME 和 `connect.yaml`，客户端显式连接 `127.0.0.1`。gRPC 监听按 Takler 当前默认行为，请在隔离的本机环境运行。

每次运行打印 `RUN_ID` 和 `RUN_DIR`。`report.json` 保存结果和清理状态，`events.jsonl` 以同一 run ID 记录启停和客户端命令类别，阶段快照提供可核对的任务状态。原始服务日志、生成的作业脚本和 checkpoint 留在权限为 `0700` 的运行目录。生成的作业脚本和在途 checkpoint 可能含测试 child 口令。

运行通过退出 0，失败或环境缺失退出 1，目录、用例或组合不合法退出 2，Ctrl-C 退出 130。清理先向本次启动的服务发送 SIGTERM，有界等待后才对同一进程升级为 SIGKILL。当前案例的 child 调用各有 6 秒外部时限，正常停机由 Takler 清理其直接 Shell 作业。

DEP/RES 用例通过 attempt 专属文件屏障控制真实作业，条件未满足的阶段连续查询至少 13 秒，核对 queued、无 attempt 和无作业脚本。`process-intervals.json` 用 PID、内核启动刻度与 CLOCK_BOOTTIME 保存实际存活区间；资源用例同时核对服务端令牌和持有者。异常时先释放本轮 stop 屏障、等待自有作业退出，再清理服务。RES-002 会先确认旧外部进程退出，再执行 requeue。

CTL 用例复用同一屏障与进程设施，核对节点暂停、服务 halted、重排身份及混合批量 force 的每项结果。TIM-001 使用真实未来分钟与暂停/依赖组合，属慢用例；TIM-002 只验证当前 RepeatDate 的三轮实际执行。EXE-002 另需设置绝对路径 `SCENARIO_ORVIX_BIN`，使用真实 orvix local 和受控提交回执故障；[SCN-06 复跑脚本](../../doc/2026-10-10-scenario-testing/records/scn-06/verify-wsl.sh) 从只读参考仓的副本构建该二进制。

## 接入下一项

在 `catalog.toml` 登记用例的分类、形态、角色、能力、profile 和实现状态。可执行入口放到 `cases/<一级>/<二级>/<用例编号小写_名称>/case.py`。在线 `local` 入口定义 `run(evidence, service)`，通过共享设施调用 CLI 和记录证据。离线 `offline` 入口定义 `run(evidence)`，直接使用公开构建 API。Go CLI、TLS 和人工模式随对应场景接入，EXE-003 跨组件业务链路的统一接入按 SCN-09 推进。

```bash
"$python" -B -m pytest -q -s tests
```

## 工作区方案与历史记录

本项目于 2026-11-05 从 `playground/scenarios/` 迁入 `repo/takler-scenarios/`，并初始化独立 Git 仓库。工作区方案与历次证据继续归档在 `doc/2026-10-10-scenario-testing/`，以下链接适用于配套工作区布局。历史日志、证据包和源码哈希保留原值，其中的旧目录名表示当次实跑位置；当前复跑命令已经更新为新路径。

[任务方案与推进状态](../../doc/2026-10-10-scenario-testing/scenario-testing-tasks.md)

[项目迁移与独立目录复跑记录](../../doc/2026-10-10-scenario-testing/records/project-migration/project-migration.md)

[SCN-02 复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-02/scn-02-wsl-harness.md) · [SCN-03 离线复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) · [SCN-04 服务与部署复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) · [SCN-05 执行、依赖与资源复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md)

[SCN-06 控制、时间循环与执行异常复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md)
