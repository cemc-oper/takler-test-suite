# Takler Test Suite

Takler 的公开行为测试套件，支持自动化场景验证和操作演练，仓库名为 `takler-test-suite`。项目包含统一入口、场景目录、共享运行设施、任务模板和设施回归测试。

[快速开始](docs/quickstart.md) · [基线与安装](docs/baseline.md) · [用例和覆盖索引](docs/coverage.md) · [失败排查与清理](docs/troubleshooting.md) · [人工演练](docs/operator-exercise.md)

当前接入 37 个用例：DEF-001–003、PAR-001–002，BAS-001–003、DPL-001–003，EXE-001–003、DEP-001–003、RES-001–002，CTL-001–003、TIM-001–002，QRY-001–005、SEC-001–004，以及 RCV-001–004。使用 Python 3.11+ 和已安装 Takler 的 Python 环境，在线用例另需 Linux/WSL 的 `ss`，HTTP 运行需要 Takler 的 HTTP extra。解释器所在环境应同时包含 `takler-server` 和 `takler-client-py`。SEC-002 另需 `openssl`，EXE-003 另需同环境中的 takflow 和绝对路径 `SCENARIO_ORVIX_BIN`。

在本工作区，先在 `repo/takler/` 准备开发虚拟环境，再从工作区根目录进入本仓库执行：

```bash
cd repo/takler-test-suite
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

QRY/SEC 的九个用例及 RES-002 已验证 Python/Go × gRPC/HTTP 四种组合。`--client go` 选择操作 CLI；共享 controlled 模板的 child 也使用该 Go 可执行文件。先设置绝对路径 `SCENARIO_GO_CLIENT`；QRY-001–003 和 SEC-003 另需设置 `SCENARIO_GO_PROBE`。后者由 `assets/query_probe.go` 在所选 Go 客户端源码副本中构建，只通过该版本的公开查询 SDK 和传输接口提供 JSON lines 控制。Python 对应入口为 `assets/query_probe.py`。缺少 Go 文件时明确记环境阻塞。旧用例及专用脚本的 Go 组合以实跑记录为准。

```bash
export SCENARIO_GO_CLIENT=/absolute/private-run/bin/takler_client
export SCENARIO_GO_PROBE=/absolute/private-run/bin/query_probe
"$python" -B "$runner" run QRY-003 --profile local --client go --transport http
"$python" -B "$runner" run SEC-002 --profile local --client python --transport grpc
```

QRY-002 属慢用例，实际跨越 120 秒摘要页保留期；保持 scope lease 后核对旧续页过期、同服务重连及替换后的重建。QRY-003 在固定版本读取中改变真实任务状态，核对宽字段、原版本一致性、取消后的续页/分块失败和完整性标志。QRY-005 实际读取同一 WSL 文件系统的输出，不代表远程文件接口。

SEC 用例在每次受控运行目录生成 operator/child 测试凭据、白名单和短期 CA/证书。`prepare(evidence, service)` 在服务启动前配置安全选项；失败时不会启动服务。`private-*.json`、`security/`、生成作业和 checkpoint 不可作为公开证据直接提交。公开响应与证据归档使用真实生成值扫描，发现泄漏即失败。Python HTTP 当前按 URL 主机名校验名称，不支持证书名称覆盖；用例以 IP SAN 的有效地址及 `localhost` 的不匹配地址核对。

QRY-004 自动运行只判定 CLI。实际 TUI 演练使用独立 fixture，打印就绪目录、公开 `takler-tui` 命令和本次控制文件；需要逐屏观察并另记结果，不会因为 fixture 启动就标 TUI 通过。

```bash
"$python" -B assets/tui_fixture.py --runs-dir /absolute/private-tui-runs --transport grpc
```

在另一个 WSL 终端按打印命令启动 TUI，选择/展开节点并按 `r` 刷新。Ctrl+P → Screenshot 使用 Textual 自带 SVG 截图，保存至本次隔离 HOME 的 Downloads（先建目录）。向打印的 CONTROL 文件写入 `pause-listener`/`resume-listener` 可暂停/恢复本次服务进程，用于观察 stale；先等待控制文件被消费并用 `/proc` 确认实际暂停，再按 `r`。结束 TUI 后写入 `finish`，fixture 完成下游并核对进程退出。fixture 使用显式 `setsid` 提交受控 Shell，最长保留 15 分钟，提供 stop 屏障及进程清理。

## 恢复与故障场景

RCV-001–004 已验证 Python/Go × gRPC/HTTP 四种组合。服务设施在同一私有目录内复用原地址重启，以 `server_history`、`cleanup_history` 和分代 `server-*.log` 保留各次启动、正常退出、预期 SIGKILL 或启动拒绝。原先单次启停的默认行为保持兼容。

```bash
"$python" -B "$runner" run RCV-001 --profile local --client python --transport grpc
"$python" -B "$runner" run RCV-004 --profile local --client go --transport http
```

Go 恢复用例只需 `SCENARIO_GO_CLIENT`，不依赖查询 probe。RCV-001 明确使用 setsid 提交受控外部 Shell，核对原进程跨正常重启后使用原 attempt 上报；RCV-003 使用直接 Shell 和真实 SIGKILL，分别保留保存时状态、崩溃前状态及外部 PID/启动刻度，不从日志补状态。RCV-004 需要非 root 用户，在专用 checkpoint 目录制造两次写权限故障，验证主备完整、故障期间上报、自动继续以及独立人工暂停的保留。`server-resume` 即使因 checkpoint_failure 返回拒绝，也会清除 manual/recovery；场景显式核对该副作用，再重新设置人工暂停。

恢复用例使用真实周期，部分运行超过两分钟；正常等待按条件和截止执行。RCV-003 另需 `/usr/bin/python3` 提供 Linux pidfd API，以收尾 SIGKILL 后的 checkpoint worker/resource_tracker；项目解释器仍用于服务和场景。辅助清理脚本以同一用户执行，按原服务进程组/会话、私有 cwd、Python 可执行文件及 PID 启动刻度识别目标，再用 pidfd 绑定精确进程。缺少能力会在启动服务前记环境阻塞。

原始 checkpoint 和 `private-*.check` 可能含在途 child 口令，公开记录只保留经过完整解码验证的状态投影及哈希。RCV-002 的拒绝启动进程退出 1、RCV-003 的崩溃进程退出 -9 均单独记录为预期故障；统一入口只有完成全部判据和清理才退出 0。

## 接入下一项

在 `catalog.toml` 登记用例的分类、形态、角色、能力、profile 和实现状态。可执行入口放到 `cases/<一级>/<二级>/<用例编号小写_名称>/case.py`。在线 `local` 入口定义 `run(evidence, service)`，通过共享设施调用 CLI 和记录证据。离线 `offline` 入口定义 `run(evidence)`，直接使用公开构建 API。

EXE-003 已接入真实 takflow → Takler → orvix local 的成功与受控失败流程，验证 Python/Go × gRPC/HTTP 四种组合。组合演练默认完成检查后关闭服务，显式 `--keep-service` 才保留服务供 CLI/TUI 观察；finish、stop、Ctrl-C 和截止都有清理路径。[跨组件运行与人工演练说明](docs/operator-exercise.md)

```bash
"$python" -B -m pytest -q -s tests
```

## 工作区方案与历史记录

本项目于 2026-11-05 从 `playground/scenarios/` 迁入 `repo/` 并初始化独立 Git 仓库，最初名称为 `takler-scenarios`，随后按最终命名调整为 `takler-test-suite`，原有 Git 历史保留。工作区方案与历次证据继续归档在 `doc/2026-10-10-scenario-testing/`，以下链接适用于配套工作区布局。历史日志、证据包和源码哈希保留原值，其中的旧目录名表示当次实跑位置；当前复跑命令已经更新为新路径。

[任务方案与推进状态](../../doc/2026-10-10-scenario-testing/scenario-testing-tasks.md)

[项目迁移与独立目录复跑记录](../../doc/2026-10-10-scenario-testing/records/project-migration/project-migration.md)

[最终命名与 Git 历史保留记录](../../doc/2026-10-10-scenario-testing/records/project-rename/project-rename.md)

[SCN-02 复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-02/scn-02-wsl-harness.md) · [SCN-03 离线复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-03/scn-03-wsl-offline.md) · [SCN-04 服务与部署复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-04/scn-04-wsl-service-deployment.md) · [SCN-05 执行、依赖与资源复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-05/scn-05-wsl-execution-dependencies-resources.md)

[SCN-06 控制、时间循环与执行异常复跑记录](../../doc/2026-10-10-scenario-testing/records/scn-06/scn-06-wsl-controls-time-exceptions.md)

[SCN-07 查询、安全与客户端矩阵](../../doc/2026-10-10-scenario-testing/records/scn-07/scn-07-wsl-query-security-matrix.md)

[SCN-08 重启恢复与组合故障](../../doc/2026-10-10-scenario-testing/records/scn-08/scn-08-wsl-recovery-faults.md)

[SCN-09 跨组件流程与人工演练整合](../../doc/2026-10-10-scenario-testing/records/scn-09/scn-09-wsl-workflow-exercise.md)

[SCN-11 使用文档、全新目录复现与选定范围交付](../../doc/2026-10-10-scenario-testing/records/scn-11/scn-11-wsl-delivery.md)
