# 快速开始

先按 [基线与环境](baseline.md) 准备 Takler 环境。Python 3.11+ 可运行目录入口；在线用例需要同一解释器环境内的 `takler-server`、`takler-client-py` 及 Linux/WSL 的 `ss`。本轮实测版本与范围见 [覆盖索引](coverage.md)。

在任意目录使用绝对路径：

```bash
suite=/absolute/path/to/takler-test-suite
python=/absolute/path/to/takler-env/bin/python
runner="$suite/run.py"
python3 -B "$runner" validate
python3 -B "$runner" list
"$python" -B "$runner" --runs-dir '/tmp/my scenario runs' run DEF-001 --profile offline
"$python" -B "$runner" --runs-dir '/tmp/my scenario runs' \
    run EXE-001 --profile local --client python --transport grpc
```

`validate/list` 只需标准库。离线用例调用公开定义 API，不启动服务、不提交作业，也不接受 client/transport。在线用例每次创建私有目录、动态端口、独立 HOME 和连接文件，完成后清理自有服务与作业。`local` 表示本地隔离服务；EXE-002/003 明确使用 orvix local，其他执行方式以用例记录为准。

运行打印 RUN_ID、RUN_DIR、RESULT。用相同输出根目录查看结果：

```bash
"$python" -B "$runner" --runs-dir '/tmp/my scenario runs' inspect exe-001-替换为实际ID
```

运行通过退出 0；失败或环境阻塞退出 1；参数/组合非法退出 2；Ctrl-C 退出 130。预期的业务失败、SIGKILL 或损坏启动拒绝，只有全部负向判据和清理通过后，用例本身才退出 0。先看 report.json 的 result/reason，再查 events、阶段快照和私有诊断。

## 复现选定自动集合

本次 [selection](../selections/wsl-delivery.toml) 包含 5 个 offline、32 个 Python/gRPC 在线运行和 9 个 Go/HTTP 代表组合，共 46 次。先安装 takflow 并构建真实 orvix、Go CLI 和查询适配器；文件位置按本轮输出替换：

```bash
export SCENARIO_ORVIX_BIN=/absolute/path/to/bin/orvix
export SCENARIO_GO_CLIENT=/absolute/path/to/bin/takler_client
export SCENARIO_GO_PROBE=/absolute/path/to/bin/query_probe
"$python" -B "$suite/reproduce.py" \
    --runs-dir '/tmp/new delivery runs' --workers 4
```

目标目录必须不存在，以保留历史结果。默认只有 1 个 worker，最多 4 个；每项仍由统一 run.py 独立执行。批量入口保存输入 selection、展开 plan、各项命令/返回码和 summary；任一失败、环境阻塞或未完成都会非零退出，不自动重试，不改写旧结果。Ctrl-C 向本次子入口发送 SIGINT，让它们释放屏障并停掉自有服务。

这是自动集合的结果，不代表 TUI 人工验收。人工操作按 [组合演练](operator-exercise.md) 另行启动、观察和留图。本轮完整交付还核对了组合 TUI 的 gRPC 选择、展开、四类状态、断连和恢复，证据见 [交付记录](../../../doc/2026-10-10-scenario-testing/records/scn-11/scn-11-wsl-delivery.md)。新机器必须为自己的人工运行保存观察记录，不能复用旧通过标志。

QRY-002 实际跨越 120 秒页保留期；TIM 和周期保存用例按真实时钟等待。运行集合需数分钟，不能用短命令时限中断后仍当作通过。需要定位失败或清理输出时，见 [排错与证据](troubleshooting.md)。
