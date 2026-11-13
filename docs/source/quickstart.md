# 运行场景

先按[环境准备](baseline.md)设置 `TAKLER_PYTHON`。本页命令从套件仓库根目录执行。也可以从其他目录调用 `run.py`，将脚本路径和输出根目录写成绝对路径。

## 查看并运行用例

```bash
python3 -B run.py validate
python3 -B run.py list
runs_dir="$HOME/.local/state/takler-test-suite/runs"
"$TAKLER_PYTHON" -B run.py --runs-dir "$runs_dir" run DEF-001 --profile offline
"$TAKLER_PYTHON" -B run.py --runs-dir "$runs_dir" run EXE-001 --profile local
```

`validate` 检查目录条目和用例路径。`list` 输出用例编号、实现状态、形态、可用 profile 和名称。按功能查找用例时，可参考[用例目录](coverage.md)。

| Profile | 行为 | 可选参数 |
| --- | --- | --- |
| `offline` | 直接调用 Takler 的公开定义 API | 无 |
| `local` | 启动独立的本地服务，运行用例并清理服务及作业 | `--client python\|go`、`--transport grpc\|http`，默认 Python/gRPC |

例如，要通过 HTTP 运行客户端配置场景，先安装 HTTP extra，再执行以下命令。

```bash
"$TAKLER_PYTHON" -B run.py --runs-dir "$runs_dir" \
    run BAS-003 --profile local --client python --transport http
```

## 查看运行报告

`run` 会打印 `RUN_ID`、`RUN_DIR` 和 `RESULT`。将下列示例中的编号替换为实际 `RUN_ID`，并使用运行时相同的 `--runs-dir` 读取 `report.json`。

```bash
python3 -B run.py --runs-dir "$runs_dir" inspect exe-001-abcd1234
```

`--runs-dir` 是全局参数，写在 `run` 或 `inspect` 前。省略时，在 `takler-devel/repo/` 布局中，运行根目录是 `takler-devel` 同级的 `workspace/takler-test-suite/runs`。独立检出时使用 `$XDG_STATE_HOME/takler-test-suite/runs`。`XDG_STATE_HOME` 未设或为相对路径时，使用 `~/.local/state/takler-test-suite/runs`。

| `run` 退出码 | 含义 |
| --- | --- |
| `0` | 用例断言和清理均通过 |
| `1` | 用例失败或缺少运行条件，详见报告中的 `result` 和 `reason` |
| `2` | 目录、参数、用例或运行组合无效 |
| `130` | 用户按 Ctrl+C 中断 |

场景中的预期业务故障也有独立断言。用例通过时，报告会记录预期的拒绝、崩溃或异常退出。更多字段及常见问题见[排错与清理](troubleshooting.md)。

## 批量运行

`reproduce.py` 按 TOML 清单运行一组自动用例。默认清单为 [`selections/wsl-delivery.toml`](../../selections/wsl-delivery.toml)，包含离线、Python/gRPC 和 Go/HTTP 组合。使用套件的 uv 环境时，先执行 `uv sync --locked --group workflow` 安装包括 takflow 在内的 Python 依赖，再按[环境准备](baseline.md)设置 Go 客户端、查询探针和 orvix 的路径。

```bash
"$TAKLER_PYTHON" -B reproduce.py --selection selections/wsl-delivery.toml \
    --runs-dir "$runs_dir/batch-01" --workers 4
```

`--runs-dir` 必须指向尚未创建的目录。`--workers` 默认为 `1`，支持 `1` 至 `4`。入口保存输入清单、执行计划、每项运行的报告和 `summary.json`。任何一项失败、阻塞或未完成时，批量命令以非零状态退出。Ctrl+C 会通知本批正在执行的用例自行收尾。人工观察使用[操作演练](operator-exercise.md)中的单独入口。
