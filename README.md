# Takler Test Suite

面向 [Takler](https://github.com/cemc-oper/takler) 的端到端场景测试与操作演练套件。通过公开 API 和客户端验证工作流行为，覆盖流程定义、部署、调度、作业执行、查询、权限、恢复和容量。每次运行生成独立报告，供复查与排错。

## 环境要求

- Python 3.11 或更新版本，以及 [uv](https://docs.astral.sh/uv/getting-started/installation/)。
- 在线用例在隔离的 Linux 或 WSL 环境中运行，需要 `ss`。Go 客户端和跨组件用例的附加依赖见[环境准备](docs/source/baseline.md)。

## 快速开始

```bash
git clone https://github.com/cemc-oper/takler-test-suite.git
cd takler-test-suite
uv sync --locked --group scenarios
python3 -B run.py validate
python3 -B run.py list
uv run --locked --group scenarios python -B run.py --runs-dir ./runs \
    run DEF-001 --profile offline
uv run --locked --group scenarios python -B run.py --runs-dir ./runs \
    run EXE-001 --profile local
```

`local` 默认使用 Python CLI 和 gRPC。查看命令输出中的 `RUN_DIR`，打开该目录下的 `report.json` 获取结果与清理状态。如何按 `RUN_ID` 查询报告及批量运行用例，见[运行指南](docs/source/quickstart.md)。

## 文档

- [用例目录](docs/source/coverage.md)
- [CLI/TUI 操作演练](docs/source/operator-exercise.md)
- [容量与多窗口监控](docs/source/capacity.md)
- [排错、运行报告与清理](docs/source/troubleshooting.md)
- [本地构建文档](docs/source/contributing.md#构建文档)

## 参与贡献

用例登记在 [`catalog.toml`](catalog.toml)，实现在 [`cases/`](cases/)。运行器的设施测试位于 [`tests/`](tests/)。添加用例和执行设施测试的步骤见[贡献指南](docs/source/contributing.md)。
