# 环境准备

## 创建 uv 环境

安装 [uv](https://docs.astral.sh/uv/getting-started/installation/)，使用 Python 3.11 或更新版本。在**套件仓库**根目录执行以下命令。uv 根据 `uv.lock` 建立 `.venv`，并安装套件配套的 Takler、HTTP 和 TUI 依赖。

```bash
uv sync --locked --group scenarios
export TAKLER_PYTHON="$PWD/.venv/bin/python"
```

- `--group scenarios` 用于运行常规用例和独立 TUI 演练。
- `--group test` 会额外安装 pytest 和 Ruff。
- `--group workflow` 会额外安装 EXE-003 所需的 takflow。批量清单包含 EXE-003 时也应选这一组。
- `--only-group docs` 只安装文档构建依赖。可用 `--all-groups` 同步各组。

`uv.lock` 通过 Git 提交锁定 Takler 和 takflow 的版本。`validate` 和 `list` 也可以直接用系统 Python 执行。

## 选择待测版本

如需针对另一份 Takler 源码测试，在 **Takler 仓库**执行 `uv sync --locked --all-groups`，然后把 `TAKLER_PYTHON` 设为该仓库 `.venv/bin/python` 的绝对路径。也可用 `uv venv` 和 `uv pip install` 将待测 wheel 安装到独立环境，按需选择 `http` 和 `tui` extras。直接调用所选解释器运行套件脚本即可。[Takler 文档](https://takler.readthedocs.io/)

## 在线用例的系统要求

在线用例使用隔离的 Linux 或 WSL 环境。确保 `ss` 可用（Ubuntu 由 `iproute2` 提供），并检查 `takler-server` 和 `takler-client-py` 都在解释器所在的 `bin` 目录中。即使选择 Go 客户端，运行器也会检查这两个入口。每次运行使用动态端口、独立的 `HOME` 和 `connect.yaml`，客户端连接 `127.0.0.1`，服务的监听绑定由待测 Takler 决定。

| 用例或运行方式 | 附加要求 |
| --- | --- |
| `--transport http` | 在待测 Python 环境安装 Takler 的 `http` extra |
| `--client go` | 设置 `SCENARIO_GO_CLIENT` 为 `takler_client` 的绝对可执行路径 |
| Go 下的 QRY-001–003、SEC-003 | 另设 `SCENARIO_GO_PROBE` 为与 Go CLI 同源码版本构建的 `query_probe` 绝对路径 |
| EXE-002、EXE-003 | 设置 `SCENARIO_ORVIX_BIN` 为真实 [orvix](https://github.com/cemc-oper/orvix) 的绝对可执行路径，使用 `local` 后端。EXE-003 要求文件名为 `orvix`，并在待测 Python 环境安装兼容的 [takflow](https://github.com/cemc-oper/takflow) |
| SEC-002 | 系统 `openssl` 命令，用于生成本轮测试证书 |
| RCV-003、CAP-004 | `/usr/bin/python3` 提供 Linux pidfd API，供清理 checkpoint 辅助进程使用 |
| RCV-004 | 以普通用户运行，运行目录所在文件系统须实施 Unix 目录权限 |
| CAP-004、人工 TUI 演练 | 安装 Takler 的 `tui` extra，确保同一环境提供 `takler-tui` |

## 构建 Go 客户端和查询探针

在与待测 Takler 版本配套的 [Go 客户端](https://github.com/cemc-oper/takler-client) 源码检出中执行以下命令。`make` 生成 `bin/takler_client`。查询探针使用该检出的 Go 模块构建，供需要探针的场景使用。

```bash
suite=/absolute/path/to/takler-test-suite
cd /absolute/path/to/takler-client
make
export SCENARIO_GO_CLIENT="$PWD/bin/takler_client"
go build -mod=readonly -o "$PWD/bin/query_probe" "$suite/assets/query_probe.go"
export SCENARIO_GO_PROBE="$PWD/bin/query_probe"
```
