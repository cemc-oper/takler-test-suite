# 基线、安装与外部入口

本工程没有替代调度器或模拟 orvix。先准备实际产品，再选择场景。目录自身的 validate/list 仅依赖 Python 标准库；场景运行使用已安装 Takler 的解释器。工程不读取工作区 records 来运行用例。

| 组件/工具 | 本轮基线 | 使用范围 |
| --- | --- | --- |
| WSL2 Ubuntu | 24.04.5，Linux 6.6.87.2，普通用户 | 本地进程、ss、真实目录权限故障 |
| Python | 3.14.3；入口要求 3.11+ | 服务、Python CLI、公开 API、设施测试 |
| Takler | `39b60fe6b2e5b74d97fb81cb0c2735f391546c1d`，wheel `0.1.1.dev184+g39b60fe6b` | 全部场景；HTTP/TUI 需要对应 extras |
| takflow | `1e108ecdfa9e7d2627a43f574585318b8f71cf46`，`0.1.2.dev31+g1e108ecdf` | EXE-003 导出；与 orvix 的 jobspec=1.0.1 |
| orvix | `5af2273a7366ea44c76871a81e2dceb9fa30aa2a`，`v0.2.1-0.20261102105730-5af2273a7366` | EXE-002/003，真实 local 后端；该源码 go.mod 要求 Go 1.26 |
| Go CLI | `db67b6fc003abf2d54e3bcd3ebb601ed6142129b` | 选择 --client go；该源码 go.mod 要求 Go 1.23 |
| Go / uv | 1.27.2 / 0.11.2 | 本轮构建工具，使用已有缓存 |
| openssl | 系统命令 | SEC-002 为本次生成短期 CA/证书 |
| 系统 Python | `/usr/bin/python3` 3.12.3，提供 Linux pidfd API | RCV-003 精确清理自有 checkpoint 辅助进程 |

常规产品开发环境按 Takler 自己的锁文件准备：在 Takler checkout 中执行 `uv sync --locked --all-groups`，然后把本工程的 python 指向该环境的绝对路径。在其他安装方式中，应确认 HTTP 所需的 fastapi/uvicorn/httpx、TUI 所需的 textual/rich 及 pytest 已安装。不要把场景项目当作产品包加入产品日常 pytest 搜集。

本轮实际复现使用工作区 [build-env.sh](../../../doc/2026-10-10-scenario-testing/records/scn-11/build-env.sh)：从四个本地源码仓创建副本、检出固定 commit，构建 Takler/takflow wheel、orvix/Go CLI/query_probe，在新虚拟环境安装两个 wheel，并通过本次 `.pth` 读取已经准备的 Takler 运行依赖。源码及参考仓均不修改。精确导入路径、运行依赖版本、构建元数据和 SHA256 在本轮 preflight/build 记录中。

这验证了新 wheel、新虚拟环境和新运行目录，未验证空缓存机器的完整离线安装。搬到其他机器时先按产品锁文件安装依赖，再运行这些步骤；不能把旧 `/tmp` 路径视作可用安装产物。

| 选择 | 需设置的入口 |
| --- | --- |
| Python/gRPC 基础在线 | 解释器旁的 takler-server、takler-client-py；系统 ss |
| HTTP | 相同环境的 HTTP extra，--transport http |
| Go | 绝对可执行路径 SCENARIO_GO_CLIENT |
| Go 查询 QRY-001–003、SEC-003 | 另设 SCENARIO_GO_PROBE，由 assets/query_probe.go 在同版 Go 源码副本构建，使用公开 SDK |
| EXE-002/003 | 绝对可执行路径 SCENARIO_ORVIX_BIN，文件名 orvix；EXE-003 另需同环境安装 takflow |
| RCV-004 | 普通用户；checkpoint 专用目录可在本次私有目录内修改权限 |
| 交互 TUI | 同环境的 takler-tui；真实终端建议 150×46，stty -ixon，私有 HOME/Downloads |

LocalService 显式连接 127.0.0.1、使用本次动态端口和独立 HOME，并隔离影响连接的环境变量。gRPC 的监听绑定遵循产品当前行为，在隔离的本机环境运行。CLI、环境与 connect 文件优先级由 BAS-003 实跑核对；缺文件的 Python/Go 诊断和退出码不同，不能硬套同一格式。
