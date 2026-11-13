# 贡献指南

## 添加场景用例

1. 在 [`catalog.toml`](../../catalog.toml) 登记用例编号、分类、形态、难度、角色、能力、profile 和实现状态。沿用对应分类下已有条目的结构。
2. 在 `cases/<一级分类>/<二级分类>/<用例编号小写_名称>/case.py` 实现入口。例如 `DEF-001` 的目录以 `def_001_` 开头。
3. 离线用例定义 `run(evidence)`，使用 Takler 的公开构建 API。在线用例定义 `run(evidence, service)`，使用共享的 `LocalService` 和公开客户端。需要在服务启动前生成配置或检查依赖时，可定义 `prepare(evidence, service)`。
4. 记录用于判定结果的公开状态、业务输出和清理结果。使用 `evidence.record_checks()` 保存关键判据。场景专用脚本与模板可以放在用例目录中，跨用例共享的资产放在 `assets/`。
5. 更新[用例目录](coverage.md)，执行目录校验和新增用例。共享运行设施的行为变更应在 `tests/` 添加回归测试。

## 运行设施测试

在套件仓库根目录执行，使用[环境准备](baseline.md)中的待测解释器。完整设施测试需要 `pytest`、Takler 的 HTTP extra 和在线环境。

```bash
python3 -B run.py validate
"$TAKLER_PYTHON" -B -m pytest -q -s tests
```

## 构建文档

在套件仓库根目录执行。`uvx` 会在独立工具环境中安装 Sphinx、PyData Sphinx Theme 和 MyST。MyST 支持现有 Markdown 页面与 Sphinx 目录和交叉引用。

```bash
uvx --from 'sphinx>=8.1' --with-requirements docs/requirements.txt \
    sphinx-build -M html docs/source docs/build -W
```

在浏览器打开 `docs/build/html/index.html`。`-W` 将 Sphinx 警告作为构建失败处理。文档源文件和 Sphinx 配置位于 `docs/source/`，导航从 `docs/source/index.rst` 开始。`docs/build/` 存放生成的 HTML 和构建缓存，已加入 Git 忽略规则。
