# 失败诊断、证据与清理

先通过 RUN_DIR 定位本轮目录，再看 report.json。result=environment_blocked 表示缺少实际入口或能力；failed 表示判据、操作或清理未通过；interrupted 不算通过。不要仅凭日志最后一行或某个业务任务 complete 推断整个集合成功，批量运行应检查 summary 的 expected_runs、passed_runs、各项 result 和退出码。

| 现象 | 核对与处理 |
| --- | --- |
| 找不到服务/CLI | 检查所选解释器的 bin 目录内有 takler-server、takler-client-py，使用绝对解释器路径。 |
| Go / orvix / probe 环境阻塞 | 按 baseline 的环境变量设置同版本真实可执行文件；EXE-003 的 orvix basename 必须为 orvix。不要用临时假程序替代环境。 |
| HTTP 连接失败 | 确认已安装 HTTP extra，使用本轮 connect.yaml 的 HTTP 端口；服务也有内部 gRPC 端口，不能互换。 |
| 缺失 connect 文件 | Python 返回 FileNotFoundError/3，Go 返回配置加载诊断/1。检查路径；不应退回默认地址继续操作。 |
| 含空格解释器下 EXE-002 失败 | 使用当前版本的 Shell trampoline，由引用后的解释器执行回执故障脚本；不要将含空格路径直接写进 shebang。 |
| 查询/周期保存等待较长 | QRY-002、TIM、RCV 等使用真实等待；保留截止和事件，避免外层工具提前终止。 |
| 重启服务 halted | RCV 的预期恢复保护；先核对 restore_summary 和外部作业，再按场景要求 resume。 |
| checkpoint_failure 后 resume 仍拒绝 | 先恢复存储权限并等周期保存成功；resume 即使拒绝也会清除 manual/recovery，人工暂停需独立核对。 |
| orvix 文件数量多于作业数 | `.job<UUID>` 是原始作业；`.submit`、`.submit.log`、`.info.yaml` 是相关产物，不能都计为重复运行。 |
| TUI 键盘/截图没有响应 | 检查真实终端尺寸、stty -ixon 和焦点；命令面板关闭后再输入 q。树属性行会改变后续行号，不照搬旧截图坐标。 |
| TUI 恢复后仍有旧错误文字 | 核对新 last refresh 和 stale 是否撤除；旧错误/通知可能继续显示，不等于仍断连。 |

证据入口：events.jsonl 记录命令类别及启停；阶段 JSON 保留公开状态和运行身份；process-intervals.json 保留 PID、启动刻度及实际进程区间；job-outputs.json 关联输出；server_history/cleanup_history 对齐重启代次。raw server 日志、client-error.log、error.log 和 probe 私有诊断用于定位，日志不是恢复数据。

## 清理顺序

正常自动运行和 Ctrl-C 由入口管理自有作业、查询探针和服务。交互演练先退出 TUI，再写本轮 CONTROL=finish 或 stop。若服务被本次演练暂停，处理器先恢复它再清理。RCV-004 的 finally 恢复目录权限；RCV-003 通过原服务 group/session、cwd、可执行文件和 PID 启动刻度绑定 pidfd 清理辅助进程。

确认 report 中 cleanup 正常、监听释放，进程观察全部有 exit_upper_ns；对于中断/失败，另查 cleanup_error、所有 peers 和服务辅助进程。只检查一个旧 PID 是否存在不足以识别原进程，需结合启动刻度。不要用进程名批量终止工作区服务。

运行目录默认保留，不自动删除历史证据。确认所有进程和端口已清理、所需证据已归档后，才清理自己明确选择的 RUN_DIR；删除文件不会终止进程。批量输出根目录禁止覆盖，复跑使用新目录。

原始 `.check`、`private-*`、security、含凭据的定义、生成 `.job<UUID>` 和 `.submit` 以及环境/探针私有诊断应留在 0700 目录。公开归档采用明确文件选择，扫描本次实际生成的 operator/API/child 秘密，发现命中即失败；不靠遮盖内容把泄漏判为通过。公开包的 manifest 用于核对文件路径和 SHA256；保留失败和未核验记录，不将它们改成成功。
