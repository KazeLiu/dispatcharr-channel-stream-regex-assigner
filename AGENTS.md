# AGENTS.md

本文件记录本项目（Dispatcharr 插件 `channel_stream_regex_assigner`）长期可复用的工程经验与约束。一次性需求、临时决策不写进来。

## 插件事件派发链路（m3u_refresh 自动触发）

- 触发信号：用户反馈"插件没有在 M3U 刷新后自动执行 / 收不到 m3u_refresh 事件 / events 字段不生效"。
- 根因 / 约束：
  - 在 action 上声明 `"events": ["m3u_refresh"]` 是**正确写法**（与官方同款参考插件 `PiratesIRC/Dispatcharr-Event-Channel-Managarr-Plugin` 一致），插件侧无需改。
  - 真正决定"能否触发"的是 **Dispatcharr 宿主版本**。派发链路为：
    `apps/m3u/tasks.py` 刷新完成 → `log_system_event('m3u_refresh', ...)`（`core/utils.py`）
    → `SystemEvent` 写库 + `_dispatch_system_event_integrations` → `dispatch_event_system`
    → `apps/connect/utils.py::trigger_event(event_name, payload)`
    → `PluginManager.iter_actions_for_event('m3u_refresh')` 过滤出声明了该事件的 action
    → 对**已启用**插件逐个 `run_action(key, action_id, {"event": "m3u_refresh", "payload": {...}})`。
  - 该 `events` 字段**官方 `Plugins.md` 尚未文档化**（Actions 章节只列了 id/label/description/button_*/confirm），属较新能力；旧版 Dispatcharr 不含 `iter_actions_for_event` 派发，插件永远收不到事件。
  - 派发时 `event` 与 `payload` 走 **`params`**（`run(action, params, context)` 的第二参），不在 `settings` 里。本插件 `run()` 用 `params.get("payload", {})` 取出，`account_name` 在 payload 中（来自 `log_system_event(account_name=account.name)`）。
- 正确做法：
  - 插件侧保持 `events: ["m3u_refresh"]` 写法，不要因为"不触发"去改插件代码。
  - 排查三件事按优先级：① Dispatcharr 版本是否够新（含上述派发链）；② 插件是否在 UI 启用（派发器跳过 disabled）；③ 升级插件后是否 reload（`events` 来自内存注册表 `lp.actions`，不 reload 读不到）。
  - 提供"每日定时执行"作为不依赖事件派发的兜底自动方案。
- 验证方式：Docker 日志搜 `channel_stream_regex_assigner run() 被调用`。手动点按钮应有 `来源=手动触发`；真实刷新后应有 `action=auto_m3u_refresh 来源=自动触发(事件=m3u_refresh)`。宿主侧 debug 日志关键字：`Dispatching event 'm3u_refresh' to N plugin action(s)`。
- 适用范围：所有"Dispatcharr 插件收不到系统事件 / events 不生效"类问题。

## 插件运行日志规范

- `run()` 入口已有 `_log_run_trigger` 记录每次调用（自动/手动来源）。target 函数（后台线程内）拿不到 context logger，统一用模块级 `LOGGER = logging.getLogger(PLUGIN_KEY)` 记录阶段日志（开始/完成/跳过/阻塞/合并/取消/失败）。
- 新增长耗时操作时，在"函数级开始与完成"加 INFO 日志即可；循环内部进度已有 `_log_progress` 节流输出，不要在循环体里逐条加日志。
- `latest_result` 是高频轮询 action，`_log_run_trigger` 已对其跳过，新增日志也不要为它打 INFO。

## 打包结构

- 产物：`dist/channel_stream_regex_assigner.zip`（扁平结构，无顶层目录）。
- 包含：`plugin.py`、`plugin.json`、`channel_rules.txt`（随包 seed 规则）、`README.md`、`sc/*.jpg`。
- `dist/`、`*.zip`、`__pycache__/` 已在 `.gitignore`，产物不入库。
- 重新打包前确保 `plugin.py` 的 `Plugin.version` 与 `plugin.json` 的 `version` 一致（Dispatcharr 靠版本号识别是否需要 reload）。

## 规则文件路径（升级安全）

- 持久路径：`/data/plugin_data/channel_stream_regex_assigner/channel_rules.txt`，**不在插件安装目录**，升级插件不会被新 zip 覆盖。
- 首次缺失时按顺序迁移：旧 `exports/channel_rules_template.txt` → 旧持久 `channel_rules_template.txt` → 随包 `channel_rules.txt` → 仍无则创建空白文件。
- 改动规则读取/解析逻辑后，优先跑 `tests/test_rules_storage.py`（含迁移、频道名匹配、任务互斥等回归）。

## 版本号约定

- 功能改动同时更新 `plugin.py::Plugin.version` 和 `plugin.json::version`，二者必须一致。
- 发版提交才允许只动版本号；否则版本号并入触发它的功能提交。
