# Channel Stream Regex Assigner

Dispatcharr 插件：按正则规则把现有 Streams 自动挂到现有 Channels，按照规则排序频道里面的流，获取订阅中的EPG放到程序中订阅，等方便程序使用的功能

![alt text](sc/QQ20260527-162442.jpg)
![alt text](sc/QQ20260527-162453.jpg)

## 安装

1. 将 `channel_stream_regex_assigner.zip` 上传到 Dispatcharr 的 Plugins 页面。
2. 启用插件。

## 推荐流程

1. 点击 `生成规则`。(我里面放了一份简单的，可以自己添加或者直接通过生成规则覆盖掉)
2. 直接编辑 `/data/plugin_data/channel_stream_regex_assigner/channel_rules.txt`。
3. 可先填写 `测试正则` 并点击 `测试` 查看单条正则匹配数量。也可以`预览规则匹配`来看整个规则最终的匹配流
4. 确认报告无误后点击 `立即执行`。
5. 后台任务运行时，可反复点击 `查看结果` 查看百分比进度，也可以在 Docker 日志里看到定时进度输出；完成后到插件目录 `exports/` 查看 txt 报告。

## 匹配结果排序

插件会在写入频道前，对同一条规则匹配到的 Streams 做可选排序：

- `关键词优先`：逗号分隔，例如 `4k,mcp`。Stream 名称包含 `4k` 的排前面，其次是包含 `mcp` 的，最后是其他流。
- `订阅源优先`：逗号分隔 M3U 订阅源名称，例如 `Source-A,Source-B`。同时填写关键词时，排序为：`Source-A` 里的 `4k/mcp`、`Source-B` 里的 `4k/mcp`、`Source-A` 其他流、`Source-B` 其他流、最后其他订阅源的流。
- 同一优先级内部按订阅源名称、Stream 名称和 ID 稳定排序。

如果 Streams 已经添加到频道里，点击 `重排已挂流` 可以只更新现有挂流顺序一次，不会新增或删除流。有规则文件时只处理规则里的频道；规则文件为空时处理全部已有挂流频道。

## 自动执行

插件只在 Dispatcharr 发出 `m3u_refresh` 事件后自动执行，不做固定间隔轮询。

1. 打开 `M3U 刷新后自动执行`。
2. 设置 `M3U 刷新后延迟分钟`。
3. M3U 刷新成功后，插件会延迟排队执行匹配。
4. 需要立刻跑时，点击 `立即执行`。

安装或更新插件后，如果页面仍显示旧版本，请在 Plugins 页面点一次 reload，或禁用再启用本插件。

## M3U 头部 EPG 自动导入

打开 `自动导入 M3U 头部 EPG` 后，插件会读取 M3U 文件开头一点点内容，识别这些写法：

```text
#EXTM3U x-tvg-url="https://example.com/epg.xml.gz"
#EXTM3U url-tvg="https://example.com/epg.xml"
#EXTM3U tvg-url="https://example.com/xmltv.php"
```

插件只看文件开头，遇到第一个 `#EXTINF` 会停止，所以不会读取整个大文件；如果开头没有写 EPG 地址，就直接跳过。找到 EPG URL 后：

- 如果 EPG Sources 里已有相同 URL，会复用现有源。
- 如果没有，会创建 `Auto EPG - <M3U账号名>`。
- 新建源会由 Dispatcharr 自动拉取一次。
- 复用已有源时，如果打开 `导入后拉取 EPG`，会再排队刷新一次。

手动点击 `扫描 EPG` 会扫描全部启用的 M3U 账号；M3U 刷新成功事件触发时，会优先扫描本次刷新的账号。

## 规则格式

推荐使用 `|||` 分隔，避免正则里的 `|` 被误拆：

```text
# channel_id ||| channel_name ||| regex ||| mode ||| max_streams
12 ||| CCTV-1 ||| ^CCTV[-_ ]?1($|高清|HD) ||| merge ||| 0
13 ||| CCTV-5 ||| ^CCTV[-_ ]?5($|体育|HD) ||| replace ||| 2
20 ||| 湖南卫视 ||| ^湖南卫视.*$ ||| merge ||| 0
```

- `channel_id`：可选频道 ID，主要用于本机兼容；共享规则时可保留但不会优先使用。
- `channel_name`：优先按频道名称找频道；如果多个频道同名，使用 ID 最小的第一个频道。
- `regex`：默认匹配 Stream 名称；可在插件设置里改为 URL 或 名称+URL。
- `mode`：`merge` 合并，`replace` 覆盖。
- `max_streams`：最大流数量，`0` 表示无限。

也兼容 Tab 分隔和 `空格 | 空格` 分隔。

规则文件的默认持久路径：

```text
/data/plugin_data/channel_stream_regex_assigner/channel_rules.txt
```

这个路径不在插件安装目录里，升级插件时不会被新 zip 覆盖。打开插件或执行动作时，如果该文件不存在，插件会先从旧路径 `/data/plugins/channel_stream_regex_assigner/exports/channel_rules_template.txt`、旧持久路径 `/data/plugin_data/channel_stream_regex_assigner/channel_rules_template.txt` 或随包 `channel_rules.txt` 自动迁移；仍不存在才创建一份空白规则文件。`生成规则` 会按当前 Channels 覆盖生成规则文件，执行前会要求确认。

## 去重逻辑

- 同一个频道已有相同 `stream_id` 时跳过。
- 合并时，URL 相同也跳过。
- 新匹配结果内部会按 `stream_id` 和 URL 去重。
- `replace` 规则没有匹配结果时，默认不会清空频道；需要打开 `允许空匹配覆盖清空频道`。
