# cf-vps-monitor 二开版

[简体中文](https://github.com/aiqiji7/cf-vps-monitor/blob/main/README.md) | [English](https://github.com/aiqiji7/cf-vps-monitor/blob/main/README-EN.md)

这是一个基于 Cloudflare Worker + D1 的 VPS 探针、网站检测和 LLM 端点可用性监控面板。

## 二开说明

本项目是基于原项目的二次开发版本。

- 当前项目地址：https://github.com/aiqiji7/cf-vps-monitor
- 原项目地址：https://github.com/kadidalax/cf-vps-monitor

### 与原项目的主要区别

- 新增 LLM 端点可用性监控，支持 OpenAI 兼容的 `/v1/chat/completions` 接口。
- 支持按 Provider 分组展示 LLM 端点；未填写 Provider 名称时，会自动使用 API URL 的域名作为默认名称。
- 新增 LLM 高延迟阈值、LLM 超时阈值和网站高延迟阈值配置。
- 后台阈值和页面响应时间统一以秒 `s` 展示；内部数据库和检测逻辑仍使用毫秒，避免破坏历史数据。
- 区分正常、高延迟、超时、故障、错误、待检测等状态；超时和高延迟在 badge、行背景和 24 小时记录色块中使用不同颜色。
- 用户刷新前台页面时，会自动触发一次公开 LLM 端点可用性检测。
- LLM 端点支持输出预览、期望内容匹配、公开展示开关、通知开关和 24 小时状态历史。
- 管理后台拆分网站阈值和 LLM 阈值，网站高延迟阈值放在网站监控管理区，LLM 阈值放在 LLM 端点管理区。
- 保留并增强 VPS 监控、网站监控、Telegram/ntfy 通知、自定义背景、页面透明度、排序和可见性控制等功能。

## 功能概览

### VPS 监控

- 通过 `cf-vps-monitor.sh` Agent 上报 VPS 状态。
- 展示 CPU、内存、磁盘、上传/下载速度、总流量、运行时长和最后更新时间。
- 后台自动生成服务器 ID、API Key 和一键安装命令。
- 支持服务器排序、公开展示开关和后台管理。

### 网站在线检测

- 支持添加 HTTP/HTTPS 网站 URL。
- 展示状态、状态码、响应时间和 24 小时历史记录。
- 支持网站高延迟阈值配置，页面以秒 `s` 显示。
- 支持公开展示开关、排序和后台管理。

### LLM 端点可用性监控

- 支持 OpenAI 兼容接口，例如：`https://api.example.com/v1/chat/completions`。
- 支持配置 API Key 和模型名称。
- 测试 Prompt 与期望包含内容为全局设置（所有端点共用，一次配置覆盖所有模型），在后台 LLM 端点管理区统一配置。
- 支持多个模型批量添加：每行一个、逗号分隔或 JSON 数组。
- 支持 Provider 分组；Provider 名称为空时自动取 API URL 域名，例如 `https://api.openai.com/v1/chat/completions` 会显示为 `api.openai.com`。
- 同一 API URL 若填写不同 Provider 名称，会视为不同 Provider，模型不会聚合到同一组。
- 状态包括：正常、高延迟、超时、故障、错误、待检测。
- 支持 LLM 高延迟阈值和全局 LLM 超时阈值配置（所有端点共用），后台输入和前台展示均使用秒 `s`。
- 前台页面加载/刷新时会自动排队触发一次公开 LLM 端点检测。
- 24 小时历史色块会区分高延迟、超时和故障。

### 通知与外观

- 支持 Telegram 通知。
- 支持 ntfy 通知。
- 支持自定义背景图和页面透明度。
- 支持浅色/暗色主题。

## 状态颜色说明

- 正常：绿色。
- 高延迟：黄色。
- 超时：橙色。
- 故障/错误：红色。
- 待检测/无记录：灰色。

## 部署要求

- 一个 Cloudflare 账户。
- 一个 Cloudflare D1 数据库。
- 一个 Cloudflare Worker。
- 建议配置 `JWT_SECRET`，并在首次登录后立即修改默认密码。

## 部署步骤

### 1. 创建 D1 数据库

1. 登录 Cloudflare 控制面板。
2. 进入 `存储和数据库` → `D1 SQL 数据库`。
3. 点击 `创建数据库`。
4. 数据库名称可自定义，例如 `vps-monitor-db`。

### 2. 创建 Worker 并部署代码

1. 进入 `Workers & Pages`。
2. 创建一个 Worker。
3. 打开 Worker 在线编辑器。
4. 删除默认代码。
5. 将本仓库 `worker.js` 的全部内容复制进去。
6. 点击部署。

### 3. 配置环境变量

在 Worker 的 `设置` → `变量和机密` 中添加：

| 变量名 | 必填 | 说明 |
| --- | --- | --- |
| `JWT_SECRET` | 建议必填 | 登录 Token 签名密钥，建议使用 30 位以上随机字符串 |
| `USERNAME` | 可选 | 初始管理员用户名，不填时使用默认值 |
| `PASSWORD` | 可选 | 初始管理员密码，不填时使用默认值 |

默认登录信息：

- 用户名：`admin`
- 密码：`monitor2025!`

首次登录后请立即修改密码。

### 4. 绑定 D1 数据库

1. 进入 Worker 设置。
2. 找到绑定配置。
3. 添加 D1 数据库绑定。
4. 变量名称必须填写：`DB`。
5. 选择你创建的 D1 数据库并保存部署。

### 5. 初始化数据库

部署完成并绑定 D1 后，访问：

```text
https://你的-worker-url/api/init-db
```

看到类似下面的结果即表示初始化成功：

```json
{"success": true, "message": "数据库初始化完成"}
```

### 6. 添加 Cron 触发器

在 Worker 的触发器中添加 Cron，用于定时检测网站和 LLM 端点。

建议先设置为每小时执行一次。后台可以配置 LLM 探测频率倍数，例如：

- `1`：每次 Cron 都检测 LLM。
- `2`：每两次 Cron 检测一次 LLM。
- `3`：每三次 Cron 检测一次 LLM。

## 使用说明

### 登录后台

访问 Worker URL，点击右上角管理员登录，或访问：

```text
https://你的-worker-url/login.html
```

登录后请先修改默认密码。

### 添加 VPS 服务器

1. 后台点击添加服务器。
2. 输入服务器名称和描述。
3. 保存后系统会生成服务器 ID 和 API Key。
4. 在后台复制一键安装命令到 VPS 上执行。

也可以手动下载脚本：

```bash
wget -O cf-vps-monitor.sh https://raw.githubusercontent.com/aiqiji7/cf-vps-monitor/main/cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh
```

或：

```bash
curl -O https://raw.githubusercontent.com/aiqiji7/cf-vps-monitor/main/cf-vps-monitor.sh && chmod +x cf-vps-monitor.sh && ./cf-vps-monitor.sh
```

安装时需要填写：

- 服务器 ID。
- API Key。
- Worker 地址。

### 添加网站监控

1. 后台点击添加监控网站。
2. 输入网站名称和 URL。
3. 保存后会按 Cron 周期检测。
4. 可在网站监控管理区调整网站高延迟阈值，单位为秒 `s`。

### 添加 LLM 端点

1. 后台点击添加 LLM 端点。
2. 输入 API URL，例如：`https://api.example.com/v1/chat/completions`。
3. 输入模型名称，支持多个模型批量添加。
4. 可填写 API Key；测试 Prompt 和期望包含内容为全局设置，在 LLM 端点管理区统一配置（所有端点共用）。
5. Provider 名称可留空，系统会自动使用 API URL 的域名；若同一 API URL 填写不同 Provider 名称，会分成不同 Provider 组。
6. 可在 LLM 端点管理区配置：
   - LLM 高延迟阈值，单位为秒 `s`。
   - LLM 超时阈值（全局，所有端点共用），单位为秒 `s`。
   - LLM 探测频率倍数。
   - 测试 Prompt 与期望包含（全局，所有端点共用，一次设置覆盖所有模型）。

## 时间单位说明

页面和后台表单统一使用秒 `s`：

- 响应时间显示为秒。
- 网站高延迟阈值显示为秒。
- LLM 高延迟阈值显示为秒。
- LLM 超时阈值显示为秒。

代码内部、数据库字段和 API payload 仍使用毫秒，字段名也保留 `_ms`，这是为了兼容已有数据和检测逻辑。

## 注意事项

- Cloudflare Worker 和 D1 有免费额度限制。VPS 越多、上报频率越高，请求和写入消耗越大。
- LLM 端点检测会请求外部 API。页面刷新会额外触发一次公开 LLM 端点检测，可能增加外部 API 调用量。
- 默认密码不安全，请首次登录后立即修改。
- API Key、Telegram Token、ntfy topic 等敏感信息请妥善保管。
- 如果部署后页面异常，请检查 Worker 日志、D1 绑定变量名是否为 `DB`，以及是否已访问 `/api/init-db` 初始化数据库。

## 致谢

感谢原项目作者提供基础版本：

- 原项目：https://github.com/kadidalax/cf-vps-monitor

本仓库是在原项目基础上的二次开发版本，当前功能和界面以本仓库代码为准。
