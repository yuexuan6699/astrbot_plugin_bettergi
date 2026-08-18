# BetterGI 远程控制插件

AstrBot 插件，用于远程控制 [BetterGI（更好的原神）](https://www.bettergi.com/)。

## 功能特性

- **本地/远程双模式** - 通过配置切换，支持同设备直接执行或跨设备远程控制
- **Webhook 事件通知** - 接收 BetterGI 事件并自动转发到聊天会话
- **任务完成检测** - 基于 BetterGI Webhook 的 `dragon.end`/`group.end` 事件
- **定时任务** - 每天定时自动执行命令
- **命令名称自定义** - 每个命令的触发词均可配置
- **简化配置** - 命令分为"一条龙"和"调度器"模板，只需填配置名

## 应用项目

- [AstrBot](https://docs.astrbot.app/)
- [BetterGI](https://www.bettergi.com/)

## 架构说明

### 本地模式（单插件式）

AstrBot 和 BetterGI 在同一设备，插件直接通过子进程执行 `BetterGI.exe` 命令。

### 远程模式（分离式）

AstrBot 和 BetterGI 在不同设备。需要在 BetterGI 所在电脑上运行辅助程序（`assistant/server.py`），插件通过网络调用辅助程序 API 来远程控制 BetterGI。

```
┌─────────────┐         HTTP          ┌──────────────┐
│   AstrBot   │  ←──────────────────→ │   辅助程序    │
│   (插件)     │   run/stop/status     │  (FastAPI)   │
└─────────────┘                       └──────┬───────┘
      ↑                                      │
      │ Webhook 事件推送                      │ subprocess
      │ (BetterGI → 插件HTTP服务器)           │
      │                              ┌───────▼───────┐
      └──────────────────────────────│   BetterGI    │
                                     └───────────────┘
```

## 配置说明

### 运行模式

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `mode` | 运行模式 (`local`/`remote`) | `local` |
| `bettergi_dir` | BetterGI 安装目录（本地模式必填） | `""` |

### 命令配置

使用模板列表配置命令，分为"一条龙"和"调度器配置组"两种类型：

- **一条龙** - 只需填写一条龙配置名称，插件自动组装 `--startOneDragon <名称>`
- **调度器配置组** - 只需填写配置组名称，插件自动组装 `--startGroups <名称>`

### 命令名称自定义

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `command_names.prefix` | 命令前缀 | `better` |
| `command_names.run` | 运行命令名 | `运行` |
| `command_names.status` | 状态命令名 | `状态` |
| `command_names.stop` | 停止命令名 | `停止` |
| `command_names.log` | 日志命令名 | `日志` |
| `command_names.bind` | 绑定命令名 | `绑定` |
| `command_names.help` | 帮助命令名 | `帮助` |

### Webhook 配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `webhook.enable` | 启用 Webhook 接收 | `true` |
| `webhook.host` | 监听地址 | `0.0.0.0` |
| `webhook.port` | 监听端口 | `8088` |
| `webhook.path` | Webhook 路径 | `/bettergi/webhook` |
| `webhook.token` | 验证令牌（可选） | `""` |

### 定时任务

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `scheduled_task.enable` | 启用定时任务 | `false` |
| `scheduled_task.run_time` | 执行时间 (HH:MM) | `04:00` |
| `scheduled_task.command_index` | 执行的命令序号 | `1` |

### 远程模式配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `remote.url` | 辅助程序地址 | `http://127.0.0.1:9099` |
| `remote.token` | 认证令牌 | `""` |

### 权限配置

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `better_master` | 有权限的用户ID列表 | `[]`（所有人可用） |

## 使用方法

### 基础命令

假设使用默认命令名称（前缀 `better`）：

| 命令 | 说明 |
|------|------|
| `better运行` | 运行第1个命令 |
| `better运行 选择` | 查看可用命令列表 |
| `better运行 1` | 运行第1个命令 |
| `better状态` | 查看运行状态 |
| `better停止` | 停止当前任务 |
| `better日志` | 查看最近10条事件 |
| `better日志 20` | 查看最近20条事件 |
| `better日志 清除` | 清除事件记录 |
| `better绑定` | 绑定当前会话为通知接收方 |
| `better帮助` | 显示帮助信息 |

### Webhook 配置

1. 在插件配置中设置 Webhook 端口和路径
2. 启动插件后，查看日志获取 Webhook 地址
3. 在 BetterGI 设置 → 通知渠道 → Webhook 中填入地址

完整地址格式：`http://<AstrBot所在IP>:<端口><路径>`

例如：`http://192.168.1.100:8088/bettergi/webhook`

### 事件类型

BetterGI 支持的 Webhook 事件类型：

| 事件 | 说明 |
|------|------|
| `dragon.start` / `dragon.end` | 一条龙启动/结束 |
| `group.start` / `group.end` | 配置组启动/结束 |
| `task.cancel` / `task.error` | 任务取消/错误 |
| `domain.start` / `domain.end` | 自动秘境启动/结束 |
| `tcg.start` / `tcg.end` | 七圣召唤启动/结束 |

完整事件列表请参考 [BetterGI Webhook 文档](https://www.bettergi.com/dev/webhook.html)

## 远程模式使用

### 1. 安装辅助程序依赖

在 BetterGI 所在电脑上：

```bash
cd assistant
pip install -r requirements.txt
```

### 2. 配置 config.yaml

编辑 `assistant/config.yaml`，填写 BetterGI 安装目录和认证令牌：

```yaml
host: "0.0.0.0"
port: 9099
bettergi_dir: "D:\\BetterGI"
token: "your-secret-token"
log_level: "INFO"
```

### 3. 使用 运行功能.bat

双击 `运行功能.bat`，通过交互式菜单操作：

| 选项 | 说明 |
|------|------|
| 1. 启动辅助程序 | 前台运行，可看到日志输出，Ctrl+C 停止 |
| 2. 注册开机自启 | 创建 Windows 计划任务，登录时自动后台运行 |
| 3. 取消开机自启 | 删除计划任务 |
| 4. 查看自启状态 | 查看计划任务是否已注册 |
| 5. 查看运行状态 | 检查辅助程序是否正在运行 |
| 6. 退出 | 退出菜单 |

### 4. 配置插件

在 AstrBot 插件配置中：
- `mode` 设为 `remote`
- `remote.url` 填入辅助程序地址（如 `http://192.168.1.200:9099`）
- `remote.token` 填入与 config.yaml 中一致的令牌

### 日志

辅助程序运行日志保存在 `assistant/assistant.log`，包含所有 API 调用和错误信息。

## 安装和依赖

插件依赖（自动安装）：
- aiohttp >= 3.8.0
- psutil >= 5.9.0

辅助程序依赖（远程模式，手动安装）：
- fastapi >= 0.100.0
- uvicorn >= 0.23.0
- psutil >= 5.9.0
- pyyaml >= 6.0

## 作者

- [苏月晅](https://yuexuan6699.dpdns.org/)

## 许可证

跟随 AstrBot 和 BetterGI 项目
