
2.0.6: 修复截图转发 26.08.18

- base64 图片改为先保存临时文件再用 file_image 发送
- 兼容性更好，避免 base64 格式在各平台适配器中的差异

2.0.5: 转发截图 26.08.18

- Webhook 事件通知支持转发 BetterGI 截图
- 使用 base64_image 发送 base64 编码的 JPEG 图片

2.0.4: 修复命令不执行 26.08.18

- 修复 yield 后代码不执行导致 BetterGI 不启动
- 命令执行改为先执行再返回结果，避免事件传播被中断
- 失败时返回错误信息给用户

2.0.3: 改用 regex 过滤器 26.08.18

- 消息监听从 event_message_type 改为 @filter.regex，priority=1
- 不受 wake_prefix 约束，消息到达更可靠

2.0.2: 多别名+多会话通知 26.08.18

- 命令名称支持多个别名（如 运行/run/启动 均可触发）
- 通知接收支持绑定多个会话（better绑定 可多次执行）
- 帮助文本展示所有别名（用 / 分隔）

2.0.1: 修复热重载问题 26.08.18

- 修复热重载后 Webhook 服务器不启动（on_astrbot_loaded 改为 initialize）
- 默认 Webhook 路径从 /bettergi/webhook 改为 /bettergi

2.0.0: 完全重写插件 26.08.18

- 新增 Webhook 事件接收与转发（aiohttp HTTP 服务器）
- 新增本地/远程双模式（配置切换）
- 新增辅助程序（FastAPI，支持开机自启）
- 新增命令名称自定义（每个命令可配置触发词）
- 新增事件类型勾选配置（22种 BetterGI 事件）
- 新增辅助程序命令校验（仅允许 --startOneDragon/--startGroups）
- 重写定时任务（修复重复触发 BUG）
- 简化命令配置（template_list 模板，只需填配置名）
- 权限检查读取 AstrBot 管理员列表作为默认授权
- 删除消息撤回、截图、队列管理功能
