import os
from typing import Any

from astrbot.api import AstrBotConfig, logger
from astrbot.api.event import AstrMessageEvent, MessageEventResult, filter
from astrbot.api.star import Context, Star, register
from astrbot.core.message.message_event_result import MessageChain

from .service import (
    EventStore,
    Scheduler,
    WebhookServer,
    build_command,
    create_runner,
)


@register("bettergi", "BetterGI", "BetterGI 远程控制插件", "2.0.2")
class BetterGIPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self._init_config()

        self._event_store = EventStore(self._get_data_dir())
        self._runner = create_runner(config)
        self._scheduler = Scheduler()

        webhook_cfg = config.get("webhook", {})
        self._webhook_server = WebhookServer(
            host=webhook_cfg.get("host", "0.0.0.0"),
            port=webhook_cfg.get("port", 8088),
            path=webhook_cfg.get("path", "/bettergi/webhook"),
            token=webhook_cfg.get("token", ""),
        )
        self._webhook_server.set_handler(self._on_webhook_event)

        raw_umo = config.get("notify", {}).get("umo", [])
        if isinstance(raw_umo, str):
            self._notify_umo: list[str] = [raw_umo] if raw_umo else []
        else:
            self._notify_umo = [str(u) for u in raw_umo] if raw_umo else []
        self._event_map: dict[str, str] = {
            "notify.test": "notify_test",
            "dragon.start": "dragon_start",
            "dragon.end": "dragon_end",
            "group.start": "group_start",
            "group.end": "group_end",
            "task.cancel": "task_cancel",
            "task.error": "task_error",
            "domain.start": "domain_start",
            "domain.end": "domain_end",
            "domain.reward": "domain_reward",
            "domain.retry": "domain_retry",
            "tcg.start": "tcg_start",
            "tcg.end": "tcg_end",
            "album.start": "album_start",
            "album.end": "album_end",
            "album.error": "album_error",
            "daily.reward": "daily_reward",
            "autoeat.start": "autoeat_start",
            "autoeat.end": "autoeat_end",
            "autoeat.info": "autoeat_info",
            "js.custom": "js_custom",
            "js.error": "js_error",
        }

    def _init_config(self) -> None:
        self._cmd_names = self.config.get("command_names", {})
        self._prefix = self._cmd_names.get("prefix", "better")

        def _get_aliases(key: str, default: str) -> list[str]:
            val = self._cmd_names.get(key, [default])
            if isinstance(val, str):
                return [val]
            return [str(v) for v in val] if val else [default]

        self._cmd_run = _get_aliases("run", "运行")
        self._cmd_status = _get_aliases("status", "状态")
        self._cmd_stop = _get_aliases("stop", "停止")
        self._cmd_log = _get_aliases("log", "日志")
        self._cmd_bind = _get_aliases("bind", "绑定")
        self._cmd_help = _get_aliases("help", "帮助")
        self._debug = self.config.get("debug_log", False)

    def _get_data_dir(self) -> str:
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path

            return os.path.join(get_astrbot_data_path(), "plugin_data", "bettergi")
        except Exception:
            return ""

    def _check_permission(self, event: AstrMessageEvent) -> bool:
        sender_id = str(event.get_sender_id())

        try:
            bot_admins = self.context.get_bot_config().get("admins_id", [])
        except Exception:
            bot_admins = []

        if sender_id in [str(a) for a in bot_admins]:
            return True

        masters = self.config.get("better_master", [])
        if not masters:
            return True
        return sender_id in [str(m) for m in masters]

    def _get_commands(self) -> list[dict[str, Any]]:
        """获取配置的命令列表。"""
        return self.config.get("commands", [])

    def _match_cmd(self, sub: str, aliases: list[str]) -> str | None:
        """匹配命令别名，返回匹配到的别名（长别名优先），未匹配返回 None。"""
        for alias in sorted(aliases, key=len, reverse=True):
            if sub.startswith(alias):
                return alias
        return None

    def _get_command_at(self, index: int) -> list[str] | None:
        """根据序号获取命令参数。"""
        commands = self._get_commands()
        if not commands or index < 1 or index > len(commands):
            return None
        entry = commands[index - 1]
        template_key = entry.get("__template_key", "dragon")
        config_name = entry.get("config_name", "")
        return build_command(template_key, config_name)

    async def initialize(self):
        """插件加载和热重载时启动服务。"""
        webhook_cfg = self.config.get("webhook", {})
        if webhook_cfg.get("enable", True):
            ok = await self._webhook_server.start()
            if not ok:
                logger.warning("[BetterGI] Webhook 服务器启动失败，请检查端口配置")

        scheduled = self.config.get("scheduled_task", {})
        if scheduled.get("enable", False):
            self._scheduler.start(self._run_scheduled_task)

        logger.info("[BetterGI] 插件已加载")

    @filter.event_message_type(filter.EventMessageType.ALL)
    async def on_message(self, event: AstrMessageEvent):
        """监听所有消息，手动解析自定义命令。"""
        if not self._check_permission(event):
            return

        msg = event.message_str.strip()
        prefix = self._prefix

        if not msg.startswith(prefix):
            return

        sub = msg[len(prefix) :].strip()
        if not sub:
            return

        event.stop_event()

        matched = self._match_cmd(sub, self._cmd_run)
        if matched is not None:
            cmd_arg = sub[len(matched) :].strip()
            async for result in self._handle_run(event, cmd_arg):
                yield result
            return

        matched = self._match_cmd(sub, self._cmd_status)
        if matched is not None:
            async for result in self._handle_status(event):
                yield result
            return

        matched = self._match_cmd(sub, self._cmd_stop)
        if matched is not None:
            async for result in self._handle_stop(event):
                yield result
            return

        matched = self._match_cmd(sub, self._cmd_log)
        if matched is not None:
            cmd_arg = sub[len(matched) :].strip()
            yield await self._handle_log(event, cmd_arg)
            return

        matched = self._match_cmd(sub, self._cmd_bind)
        if matched is not None:
            yield await self._handle_bind(event)
            return

        matched = self._match_cmd(sub, self._cmd_help)
        if matched is not None:
            yield event.plain_result(self._build_help_text())

    async def _handle_run(self, event: AstrMessageEvent, arg: str):
        commands = self._get_commands()

        if not commands:
            yield event.plain_result("❌ 未配置任何命令，请先在配置中添加")
            return

        if arg in ("", "默认", "default"):
            args = self._get_command_at(1)
            if args:
                yield event.plain_result(f"✅ 正在执行第1个命令: {' '.join(args)}")
                await self._execute_command(args)
            else:
                yield event.plain_result("❌ 无法解析第1个命令配置")
            return

        if arg in ("选择", "select", "列表", "list"):
            msg_lines = ["📋 可用命令列表："]
            for i, cmd in enumerate(commands, 1):
                key = cmd.get("__template_key", "unknown")
                name = cmd.get("config_name", "未命名")
                type_name = "一条龙" if key == "dragon" else "调度器"
                msg_lines.append(f"{i}. [{type_name}] {name}")
            msg_lines.append(f"\n发送 {self._prefix}{self._cmd_run[0]} 序号 来执行")
            yield event.plain_result("\n".join(msg_lines))
            return

        if arg.isdigit():
            index = int(arg)
            args = self._get_command_at(index)
            if args:
                yield event.plain_result(
                    f"✅ 正在执行第{index}个命令: {' '.join(args)}"
                )
                await self._execute_command(args)
            else:
                yield event.plain_result(
                    f"❌ 无效的序号，请输入 1-{len(commands)} 之间的数字"
                )
            return

        if arg.startswith("--"):
            yield event.plain_result("❌ 请使用命令配置列表，无需手动填写命令参数")
            return

        yield event.plain_result(
            f"发送 {self._prefix}{self._cmd_run[0]} 选择 查看可用命令"
        )

    async def _execute_command(self, args: list[str]) -> None:
        """执行命令的实际逻辑。"""
        ok, msg = await self._runner.check_env()
        if not ok:
            logger.error(f"[BetterGI] 环境检查失败: {msg}")
            return

        success = await self._runner.run(args)
        if not success:
            logger.error(f"[BetterGI] 命令执行失败: {' '.join(args)}")

    async def _handle_stop(self, event: AstrMessageEvent):
        stopped = await self._runner.stop()
        if stopped:
            yield event.plain_result("✅ BetterGI 任务已停止")
        else:
            yield event.plain_result("❌ 当前没有正在运行的任务")

    async def _handle_status(self, event: AstrMessageEvent):
        status = await self._runner.get_status()
        sched_status = self._scheduler.get_status()

        lines = ["📊 BetterGI 状态：", ""]

        mode = status.get("mode", "local")
        mode_name = "本地" if mode == "local" else "远程"
        lines.append(f"🔹 运行模式: {mode_name}")

        is_running = status.get("is_running", False)
        lines.append(f"🔹 运行状态: {'运行中' if is_running else '空闲'}")

        current = status.get("current_command", "")
        if current:
            lines.append(f"🔹 当前命令: {current}")

        if status.get("pid"):
            lines.append(f"🔹 进程PID: {status['pid']}")

        webhook_running = self._webhook_server.is_running
        lines.append(f"🔹 Webhook: {'已启动' if webhook_running else '未启动'}")
        if webhook_running:
            lines.append(f"  地址: {self._webhook_server.webhook_url}")

        lines.append(
            f"🔹 定时任务: {'已启用' if sched_status.get('is_running') else '未启用'}"
        )
        if sched_status.get("last_run_date"):
            lines.append(f"  上次执行: {sched_status['last_run_date']}")

        if self._notify_umo:
            lines.append(f"🔹 通知绑定: 已绑定 {len(self._notify_umo)} 个会话")

        yield event.plain_result("\n".join(lines))

    async def _handle_log(
        self, event: AstrMessageEvent, arg: str
    ) -> MessageEventResult:
        if arg in ("清除", "清空", "clear"):
            count = await self._event_store.clear()
            return event.plain_result(f"✅ 已清除 {count} 条事件记录")

        count = 10
        if arg.isdigit():
            count = int(arg)

        events = await self._event_store.get_recent(count)
        text = EventStore.format_events(events)
        return event.plain_result(text)

    async def _handle_bind(self, event: AstrMessageEvent) -> MessageEventResult:
        umo = event.unified_msg_origin

        if umo in self._notify_umo:
            return event.plain_result("✅ 当前会话已绑定，无需重复绑定")

        self._notify_umo.append(umo)

        try:
            self.config["notify"]["umo"] = self._notify_umo
            self.config.save_config()
        except Exception as e:
            logger.warning(f"[BetterGI] 保存绑定配置失败: {e}")

        return event.plain_result(
            f"✅ 已绑定当前会话为通知接收方\n"
            f"BetterGI 事件将自动转发到此处\n"
            f"当前共绑定 {len(self._notify_umo)} 个会话"
        )

    async def _on_webhook_event(self, event_data: dict) -> None:
        """处理 BetterGI Webhook 事件。"""
        await self._event_store.add(event_data)

        event_type = event_data.get("event", "")
        message = event_data.get("message", "")
        result = event_data.get("result", "")
        timestamp = event_data.get("timestamp", "")

        logger.info(f"[BetterGI] Webhook 事件: {event_type} | {result} | {message}")

        if self._notify_umo and self._should_notify(event_type):
            await self._send_notify(event_type, result, message, timestamp)

    def _should_notify(self, event_type: str) -> bool:
        events_cfg = self.config.get("notify", {}).get("events", {})
        config_key = self._event_map.get(event_type)
        if not config_key:
            return False
        return events_cfg.get(config_key, False)

    async def _send_notify(
        self, event_type: str, result: str, message: str, timestamp: str
    ) -> None:
        """发送事件通知到所有绑定的会话。"""
        text = (
            f"📢 BetterGI 事件通知\n"
            f"事件: {event_type}\n"
            f"结果: {result}\n"
            f"时间: {timestamp}\n"
        )
        if message:
            text += f"消息: {message}"

        chain = MessageChain().message(text)
        for umo in self._notify_umo:
            try:
                await self.context.send_message(umo, chain)
            except Exception as e:
                logger.error(f"[BetterGI] 发送通知到 {umo} 失败: {e}")

    async def _run_scheduled_task(self) -> None:
        """定时任务执行函数。"""
        scheduled = self.config.get("scheduled_task", {})
        index = scheduled.get("command_index", 1)

        args = self._get_command_at(index)
        if not args:
            logger.error(f"[BetterGI] 定时任务: 无法找到第{index}个命令配置")
            return

        logger.info(f"[BetterGI] 定时任务执行: {' '.join(args)}")
        await self._execute_command(args)

    def _build_help_text(self) -> str:
        p = self._prefix

        def _alias_str(aliases: list[str]) -> str:
            return "/".join(aliases) if aliases else ""

        run = _alias_str(self._cmd_run)
        status = _alias_str(self._cmd_status)
        stop = _alias_str(self._cmd_stop)
        log = _alias_str(self._cmd_log)
        bind = _alias_str(self._cmd_bind)
        help_cmd = _alias_str(self._cmd_help)
        return (
            f"BetterGI 远程控制插件 使用帮助\n\n"
            f"📌 命令列表（前缀: {p}）：\n"
            f"  {p}{run}        - 运行第1个命令\n"
            f"  {p}{run} 选择   - 查看可用命令列表\n"
            f"  {p}{run} 序号   - 运行指定序号的命令\n"
            f"  {p}{status}      - 查看运行状态\n"
            f"  {p}{stop}        - 停止当前任务\n"
            f"  {p}{log}        - 查看最近事件日志\n"
            f"  {p}{log} 清除   - 清除事件记录\n"
            f"  {p}{bind}        - 绑定通知会话\n"
            f"  {p}{help_cmd}        - 显示此帮助\n\n"
            f"📌 Webhook 配置：\n"
            f"  在 BetterGI 设置中配置 Webhook 地址为：\n"
            f"  {self._webhook_server.webhook_url}\n\n"
            f"📌 事件类型参考：\n"
            f"  dragon.start/end - 一条龙启动/结束\n"
            f"  group.start/end  - 配置组启动/结束\n"
            f"  task.cancel/error - 任务取消/错误\n"
            f"  domain.start/end/reward/retry - 秘境相关\n"
            f"  tcg.start/end - 七圣召唤启动/结束\n"
            f"  album.start/end/error - 音游相关\n"
            f"  autoeat.start/end/info - 自动吃药\n"
            f"  daily.reward - 每日奖励状态\n"
            f"  js.custom/error - JS自定义/错误\n"
            f"  notify.test - 测试通知"
        )

    async def terminate(self):
        """插件卸载时清理资源。"""
        logger.info("[BetterGI] 插件正在卸载...")
        await self._scheduler.stop()
        await self._webhook_server.stop()
        await self._runner.cleanup()
        logger.info("[BetterGI] 插件已卸载")
