import os
import sys
import asyncio
import time
from asyncio import sleep
from typing import Optional, List, Dict, Any, Tuple
from PIL import ImageGrab

from astrbot.api.event import filter, AstrMessageEvent, MessageEventResult
from astrbot.api.star import Context, Star, register
from astrbot.api import logger, AstrBotConfig
from astrbot.core.message.message_event_result import MessageChain

from .service.bettergiService import bettergi_service, running_processes, BettergiService



async def _send_with_recall(self, event: AstrMessageEvent, message_chain: MessageChain) -> None:
    try:
        from .service.recall import recall_send
        delay = self.config.get("bettergi_recall_delay", 60)
        logger.debug(f"[recall] 发送消息，撤回延迟: {delay}秒")
        await recall_send(delay, event, message_chain)
    except Exception as e:
        logger.error(f"[recall] 发送消息失败: {e}")
        await event.send(message_chain)


@register("bettergi", "BetterGI", "BetterGI 远程控制插件", "1.0.0")
class BetterGIPlugin(Star):
    def __init__(self, context: Context, config: AstrBotConfig):
        super().__init__(context)
        self.config = config
        self.bettergi_config = self._get_bettergi_config()
        self.bettergi_user_state = {}
        
        self._initialize_plugin()
    
    def _get_bettergi_config(self) -> Dict[str, Any]:
        config = self.config
        if not config:
            return {
                "base": {},
                "scheduled_task": {},
                "manual_trigger": {},
                "better_master": [],
                "help_text": "帮助信息未配置"
            }
        
        return {
            "base": config.get("base", {}),
            "scheduled_task": config.get("scheduled_task", {}),
            "manual_trigger": config.get("manual_trigger", {}),
            "better_master": config.get("better_master", []),
            "help_text": config.get("help_text", "帮助信息未配置")
        }
    
    def _initialize_plugin(self) -> None:
        try:
            self._cleanup_screenshots()
            
            bettergi_config = self.bettergi_config
            base_config = bettergi_config["base"]
            scheduled_config = bettergi_config["scheduled_task"]
            
            bettergi_dir = base_config.get("bettergi_dir", "未配置")
            debug_log = base_config.get("debug_log", False)
            default_command = base_config.get("default_command", "startOneDragon")
            enable_schedule = scheduled_config.get("enable", False)
            
            logger.info("[BetterGI] 插件已加载")
            logger.info(f"[BetterGI] BetterGI 路径: {bettergi_dir}")
            logger.info(f"[BetterGI] 默认命令: {default_command}")
            logger.info(f"[BetterGI] 定时任务: {'开启' if enable_schedule else '关闭'}")
            
            if enable_schedule:
                logger.info("[BetterGI] 定时任务已启用")
                bettergi_service.start_scheduled_task(self.context, self.config)
                
        except Exception as e:
            logger.error(f"[BetterGI] 初始化失败: {e}", exc_info=True)
    
    def _cleanup_screenshots(self, max_age_days: int = 7) -> None:
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
            from pathlib import Path
            save_dir = Path(get_astrbot_data_path()) / "plugin_data" / "bettergi" / "screenshots"
            save_dir.mkdir(parents=True, exist_ok=True)
            
            max_age_seconds = max_age_days * 24 * 60 * 60
            current_time = time.time()
            
            for filename in os.listdir(save_dir):
                file_path = os.path.join(save_dir, filename)
                
                if not filename.startswith("status_") or not filename.endswith(".png"):
                    continue
                    
                file_mtime = os.path.getmtime(file_path)
                
                if current_time - file_mtime > max_age_seconds:
                    os.remove(file_path)
                    logger.debug(f"[BetterGI] 已删除旧截图: {filename}")
                    
        except Exception as e:
            logger.error(f"[BetterGI] 清理截图失败: {e}", exc_info=True)
    
    def _validate_config(self) -> Tuple[bool, str]:
        base_config = self.bettergi_config["base"]
        
        bettergi_dir = base_config.get("bettergi_dir", "")
        if not bettergi_dir:
            return False, "BetterGI路径未配置"
            
        if not os.path.exists(bettergi_dir):
            return False, f"BetterGI 目录不存在: {bettergi_dir}"
            
        bettergi_exe = os.path.join(bettergi_dir, "BetterGI.exe")
        if not os.path.exists(bettergi_exe):
            return False, f"BetterGI 可执行文件不存在: {bettergi_exe}"
            
        return True, ""
    
    def _check_permission(self, event: AstrMessageEvent) -> bool:
        better_master = self.bettergi_config.get("better_master", [])
        user_id = event.get_sender_id()
        logger.info(f"[BetterGI] 检查权限 - 用户ID: {user_id}, 管理员列表: {better_master}")
        if not better_master:
            return True
        return user_id in better_master
    
    @filter.command("better")
    async def better_command(self, event: AstrMessageEvent):
        """BetterGI 主命令
        
        better运行 - 运行默认命令
        better运行 1 - 运行配置中的第1个命令
        better状态 - 查看当前状态
        better停止 - 停止当前任务
        better帮助 - 显示帮助信息
        """
        if not self._check_permission(event):
            return
        
        event.stop_event()
        
        msg = event.message_str.strip()
        if msg.startswith("better"):
            msg = msg[len("better"):].strip()
        
        logger.info(f"[BetterGI] 处理命令: '{msg}'")
        
        async for result in self._route_command(event, msg):
            yield result
    
    async def _route_command(self, event: AstrMessageEvent, msg: str):
        if msg.startswith("运行") or msg.startswith("run") or msg.startswith("启动"):
            async for result in self._handle_run_command(event, msg):
                yield result
        elif msg in ["帮助", "help", "使用帮助"]:
            help_text = self.bettergi_config.get("help_text", "帮助信息未配置").strip()
            await _send_with_recall(self, event, MessageChain().message(help_text))
        elif msg in ["停止", "stop", "终止", "结束"]:
            result = await bettergi_service.stop_task(self.context)
            if result:
                yield event.plain_result("✅ BetterGI 任务已停止")
            else:
                yield event.plain_result("❌ 当前没有正在运行的 BetterGI 任务")
        elif msg in ["状态", "status", "信息", "info"]:
            async for result in self._handle_status_command(event):
                yield result
        elif msg.startswith("删除") or msg.startswith("remove") or msg.startswith("rm"):
            yield await self._handle_remove_command(event, msg)
    
    async def _handle_run_command(self, event: AstrMessageEvent, msg: str):
        try:
            cmd_prefix = None
            for prefix in ["运行", "run", "启动"]:
                if msg.startswith(prefix):
                    cmd_prefix = prefix
                    break
            
            if not cmd_prefix:
                return
                
            command_part = msg[len(cmd_prefix):].strip()
            commands_to_run = await self._parse_command_part(event, command_part)
            
            if not commands_to_run:
                return
            
            is_valid, error_msg = self._validate_config()
            if not is_valid:
                yield event.plain_result(f"❌ {error_msg}")
                return
            
            result = await bettergi_service.run_bettergi(self.context, self.config, commands_to_run)
            
            if result:
                yield event.plain_result(f"✅ 命令已加入队列: {'、'.join(commands_to_run)}")
            else:
                yield event.plain_result(f"❌ BetterGI 运行失败，命令: {'、'.join(commands_to_run)}")
                
        except Exception as e:
            logger.error(f"[BetterGI] 运行命令失败: {e}", exc_info=True)
            yield event.plain_result(f"❌ 运行出错: {str(e)}")
    
    async def _parse_command_part(self, event: AstrMessageEvent, command_part: str) -> Optional[List[str]]:
        try:
            user_id = event.get_sender_id()
            manual_commands = self.bettergi_config["manual_trigger"].get("command", None)
            
            if user_id in self.bettergi_user_state and self.bettergi_user_state[user_id] == 'selecting_command':
                return await self._handle_command_selection(event, manual_commands, command_part, user_id)
            
            if command_part.lower() == "select" or command_part == "选择":
                return await self._handle_command_list_request(event, manual_commands, user_id)
            
            if command_part.isdigit() and manual_commands and isinstance(manual_commands, list):
                return await self._handle_direct_index_selection(event, manual_commands, command_part)
            
            if command_part:
                logger.info(f"[BetterGI] 自定义命令: {command_part}")
                return [command_part]
            
            default_command = self.bettergi_config["base"].get("default_command", "startOneDragon")
            logger.info(f"[BetterGI] 使用默认命令: {default_command}")
            return default_command if isinstance(default_command, list) else [default_command]
            
        except Exception as e:
            logger.error(f"[BetterGI] 解析命令失败: {e}", exc_info=True)
            return None
    
    async def _handle_command_selection(self, event: AstrMessageEvent, manual_commands: Optional[List[str]], 
                                       command_part: str, user_id: str) -> Optional[List[str]]:
        if user_id in self.bettergi_user_state:
            del self.bettergi_user_state[user_id]
        
        index_str = command_part[2:].strip() if command_part.startswith("选择") or command_part.startswith("select") else command_part.strip()
        
        try:
            index = int(index_str) - 1
            if manual_commands and isinstance(manual_commands, list) and 0 <= index < len(manual_commands):
                return [manual_commands[index]]
            else:
                await event.send(MessageChain().message(f"无效的命令索引，请输入 1-{len(manual_commands)} 之间的数字"))
        except ValueError:
            await event.send(MessageChain().message("请输入有效的数字索引"))
        return None
    
    async def _handle_command_list_request(self, event: AstrMessageEvent, manual_commands: Optional[List[str]], 
                                          user_id: str) -> Optional[List[str]]:
        if manual_commands and isinstance(manual_commands, list) and manual_commands:
            msg = "📋 可用命令列表：\n" + "\n".join([f"{i}. {cmd}" for i, cmd in enumerate(manual_commands, 1)])
            msg += "\n\n请输入序号选择要执行的命令"
            
            self.bettergi_user_state[user_id] = 'selecting_command'
            await _send_with_recall(self, event, MessageChain().message(msg))
        else:
            await event.send(MessageChain().message("配置中没有设置命令列表"))
        return None
    
    async def _handle_direct_index_selection(self, event: AstrMessageEvent, manual_commands: List[str], 
                                            command_part: str) -> Optional[List[str]]:
        try:
            index = int(command_part) - 1
            if 0 <= index < len(manual_commands):
                return [manual_commands[index]]
            else:
                await event.send(MessageChain().message(f"无效的命令索引，请输入 1-{len(manual_commands)} 之间的数字"))
        except Exception as e:
            logger.error(f"[BetterGI] 索引选择失败: {e}", exc_info=True)
        return None
    
    async def _handle_remove_command(self, event: AstrMessageEvent, msg: str) -> MessageEventResult:
        try:
            cmd_prefix = None
            for prefix in ["删除", "remove", "rm"]:
                if msg.startswith(prefix):
                    cmd_prefix = prefix
                    break
            
            if not cmd_prefix:
                return event.plain_result("❌ 无效的删除命令格式")
                
            params = msg[len(cmd_prefix):].strip()
            
            if not params:
                return event.plain_result("❌ 请指定要删除的命令或输入'list'查看队列")
            
            if params.lower() == "list" or params == "列表":
                queue_list = await self._show_queue_list()
                return event.plain_result(queue_list + "\n输入 'better删除 索引' 删除对应命令")
            elif params.isdigit():
                return await self._remove_by_index(event, int(params))
            else:
                return await self._remove_by_command(event, params)
                
        except Exception as e:
            logger.error(f"[BetterGI] 删除命令失败: {e}", exc_info=True)
            return event.plain_result(f"❌ 删除出错: {str(e)}")
    
    async def _show_queue_list(self) -> str:
        try:
            queue_commands = await bettergi_service.get_queue_commands()
            
            if not queue_commands:
                return "📋 当前队列为空"
            
            return "📋 当前队列命令列表：\n" + "\n".join([f"{cmd['index']}. {cmd['command']}" for cmd in queue_commands])
        except Exception as e:
            logger.error(f"[BetterGI] 获取队列失败: {e}", exc_info=True)
            return f"❌ 获取队列列表失败: {str(e)}"
    
    async def _remove_by_index(self, event: AstrMessageEvent, index: int) -> MessageEventResult:
        try:
            queue_commands = await bettergi_service.get_queue_commands()
            
            if not queue_commands:
                return event.plain_result("📋 当前队列为空")
            
            if 1 <= index <= len(queue_commands):
                command_to_remove = queue_commands[index-1]["command"]
                removed_count = await bettergi_service.remove_command_from_queue(command_to_remove)
                if removed_count > 0:
                    return event.plain_result(f"✅ 成功从队列中删除命令: {command_to_remove}")
                else:
                    return event.plain_result("❌ 删除失败，命令可能已被处理")
            else:
                return event.plain_result(f"❌ 索引无效，有效索引范围是 1-{len(queue_commands)}")
        except Exception as e:
            logger.error(f"[BetterGI] 删除命令失败: {e}", exc_info=True)
            return event.plain_result(f"❌ 删除命令失败: {str(e)}")
    
    async def _remove_by_command(self, event: AstrMessageEvent, command_to_remove: str) -> MessageEventResult:
        removed_count = await bettergi_service.remove_command_from_queue(command_to_remove)
        
        if removed_count > 0:
            return event.plain_result(f"✅ 成功从队列中删除 {removed_count} 个 '{command_to_remove}' 命令")
        else:
            return event.plain_result(f"❌ 队列中未找到命令 '{command_to_remove}'")
    
    async def _handle_status_command(self, event: AstrMessageEvent):
        try:
            base_config = self.bettergi_config["base"]
            scheduled_config = self.bettergi_config["scheduled_task"]
            manual_enabled = self.bettergi_config["manual_trigger"].get("enable", True)
            
            status_info = await self._get_status(base_config, scheduled_config)
            message = self._build_status_message(status_info, base_config, scheduled_config, manual_enabled)
            picture_path = await self._build_status_picture()
            
            from astrbot.api.message_components import Image, Plain
            
            if picture_path and os.path.exists(picture_path):
                await _send_with_recall(self, event, MessageChain([Plain(message), Image(file=picture_path)]))
            else:
                await _send_with_recall(self, event, MessageChain().message(message))
                
        except Exception as e:
            logger.error(f"[BetterGI] 获取状态失败: {e}", exc_info=True)
            yield event.plain_result(f"❌ 获取状态信息失败: {str(e)}")
    
    async def _get_status(self, base_config: Dict[str, Any], scheduled_config: Dict[str, Any]) -> Dict[str, Any]:
        try:
            queue_status = await bettergi_service.get_status()
            queue_list = await self._show_queue_list()
            
            bettergi_dir = base_config.get("bettergi_dir", "")
            dir_exists = os.path.exists(bettergi_dir) if bettergi_dir else False
            exe_exists = os.path.exists(os.path.join(bettergi_dir, "BetterGI.exe")) if dir_exists else False
            
            return {
                "queue_enabled": queue_status.get('queue_enabled', False),
                "queue_size": queue_status.get('queue_size', 0),
                "current_command": queue_status.get('current_command', None),
                "scheduled_running": bettergi_service.task_running,
                "running_processes_count": len(running_processes),
                "active_tasks_count": len(getattr(bettergi_service, 'active_tasks', [])),
                "queue_list": queue_list,
                "dir_exists": dir_exists,
                "exe_exists": exe_exists,
            }
        except Exception as e:
            logger.error(f"[BetterGI] 收集状态失败: {e}", exc_info=True)
            return {
                "queue_enabled": False,
                "queue_size": 0,
                "current_command": None,
                "scheduled_running": False,
                "running_processes_count": 0,
                "active_tasks_count": 0,
                "queue_list": "❌ 获取队列信息失败",
                "dir_exists": False,
                "exe_exists": False,
            }
    
    async def _build_status_picture(self) -> str:
        try:
            from astrbot.core.utils.astrbot_path import get_astrbot_data_path
            from pathlib import Path
            save_dir = Path(get_astrbot_data_path()) / "plugin_data" / "bettergi" / "screenshots"
            save_dir.mkdir(parents=True, exist_ok=True)
            
            image = ImageGrab.grab()
            save_path = save_dir / f"status_{int(time.time())}.png"
            image.save(save_path)
            return str(save_path)
        except Exception as e:
            logger.error(f"[BetterGI] 截图失败: {e}")
            return ""
    
    def _build_status_message(self, status_info: Dict[str, Any], base_config: Dict[str, Any], 
                              scheduled_config: Dict[str, Any], manual_enabled: bool) -> str:
        try:
            msg = f"""📊 BetterGI 当前状态：

🔹 运行状态
   当前执行命令: {status_info["current_command"] or "无"}
   {status_info["queue_list"]}
🔹 当前屏幕内容截图"""
        
            if not status_info["dir_exists"]:
                msg += "\n\n⚠️ 警告：BetterGI目录不存在"
            elif not status_info["exe_exists"]:
                msg += "\n\n⚠️ 警告：BetterGI可执行文件不存在"
        
            return msg
        except Exception as e:
            logger.error(f"[BetterGI] 构建状态消息失败: {e}", exc_info=True)
            return f"❌ 构建状态消息失败: {str(e)}"
    
    async def terminate(self):
        logger.info("[BetterGI] 插件正在卸载...")
        await bettergi_service.stop_task(self.context)
        logger.info("[BetterGI] 插件已卸载")
