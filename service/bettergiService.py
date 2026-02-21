"""
BetterGI 服务模块

本模块提供 BetterGI (Better Genshin Impact) 自动化工具的集成服务，
包括进程管理、命令执行、任务队列、定时任务等功能。

主要功能:
    - 异步命令执行与进程隔离
    - 任务队列管理（支持去重）
    - 定时任务调度
    - 进程监控与强制终止
    - 日志文件监控

作者: AstrBot Plugin
"""

import os
import subprocess
import asyncio
import psutil
from datetime import datetime
import logging
import ctypes
import time
import glob
import re
from typing import Optional, Union, Dict, Any, List

try:
    from astrbot.api import logger
except ImportError:
    logger = logging.getLogger("bettergi")

running_processes: Dict[int, asyncio.subprocess.Process] = {}


async def _execute_with_debug_log(
    command: Union[str, List[str]], 
    cwd: Optional[str], 
    env: Dict[str, str]
) -> asyncio.subprocess.Process:
    """
    执行命令并记录调试日志。
    
    Args:
        command: 要执行的命令，可以是字符串或列表
        cwd: 工作目录
        env: 环境变量字典
        
    Returns:
        启动的异步进程对象
    """
    command_str = ' '.join(command) if isinstance(command, list) else command
    logger.debug(f"在Windows系统上执行命令: {command_str}, 工作目录: {cwd}")
    
    if cwd and os.path.exists(cwd):
        files_in_dir = os.listdir(cwd)
        exe_files = [f for f in files_in_dir if f.lower().startswith('bettergi')]
        logger.debug(f"工作目录中的BetterGI相关文件: {exe_files}")
    
    if isinstance(command, list):
        process = await asyncio.create_subprocess_exec(
            *command,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
    else:
        process = await asyncio.create_subprocess_shell(
            command_str,
            cwd=cwd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env
        )
    
    logger.debug(f"进程已启动，PID: {process.pid}")
    return process


async def _execute_with_pipe_output(
    process: asyncio.subprocess.Process, 
    command_str: str, 
    timeout: int
) -> Dict[str, Any]:
    """
    等待进程完成并捕获输出。
    
    Args:
        process: 异步进程对象
        command_str: 命令字符串（用于日志）
        timeout: 超时时间（秒）
        
    Returns:
        包含执行结果的字典:
            - success: 是否成功
            - returncode: 返回码
            - stdout: 标准输出
            - stderr: 标准错误
    """
    try:
        try:
            stdout, stderr = await asyncio.wait_for(
                asyncio.gather(process.stdout.read(), process.stderr.read()),
                timeout=timeout
            )
        except asyncio.CancelledError:
            logger.debug(f"命令执行任务被取消: {command_str}")
            try:
                process.kill()
                await asyncio.wait_for(process.wait(), timeout=5)
            except Exception:
                pass
            raise
        
        stdout_str = stdout.decode('utf-8', errors='replace') if stdout else ''
        stderr_str = stderr.decode('utf-8', errors='replace') if stderr else ''
        
        await process.wait()
        
        logger.debug(f"进程执行完成，返回码: {process.returncode}")
        
        result = {
            "success": process.returncode == 0,
            "returncode": process.returncode,
            "stdout": stdout_str,
            "stderr": stderr_str
        }
        
        if result["success"]:
            logger.info(f"命令执行成功: {command_str}")
        else:
            logger.warning(f"命令执行失败: {command_str}, 返回码: {process.returncode}")
        
        return result
        
    except asyncio.TimeoutError:
        logger.error(f"命令执行超时 ({timeout} 秒): {command_str}")
        try:
            process.kill()
            await asyncio.wait_for(process.wait(), timeout=5)
        except Exception:
            pass
        
        return {
            "success": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"命令执行超时 ({timeout} 秒)"
        }


async def execute_command_isolated(
    command: Union[str, List[str]], 
    cwd: Optional[str] = None, 
    timeout: int = 18000
) -> Dict[str, Any]:
    """
    在隔离环境中执行命令。
    
    清除 PYTHONPATH 和 PYTHONHOME 环境变量，避免与宿主环境冲突。
    同时启动日志监控任务，检测进程是否卡住。
    
    Args:
        command: 要执行的命令
        cwd: 工作目录
        timeout: 超时时间（秒），默认5小时
        
    Returns:
        执行结果字典
    """
    command_str = ' '.join(command) if isinstance(command, list) else command
    cmd_str = command_str
    
    logger.info(f"开始执行命令: {cmd_str}, 工作目录: {cwd}, 超时时间: {timeout}秒")
    
    env = os.environ.copy()
    env.pop('PYTHONPATH', None)
    env.pop('PYTHONHOME', None)
    
    if cwd and not os.path.exists(cwd):
        logger.error(f"工作目录不存在: {cwd}")
        return {
            "success": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"工作目录不存在: {cwd}"
        }
    
    process = None
    log_monitor_task = None
    
    try:
        process = await _execute_with_debug_log(command, cwd, env)
        
        running_processes[process.pid] = process
        
        if cwd:
            log_monitor_task = asyncio.create_task(_monitor_bettergi_log(cwd, process))
        
        result = await _execute_with_pipe_output(process, cmd_str, timeout)
        return result
            
    except PermissionError as e:
        logger.error(f"权限不足，无法执行命令: {cmd_str}")
        return {
            "success": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"权限不足: {str(e)}。请确保您有足够的权限执行此文件，或尝试以管理员身份运行程序。"
        }
    except Exception as e:
        logger.exception(f"执行命令时发生未预期的错误: {cmd_str}")
        error_msg = str(e)
        if "系统找不到指定的文件" in error_msg and cwd:
            files_in_dir = os.listdir(cwd) if os.path.exists(cwd) else []
            exe_files = [f for f in files_in_dir if f.lower().startswith('bettergi')]
            error_msg += f"。工作目录({cwd})中的BetterGI相关文件: {exe_files}"
        return {
            "success": False,
            "returncode": -1,
            "stdout": "",
            "stderr": f"执行命令时发生错误: {error_msg}"
        }
    finally:
        if process and process.pid in running_processes:
            del running_processes[process.pid]
        
        if log_monitor_task and not log_monitor_task.done():
            log_monitor_task.cancel()
            try:
                await log_monitor_task
            except asyncio.CancelledError:
                pass


async def _monitor_bettergi_log(cwd: str, process: asyncio.subprocess.Process) -> None:
    """
    监控 BetterGI 日志文件，检测进程是否卡住。
    
    如果日志文件超过10秒没有更新，则认为进程卡住，自动终止。
    
    Args:
        cwd: BetterGI 工作目录
        process: 要监控的进程对象
    """
    log_file_pattern = os.path.join(cwd, "better-genshin-impact*.log")
    log_files = glob.glob(log_file_pattern)
    
    if not log_files:
        logger.debug("未找到BetterGI日志文件")
        return
    
    latest_log_file = max(log_files, key=os.path.getctime)
    logger.debug(f"监控日志文件: {latest_log_file}")
    
    last_position = os.path.getsize(latest_log_file) if os.path.exists(latest_log_file) else 0
    last_log_time = datetime.now()
    
    try:
        while process.returncode is None:
            if os.path.exists(latest_log_file):
                current_size = os.path.getsize(latest_log_file)
                if current_size > last_position:
                    with open(latest_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        f.seek(last_position)
                        new_content = f.read()
                        last_position = current_size
                        last_log_time = datetime.now()
            
            current_time = datetime.now()
            time_diff = (current_time - last_log_time).total_seconds()
            if time_diff > 10:
                logger.info(f"检测到日志长时间无更新（{time_diff:.1f}秒），正在终止BetterGI进程")
                try:
                    process.terminate()
                    await asyncio.wait_for(process.wait(), timeout=10)
                except asyncio.TimeoutError:
                    process.kill()
                    await process.wait()
                except Exception as e:
                    logger.error(f"终止进程时出错: {e}")
                break
            
            await asyncio.sleep(1)
    except Exception as e:
        logger.error(f"日志监控过程中出错: {e}")


def check_admin_rights() -> bool:
    """
    检查当前进程是否具有管理员权限。
    
    Returns:
        True 如果具有管理员权限，否则 False
    """
    try:
        return ctypes.windll.shell32.IsUserAnAdmin()
    except Exception:
        return False


class BettergiService:
    """
    BetterGI 服务类。
    
    提供 BetterGI 自动化工具的完整管理功能，包括:
    - 命令队列管理
    - 定时任务调度
    - 进程监控与终止
    - 任务状态查询
    
    Attributes:
        task_running: 定时任务是否正在运行
        running_task: 当前运行的定时任务
        active_tasks: 活动任务列表
        command_queue: 命令队列
        queue_processing: 队列是否正在处理
        queue_task: 队列处理任务
        queue_lock: 队列操作锁
        queue_commands: 队列中的命令集合（用于去重）
        current_command: 当前正在执行的命令
        config: 配置对象
    """
    
    def __init__(self):
        self.task_running = False
        self.running_task = None
        self.active_tasks = []
        self.command_queue = asyncio.Queue()
        self.queue_processing = False
        self.queue_task = None
        self.queue_lock = asyncio.Lock()
        self.queue_commands = set()
        self.current_command = None
        self.task_running_count = 0
        self.last_run_hour_start = None
        self.last_run_hour_end = None
        self.logger = logger
        self.config = None
    
    async def get_queue_commands(self) -> List[Dict[str, Any]]:
        """
        获取当前队列中的命令列表。
        
        Returns:
            命令列表，每个元素包含 index 和 command 字段
        """
        try:
            self.logger.info("[BetterGI-队列] 获取队列命令列表")
            
            queue_commands = []
            temp_queue = asyncio.Queue()
            index = 1
            
            while not self.command_queue.empty():
                try:
                    item = self.command_queue.get_nowait()
                    queue_commands.append({
                        "index": index,
                        "command": item["command"]
                    })
                    temp_queue.put_nowait(item)
                    index += 1
                except asyncio.QueueEmpty:
                    break
            
            while not temp_queue.empty():
                try:
                    item = temp_queue.get_nowait()
                    self.command_queue.put_nowait(item)
                except asyncio.QueueEmpty:
                    break
            
            self.logger.info(f"[BetterGI-队列] 当前队列包含 {len(queue_commands)} 个命令")
            return queue_commands
            
        except Exception as e:
            self.logger.error(f"[BetterGI-队列] 获取队列命令列表时发生错误: {e}")
            return []

    async def _process_command_queue(self, context, config):
        """
        处理命令队列的后台任务。
        
        持续从队列中取出命令并执行，支持回调通知。
        
        Args:
            context: 上下文对象
            config: 配置对象
        """
        self.logger.info("[BetterGI-队列] 开始处理命令队列")
        
        try:
            while self.queue_processing:
                try:
                    command_info = await self.command_queue.get()
                    command = command_info["command"]
                    bettergi_dir = command_info["bettergi_dir"]
                    debug_log = command_info["debug_log"]
                    callback = command_info.get("callback")
                    callback_args = command_info.get("callback_args", {})
                    
                    self.logger.info(f"[BetterGI-队列] 开始执行队列中的命令: {command}，队列剩余: {self.command_queue.qsize()}")
                    self.current_command = command
                    
                    success = await self._execute_single_command(bettergi_dir, command, debug_log)
                    
                    if callback:
                        try:
                            await callback(success=success, command=command, **callback_args)
                        except Exception as e:
                            self.logger.error(f"[BetterGI-队列] 执行回调时发生异常: {str(e)}")
                    
                except asyncio.CancelledError:
                    self.logger.info("[BetterGI-队列] 队列处理任务被取消")
                    break
                except Exception as e:
                    self.logger.error(f"[BetterGI-队列] 处理队列命令时发生异常: {str(e)}")
                    await asyncio.sleep(1)
                finally:
                    self.current_command = None
                    async with self.queue_lock:
                        self.queue_commands.discard(command)
                    self.command_queue.task_done()
        finally:
            async with self.queue_lock:
                self.queue_processing = False
                self.queue_commands.clear()
            self.logger.info("[BetterGI-队列] 命令队列处理已停止")
    
    async def run_bettergi(
        self, 
        context, 
        config=None, 
        custom_command=None, 
        callback=None, 
        **callback_args
    ) -> bool:
        """
        运行 BetterGI 任务。
        
        将命令加入队列执行，支持自定义命令和回调。
        相同命令会自动去重，不会重复添加到队列。
        
        Args:
            context: 上下文对象
            config: 配置对象
            custom_command: 自定义命令，可以是字符串或列表
            callback: 任务完成后的回调函数
            **callback_args: 回调函数的额外参数
            
        Returns:
            True 如果命令成功加入队列，否则 False
        """
        self.config = config
        
        try:
            self.logger.info("[BetterGI] 开始执行功能")
            
            base_config = config.get("base", {}) if config else {}
            bettergi_dir = base_config.get("bettergi_dir")
            debug_log = base_config.get("debug_log", False)
            
            if not bettergi_dir:
                self.logger.error("[BetterGI] 路径未配置")
                return False
            
            if not os.path.exists(bettergi_dir):
                self.logger.error(f"[BetterGI] 路径不存在: {bettergi_dir}")
                return False
            
            if custom_command:
                commands_to_execute = [custom_command] if isinstance(custom_command, str) else custom_command
            else:
                default_command = base_config.get("default_command", "startOneDragon")
                commands_to_execute = default_command if isinstance(default_command, list) else [default_command]
            
            async with self.queue_lock:
                if not self.queue_processing:
                    self.logger.info("[BetterGI-队列] 启动命令队列处理任务")
                    self.queue_processing = True
                    self.queue_task = asyncio.create_task(self._process_command_queue(context, config))
                    self.active_tasks.append(self.queue_task)
                    self.logger.info("[BetterGI-队列] 命令队列处理任务已启动")
            
            added_count = 0
            skipped_count = 0
            
            for command in commands_to_execute:
                async with self.queue_lock:
                    if command in self.queue_commands:
                        self.logger.info(f"[BetterGI-队列] 命令 '{command}' 已在队列中，跳过添加")
                        skipped_count += 1
                        continue
                    
                    await self.command_queue.put({
                        "command": command,
                        "bettergi_dir": bettergi_dir,
                        "debug_log": debug_log,
                        "callback": callback,
                        "callback_args": callback_args
                    })
                    self.queue_commands.add(command)
                    added_count += 1
            
            self.logger.info(f"[BetterGI-队列] 已将 {added_count} 个命令加入队列，跳过 {skipped_count} 个重复命令，当前队列长度: {self.command_queue.qsize()}")
            
            return True
            
        except Exception as e:
            self.logger.exception("[BetterGI] 运行时发生异常")
            return False
    
    async def _execute_single_command(
        self, 
        bettergi_dir: str, 
        command: str, 
        debug_log: bool
    ) -> bool:
        """
        执行单个 BetterGI 命令。
        
        包括进程检测、可执行文件查找、命令构建、执行和结果监控。
        
        Args:
            bettergi_dir: BetterGI 安装目录
            command: 要执行的命令
            debug_log: 是否启用调试日志
            
        Returns:
            True 如果命令执行成功，否则 False
        """
        try:
            self.logger.debug(f"[BetterGI] 开始执行单个命令: {command}")
            
            if not os.path.exists(bettergi_dir):
                self.logger.error(f"[BetterGI] 安装目录不存在: {bettergi_dir}")
                return False
            
            existing_bettergi_processes = self._find_bettergi_processes()
            if existing_bettergi_processes:
                self.logger.warning(f"[BetterGI] 检测到已有BetterGI进程在运行 (PID: {existing_bettergi_processes}), 等待其结束后再执行新命令")
                for pid in existing_bettergi_processes:
                    try:
                        proc = psutil.Process(pid)
                        proc.wait(timeout=30)
                        self.logger.info(f"[BetterGI] 现有BetterGI进程 (PID: {pid}) 已结束")
                    except psutil.NoSuchProcess:
                        continue
                    except psutil.TimeoutExpired:
                        self.logger.warning(f"[BetterGI] 等待现有BetterGI进程 (PID: {pid}) 超时，强制终止")
                        self._force_kill_windows_process(pid)
            
            bettergi_executable = None
            possible_executables = ["BetterGI.exe", "BetterGI.bat", "BetterGI.cmd", "BetterGI.py", "BetterGI"]
            
            for exe in possible_executables:
                exe_path = os.path.join(bettergi_dir, exe)
                if os.path.exists(exe_path):
                    bettergi_executable = exe_path
                    break
            
            files_in_dir = os.listdir(bettergi_dir) if os.path.exists(bettergi_dir) else []
            self.logger.debug(f"[BetterGI] 工作目录 {bettergi_dir} 中的文件: {files_in_dir}")
            
            if not bettergi_executable:
                self.logger.error(f"[BetterGI] 在目录 {bettergi_dir} 中未找到BetterGI可执行文件")
                bettergi_executable = os.path.join(bettergi_dir, "BetterGI")
            else:
                self.logger.info(f"[BetterGI] 找到BetterGI可执行文件: {bettergi_executable}")
            
            cmd_args = [bettergi_executable]
            
            if command:
                if command.startswith("--"):
                    parts = command.split(" ", 1)
                    cmd_args.append(parts[0])
                    if len(parts) > 1:
                        cmd_args.append(parts[1])
                else:
                    cmd_args.append(command)
            
            base_config = self.config.get("base", {}) if self.config else {}
            if base_config.get("toggle_audio_service", False):
                cmd_args.append("--disable-audio")
            
            if debug_log:
                cmd_args.append("--debug")
            
            full_command_str = '"' + '" "'.join(cmd_args) + '"'
            self.logger.debug(f"[BetterGI] 构建的完整命令: {full_command_str}")
            
            self.logger.info(f"[BetterGI] 执行命令: {full_command_str}")
            
            has_admin = check_admin_rights()
            self.logger.debug(f"[BetterGI] 当前进程是否具有管理员权限: {has_admin}")
            if not has_admin:
                self.logger.warning(f"[BetterGI] 警告: 可能需要管理员权限才能正常执行某些BetterGI功能")
            
            timeout = base_config.get("command_timeout", 3600)
            self.logger.info(f"[BetterGI] 使用命令超时设置: {timeout}秒")
            
            result = await execute_command_isolated(cmd_args, cwd=bettergi_dir, timeout=timeout)
            
            if result["success"]:
                completion_result = await self._wait_for_command_completion(bettergi_dir, command)
                if not completion_result:
                    result["success"] = False
                    self.logger.warning(f"[BetterGI] 命令执行返回成功，但任务完成检测失败: {command}")
                else:
                    self.logger.info(f"[BetterGI] 命令执行成功且任务已完成: {full_command_str}")
            
            if result["success"]:
                if result["stdout"]:
                    self.logger.debug(f"[BetterGI] 命令输出:\n{result['stdout'][:500]}{'...' if len(result['stdout']) > 500 else ''}")
            else:
                self.logger.error(f"[BetterGI] 命令执行失败: {full_command_str}, 返回码: {result['returncode']}")
                error_msg = ""
                if result["stderr"]:
                    error_msg += f"错误信息: {result['stderr'][:500]}{'...' if len(result['stderr']) > 500 else ''} "
                if result["stdout"]:
                    error_msg += f"标准输出: {result['stdout'][:500]}{'...' if len(result['stdout']) > 500 else ''}"
                self.logger.error(f"[BetterGI] 执行详情: {error_msg}")
            
            self.logger.debug(f"[BetterGI] 单个命令执行完成: {command}, 结果: {result['success']}")
            return result["success"]
            
        except asyncio.CancelledError:
            self.logger.info(f"[BetterGI] 命令执行任务被取消: {command}")
            return False
            
        except Exception as e:
            self.logger.error(f"[BetterGI] 执行命令 '{command}' 时发生异常: {str(e)}")
            self.logger.exception(f"[BetterGI] 异常详情")
            return False
    
    def start_scheduled_task(self, context, config):
        """
        启动定时任务。
        
        Args:
            context: 上下文对象
            config: 配置对象
        """
        if self.task_running:
            self.logger.info("[BetterGI-定时任务] 定时任务已经在运行中")
            return
            
        self.logger.info("[BetterGI-定时任务] 准备启动定时任务")
        try:
            loop = asyncio.get_running_loop()
            self.running_task = loop.create_task(self.scheduled_runner(context, config))
            self.active_tasks.append(self.running_task)
            self.logger.info("[BetterGI-定时任务] 定时任务已成功启动")
        except RuntimeError as e:
            if "no running event loop" in str(e):
                self.logger.info("[BetterGI-定时任务] 没有运行的事件循环，创建新的事件循环")
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                self.running_task = loop.create_task(self.scheduled_runner(context, config))
                self.active_tasks.append(self.running_task)
                self.logger.info("[BetterGI-定时任务] 新事件循环已创建，定时任务已启动")
            else:
                self.logger.error(f"[BetterGI-定时任务] 启动定时任务时发生错误: {e}")
        except Exception as e:
            self.logger.error(f"[BetterGI-定时任务] 启动定时任务时发生未预期的错误: {e}")
    
    async def scheduled_runner(self, context, config):
        """
        定时任务运行器。
        
        根据配置的时间每天执行一次任务。
        
        Args:
            context: 上下文对象
            config: 配置对象
        """
        try:
            self.task_running = True
            self.running_task = asyncio.current_task()
            self.logger.info("[BetterGI-定时任务] 已启动")
            
            scheduled_config = config.get("scheduled_task", {}) if config else {}
            run_time = scheduled_config.get("run_time", "00:00")
            task_command = scheduled_config.get("command", "startOneDragon")
            
            self.logger.info(f"[BetterGI-定时任务] 计划运行时间: {run_time}, 执行命令: {task_command}")
            
            run_count = 0
            last_run_date = None
            
            while self.task_running:
                now = datetime.now()
                scheduled_time = datetime.strptime(run_time, "%H:%M").replace(
                    year=now.year, month=now.month, day=now.day
                )
                
                current_date = now.date()
                if last_run_date != current_date:
                    self.logger.debug(f"[BetterGI-定时任务] 检测到日期变更，从 {last_run_date} 变更为 {current_date}，重置运行计数器")
                    run_count = 0
                    last_run_date = current_date
                
                if now >= scheduled_time and run_count == 0:
                    self.logger.info("[BetterGI-定时任务] 开始执行")
                    
                    result = await self.run_bettergi(context, config, task_command)
                    
                    if result:
                        self.logger.info("[BetterGI-定时任务] 执行成功")
                    else:
                        self.logger.error("[BetterGI-定时任务] 执行失败")
                    
                    run_count += 1
                
                await asyncio.sleep(600)
                
        except asyncio.CancelledError:
            self.logger.info("[BetterGI-定时任务] 已被取消")
            current_task = asyncio.current_task()
            if current_task and hasattr(self, 'active_tasks') and current_task in self.active_tasks:
                self.active_tasks.remove(current_task)
        except Exception as e:
            self.logger.exception("[BetterGI-定时任务] 执行过程中发生异常")
        finally:
            self.task_running = False
            self.logger.info("[BetterGI-定时任务] 已停止")
    
    def _force_kill_windows_process(self, pid: int) -> bool:
        """
        强制终止 Windows 进程。
        
        首先尝试使用 psutil，失败后依次尝试 taskkill 和 wmic。
        
        Args:
            pid: 进程ID
            
        Returns:
            True 如果成功终止，否则 False
        """
        try:
            parent = psutil.Process(pid)
            
            children = parent.children(recursive=True)
            for child in children:
                try:
                    self.logger.info(f"[BetterGI-停止功能] 强制终止子进程 PID: {child.pid}")
                    child.kill()
                except Exception as e:
                    self.logger.error(f"[BetterGI-停止功能] 强制终止子进程 PID: {child.pid} 失败: {e}")
            
            self.logger.info(f"[BetterGI-停止功能] 强制终止父进程 PID: {pid}")
            parent.kill()
            
            try:
                parent.wait(timeout=3)
                return True
            except psutil.TimeoutExpired:
                self.logger.warning(f"[BetterGI-停止功能] 强制终止进程 PID: {pid} 超时，尝试使用taskkill")
                return self._taskkill_process(pid)
        except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess) as e:
            self.logger.warning(f"[BetterGI-停止功能] 进程 PID: {pid} 不存在或无法访问: {e}，尝试使用taskkill")
            return self._taskkill_process(pid)
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] 强制终止进程 PID: {pid} 时发生未知错误: {e}，尝试使用taskkill")
            return self._taskkill_process(pid)
    
    def _taskkill_process(self, pid: int) -> bool:
        """
        使用 taskkill 命令终止进程。
        
        Args:
            pid: 进程ID
            
        Returns:
            True 如果成功终止，否则尝试 wmic
        """
        try:
            result = subprocess.run(
                ["taskkill", "/F", "/PID", str(pid)],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                self.logger.info(f"[BetterGI-停止功能] taskkill成功终止进程 PID: {pid}")
                return True
            else:
                self.logger.warning(f"[BetterGI-停止功能] taskkill终止进程 PID: {pid} 失败: {result.stderr.strip()}")
                return self._wmic_kill_process(pid)
        except subprocess.TimeoutExpired:
            self.logger.warning(f"[BetterGI-停止功能] taskkill终止进程 PID: {pid} 超时")
            return self._wmic_kill_process(pid)
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] taskkill终止进程 PID: {pid} 时发生错误: {e}")
            return self._wmic_kill_process(pid)
    
    def _wmic_kill_process(self, pid: int) -> bool:
        """
        使用 wmic 命令终止进程（最后的手段）。
        
        Args:
            pid: 进程ID
            
        Returns:
            True 如果成功终止，否则 False
        """
        try:
            result = subprocess.run(
                ["wmic", "process", "where", f"ProcessId={pid}", "delete"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                self.logger.info(f"[BetterGI-停止功能] wmic成功终止进程 PID: {pid}")
                return True
            else:
                self.logger.warning(f"[BetterGI-停止功能] wmic终止进程 PID: {pid} 失败: {result.stderr.strip()}")
                return False
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] wmic终止进程 PID: {pid} 时发生错误: {e}")
            return False
    
    def _find_game_processes(self) -> List[int]:
        """
        查找游戏进程。
        
        支持原神、星穹铁道等游戏进程。
        
        Returns:
            游戏进程PID列表
        """
        game_pids = []
        try:
            game_process_names = [
                "YuanShen.exe",
                "GenshinImpact.exe",
                "StarRail.exe"
            ]
            
            for proc in psutil.process_iter(['pid', 'name']):
                try:
                    proc_info = proc.info
                    if proc_info['name'] in game_process_names:
                        game_pids.append(proc_info['pid'])
                        self.logger.info(f"[BetterGI-停止功能] 发现游戏进程: {proc_info['name']} (PID: {proc_info['pid']})")
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] 查找游戏进程时发生错误: {e}")
        
        return game_pids

    def _terminate_game_processes(self) -> bool:
        """
        终止所有游戏进程。
        
        Returns:
            True 如果所有进程都成功终止，否则 False
        """
        game_pids = self._find_game_processes()
        if not game_pids:
            self.logger.info("[BetterGI-停止功能] 未检测到游戏进程")
            return True
            
        self.logger.info(f"[BetterGI-停止功能] 检测到 {len(game_pids)} 个游戏进程，正在终止")
        termination_results = []
        
        for pid in game_pids:
            result = self._force_kill_windows_process(pid)
            termination_results.append(result)
            if result:
                self.logger.info(f"[BetterGI-停止功能] 成功终止游戏进程 PID: {pid}")
            else:
                self.logger.warning(f"[BetterGI-停止功能] 未能成功终止游戏进程 PID: {pid}")
        
        all_terminated = all(termination_results)
        if not all_terminated:
            self.logger.warning("[BetterGI-停止功能] 部分游戏进程可能未能成功终止")
        else:
            self.logger.info("[BetterGI-停止功能] 所有游戏进程已成功终止")
            
        return all_terminated

    async def _wait_for_command_completion(
        self, 
        bettergi_dir: str, 
        command: str
    ) -> bool:
        """
        等待命令执行完成。
        
        通过监控日志文件和进程状态来判断任务是否完成。
        
        Args:
            bettergi_dir: BetterGI 安装目录
            command: 执行的命令
            
        Returns:
            True 如果任务成功完成，否则 False
        """
        try:
            self.logger.info(f"[BetterGI-监控] 开始监控命令完成状态: {command}")
            
            if not os.path.exists(bettergi_dir):
                self.logger.error(f"[BetterGI-监控] 安装目录不存在: {bettergi_dir}")
                return False
            
            log_file_pattern = os.path.join(bettergi_dir, "better-genshin-impact*.log")
            
            log_files = glob.glob(log_file_pattern)
            if not log_files:
                self.logger.warning(f"[BetterGI-监控] 未找到日志文件: {log_file_pattern}")
                return await self._wait_for_process_completion(command)
            
            latest_log_file = max(log_files, key=os.path.getctime)
            self.logger.info(f"[BetterGI-监控] 监控日志文件: {latest_log_file}")
            
            last_position = os.path.getsize(latest_log_file) if os.path.exists(latest_log_file) else 0
            start_time = time.time()
            last_log_time = start_time
            
            task_end_patterns = [
                r"任务执行完成",
                r"所有任务已完成",
                r"脚本执行结束",
                r"better-genshin-impact.exe 已退出",
                r"程序已正常退出",
                r"执行完成",
                r"任务结束",
                r"操作完成",
                r"流程结束",
                r"全部完成",
                r"任务已全部执行完毕",
                r"脚本运行完毕",
                r"程序退出",
                r"进程已结束",
                r"执行完毕",
                r"任务完成",
                r"一条龙任务执行完毕",
                r"自动每日任务完成",
                r"已完成所有任务"
            ]
            
            timeout = self.config.get("base", {}).get("command_timeout", 3600) if self.config else 3600
            self.logger.info(f"[BetterGI-监控] 使用超时设置: {timeout}秒")
            
            while time.time() - start_time < timeout:
                current_time = time.time()
                
                bettergi_processes = self._find_bettergi_processes()
                game_processes = self._find_game_processes()
                
                if not bettergi_processes and not game_processes:
                    self.logger.info(f"[BetterGI-监控] 所有BetterGI进程和游戏进程已结束，命令 {command} 执行完成")
                    return True
                
                if not os.path.exists(latest_log_file):
                    self.logger.warning(f"[BetterGI-监控] 日志文件 {latest_log_file} 已不存在，检查新的日志文件")
                    log_files = glob.glob(log_file_pattern)
                    if log_files:
                        latest_log_file = max(log_files, key=os.path.getctime)
                        last_position = os.path.getsize(latest_log_file) if os.path.exists(latest_log_file) else 0
                        last_log_time = current_time
                    continue
                
                current_size = os.path.getsize(latest_log_file)
                if current_size > last_position:
                    last_log_time = current_time
                elif (current_time - last_log_time) > 300:
                    self.logger.warning(f"[BetterGI-监控] 日志文件已超过5分钟无更新，命令 {command} 可能已卡住")
                    self._terminate_game_processes()
                    return False
                
                try:
                    with open(latest_log_file, 'r', encoding='utf-8', errors='ignore') as f:
                        f.seek(last_position)
                        new_content = f.read()
                        last_position = f.tell()
                        
                        if new_content:
                            for pattern in task_end_patterns:
                                if re.search(pattern, new_content, re.IGNORECASE):
                                    self.logger.info(f"[BetterGI-监控] 检测到任务结束模式: '{pattern}'，命令 {command} 执行完成")
                                    return True
                except Exception as e:
                    self.logger.error(f"[BetterGI-监控] 读取日志文件时发生错误: {e}")
                
                await asyncio.sleep(5)
            
            self.logger.warning(f"[BetterGI-监控] 命令 {command} 执行超时 ({timeout}秒)")
            return False
            
        except asyncio.CancelledError:
            self.logger.info(f"[BetterGI-监控] 命令执行任务被取消: {command}")
            return False
        
        except Exception as e:
            self.logger.error(f"[BetterGI-监控] 等待命令完成时发生异常: {e}")
            self.logger.exception(f"[BetterGI-监控] 异常详情")
            return False
    
    async def _wait_for_process_completion(self, command: str) -> bool:
        """
        等待进程完成（无日志文件时的备用方案）。
        
        Args:
            command: 执行的命令
            
        Returns:
            True 如果进程成功结束，否则 False
        """
        try:
            self.logger.info(f"[BetterGI-进程] 开始监控进程完成状态: {command}")
            
            timeout = self.config.get("base", {}).get("command_timeout", 3600) if self.config else 3600
            start_time = time.time()
            
            while time.time() - start_time < timeout:
                bettergi_processes = self._find_bettergi_processes()
                if not bettergi_processes:
                    self.logger.info(f"[BetterGI-进程] 所有BetterGI进程已结束，命令 {command} 执行完成")
                    return True
                
                await asyncio.sleep(10)
            
            self.logger.warning(f"[BetterGI-进程] 命令 {command} 执行超时 ({timeout}秒)")
            return False
            
        except Exception as e:
            self.logger.error(f"[BetterGI-进程] 等待进程完成时发生异常: {e}")
            return False

    def _find_bettergi_processes(self) -> List[int]:
        """
        查找 BetterGI 相关进程。
        
        Returns:
            BetterGI 进程PID列表
        """
        bettergi_pids = []
        try:
            for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
                try:
                    proc_info = proc.info
                    if ('bettergi' in proc_info['name'].lower() or 
                        proc_info['cmdline'] and any('bettergi' in arg.lower() for arg in proc_info['cmdline'])):
                        bettergi_pids.append(proc_info['pid'])
                except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                    pass
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] 查找BetterGI进程时发生错误: {e}")
        
        return bettergi_pids
    
    def _terminate_external_programs(self, external_programs: List[str]) -> None:
        """
        终止外部程序进程。
        
        会自动过滤游戏进程，只终止非游戏进程。
        
        Args:
            external_programs: 要终止的程序名称列表
        """
        try:
            game_processes = ["YuanShen.exe", "GenshinImpact.exe", "StarRail.exe"]
            filtered_programs = [prog for prog in external_programs if prog not in game_processes]
            
            if filtered_programs != external_programs:
                self.logger.info(f"[BetterGI-停止功能] 过滤掉了游戏进程，只处理非游戏进程: {filtered_programs}")
            
            for program_name in filtered_programs:
                found_processes = False
                for proc in psutil.process_iter(['pid', 'name', 'exe']):
                    try:
                        if proc.info['name'] == program_name:
                            found_processes = True
                            pid = proc.info['pid']
                            self.logger.info(f"[BetterGI-停止功能] 发现外部程序进程 {program_name} (PID: {pid})")
                            
                            try:
                                proc.terminate()
                                gone, alive = psutil.wait_procs([proc], timeout=10)
                                if alive:
                                    self.logger.warning(f"[BetterGI-停止功能] 终止 {program_name} (PID: {pid}) 超时，尝试强制终止")
                                    proc.kill()
                                    self.logger.info(f"[BetterGI-停止功能] 已强制终止 {program_name} (PID: {pid})")
                                else:
                                    self.logger.info(f"[BetterGI-停止功能] 已成功终止 {program_name} (PID: {pid})")
                            except Exception as e:
                                self.logger.error(f"[BetterGI-停止功能] 终止 {program_name} (PID: {pid}) 时出错: {e}")
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
                
                if not found_processes:
                    self.logger.info(f"[BetterGI-停止功能] 未发现运行中的 {program_name} 进程")
        except ImportError:
            self.logger.warning("[BetterGI-停止功能] 未找到psutil模块，无法检查外部程序进程")
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] 检查外部程序进程时发生错误: {e}")

    async def _stop_scheduled_task(self) -> None:
        """停止定时任务。"""
        self.task_running = False
        
        if self.running_task and not self.running_task.done():
            self.logger.info("[BetterGI-停止功能] 正在取消定时任务")
            self.running_task.cancel()
            try:
                await asyncio.wait_for(self.running_task, timeout=5)
                self.logger.info("[BetterGI-停止功能] 定时任务已成功取消")
            except asyncio.CancelledError:
                self.logger.info("[BetterGI-停止功能] 定时任务已被取消")
            except asyncio.TimeoutError:
                self.logger.warning("[BetterGI-停止功能] 取消定时任务超时")
            except Exception as e:
                self.logger.error(f"[BetterGI-停止功能] 取消定时任务时发生错误: {e}")
    
    async def _cancel_active_tasks(self) -> None:
        """取消所有活动任务。"""
        if self.active_tasks:
            self.logger.info(f"[BetterGI-停止功能] 正在取消 {len(self.active_tasks)} 个活动任务")
            
            tasks_to_cancel = self.active_tasks.copy()
            cancellation_tasks = []
            
            for task in tasks_to_cancel:
                if not task.done():
                    task.cancel()
                    async def wait_for_cancellation(t):
                        try:
                            await asyncio.wait_for(t, timeout=5)
                        except asyncio.CancelledError:
                            pass
                        except asyncio.TimeoutError:
                            self.logger.warning(f"[BetterGI-停止功能] 等待任务取消超时")
                    
                    cancellation_tasks.append(wait_for_cancellation(task))
            
            if cancellation_tasks:
                await asyncio.gather(*cancellation_tasks, return_exceptions=True)
            
            self.active_tasks.clear()
            self.logger.info("[BetterGI-停止功能] 所有活动任务已取消")
    
    async def _terminate_tracking_processes(self) -> bool:
        """
        终止正在跟踪的进程。
        
        Returns:
            True 如果所有进程都成功终止，否则 False
        """
        if not running_processes:
            return True
            
        self.logger.info(f"[BetterGI-停止功能] 正在终止 {len(running_processes)} 个跟踪进程")
        
        process_pids = list(running_processes.keys())
        termination_results = []
        
        for pid in process_pids:
            try:
                process = running_processes.get(pid)
                if process:
                    self.logger.info(f"[BetterGI-停止功能] 终止跟踪进程 PID: {pid}")
                    
                    try:
                        process.terminate()
                        await asyncio.wait_for(process.wait(), timeout=5)
                        self.logger.info(f"[BetterGI-停止功能] 进程 PID: {pid} 已正常终止")
                        termination_results.append(True)
                    except asyncio.TimeoutError:
                        self.logger.warning(f"[BetterGI-停止功能] 进程 PID: {pid} 终止超时，尝试强制终止")
                        force_result = self._force_kill_windows_process(pid)
                        termination_results.append(force_result)
                    except Exception as e:
                        self.logger.error(f"[BetterGI-停止功能] 终止进程 PID: {pid} 时发生错误: {e}")
                        force_result = self._force_kill_windows_process(pid)
                        termination_results.append(force_result)
            except Exception as e:
                self.logger.error(f"[BetterGI-停止功能] 处理进程 PID: {pid} 时发生错误: {e}")
                termination_results.append(False)
        
        running_processes.clear()
        
        all_terminated = all(termination_results)
        if not all_terminated:
            self.logger.warning("[BetterGI-停止功能] 部分跟踪进程可能未能成功终止")
        
        return all_terminated
    
    def _terminate_external_bettergi_processes(self) -> bool:
        """
        终止外部 BetterGI 进程。
        
        Returns:
            True 如果所有进程都成功终止，否则 False
        """
        external_processes = self._find_bettergi_processes()
        if not external_processes:
            self.logger.info("[BetterGI-停止功能] 未检测到外部BetterGI进程")
            return True
            
        self.logger.info(f"[BetterGI-停止功能] 检测到 {len(external_processes)} 个BetterGI相关进程，正在终止")
        force_termination_results = []
        for pid in external_processes:
            result = self._force_kill_windows_process(pid)
            force_termination_results.append(result)
            if result:
                self.logger.info(f"[BetterGI-停止功能] 成功终止BetterGI进程 PID: {pid}")
            else:
                self.logger.warning(f"[BetterGI-停止功能] 未能成功终止BetterGI进程 PID: {pid}")
        
        all_external_terminated = all(force_termination_results)
        if not all_external_terminated:
            self.logger.warning("[BetterGI-停止功能] 部分外部BetterGI进程可能未能成功终止")
        else:
            self.logger.info("[BetterGI-停止功能] 所有外部BetterGI进程已成功终止")
            
        return all_external_terminated
    
    async def _stop_queue_processing(self) -> None:
        """停止队列处理并清空队列。"""
        if hasattr(self, 'queue_processing') and self.queue_processing:
            self.logger.info("[BetterGI-停止功能] 停止命令队列处理")
            self.queue_processing = False
            if hasattr(self, 'queue_task') and self.queue_task and not self.queue_task.done():
                try:
                    self.queue_task.cancel()
                    await asyncio.wait_for(self.queue_task, timeout=5)
                except (asyncio.CancelledError, asyncio.TimeoutError):
                    self.logger.info("[BetterGI-停止功能] 队列处理任务已取消")
        
        if hasattr(self, 'command_queue') and not self.command_queue.empty():
            queue_size = self.command_queue.qsize()
            while not self.command_queue.empty():
                try:
                    self.command_queue.get_nowait()
                    self.command_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            self.logger.info(f"[BetterGI-停止功能] 已清空命令队列，共 {queue_size} 个任务被移除")
        
        if hasattr(self, 'current_command'):
            self.current_command = None
        if hasattr(self, 'task_running_count'):
            self.task_running_count = 0
    
    async def stop_task(self, context) -> bool:
        """
        停止所有任务。
        
        执行完整的停止流程，包括:
        1. 停止定时任务
        2. 取消活动任务
        3. 终止跟踪进程
        4. 终止外部 BetterGI 进程
        5. 终止游戏进程
        6. 停止队列处理
        7. 最终检查并清理
        
        Args:
            context: 上下文对象
            
        Returns:
            True 如果有任务被停止，False 如果没有运行中的任务
        """
        self.logger.info("[BetterGI-停止功能] 收到停止请求，开始执行增强版停止流程")
        
        initial_tasks_running = self.task_running
        initial_active_tasks_count = len(self.active_tasks)
        initial_processes_count = len(running_processes)
        
        has_running_task = initial_tasks_running or initial_active_tasks_count > 0 or initial_processes_count > 0
        
        all_bettergi_processes = self._find_bettergi_processes()
        has_external_processes = len(all_bettergi_processes) > 0
        
        if not has_running_task and not has_external_processes:
            self.logger.info("[BetterGI-停止功能] 当前没有正在运行的任务、进程或外部BetterGI进程")
            return False
        else:
            if has_external_processes:
                self.logger.info(f"[BetterGI-停止功能] 检测到 {len(all_bettergi_processes)} 个BetterGI相关进程")
        
        await self._stop_scheduled_task()
        await self._cancel_active_tasks()
        await self._terminate_tracking_processes()
        self._terminate_external_bettergi_processes()
        self._terminate_game_processes()
        await self._stop_queue_processing()
        
        await asyncio.sleep(2)
        
        remaining_processes = self._find_bettergi_processes()
        if remaining_processes:
            self.logger.warning(f"[BetterGI-停止功能] 警告：仍有 {len(remaining_processes)} 个BetterGI相关进程可能在运行: {remaining_processes}")
            final_termination_results = []
            for pid in remaining_processes:
                result = self._force_kill_windows_process(pid)
                final_termination_results.append(result)
            if all(final_termination_results):
                self.logger.info("[BetterGI-停止功能] 最终检查后成功终止所有剩余进程")
            else:
                self.logger.warning("[BetterGI-停止功能] 最终检查后仍有部分进程未能终止")
        else:
            self.logger.info("[BetterGI-停止功能] 最终检查确认所有BetterGI进程已终止")
        
        self.logger.info("[BetterGI-停止功能] 最终检查外部程序进程")
        try:
            known_external_programs = []
            remaining_external_programs = {}
            
            for program_name in known_external_programs:
                program_processes = []
                for proc in psutil.process_iter(['pid', 'name']):
                    try:
                        if proc.info['name'] == program_name:
                            program_processes.append(proc.info['pid'])
                    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
                        pass
                
                if program_processes:
                    remaining_external_programs[program_name] = program_processes
            
            if remaining_external_programs:
                self.logger.warning(f"[BetterGI-停止功能] 警告：仍有外部程序进程在运行: {remaining_external_programs}")
                for program_name, pids in remaining_external_programs.items():
                    for pid in pids:
                        try:
                            subprocess.run(["taskkill", "/F", "/PID", str(pid)], check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                            self.logger.info(f"[BetterGI-停止功能] 使用taskkill成功终止 {program_name} (PID: {pid})")
                        except subprocess.CalledProcessError as e:
                            self.logger.error(f"[BetterGI-停止功能] 使用taskkill终止 {program_name} (PID: {pid}) 失败: {e.stderr}")
            else:
                self.logger.info("[BetterGI-停止功能] 最终检查确认所有外部程序进程已终止")
        except Exception as e:
            self.logger.error(f"[BetterGI-停止功能] 最终检查外部程序进程时发生错误: {e}")
        
        self.logger.info("[BetterGI-停止功能] 增强版停止流程执行完成")
        return True

    async def remove_command_from_queue(self, command_to_remove: str) -> int:
        """
        从队列中移除指定命令。
        
        Args:
            command_to_remove: 要移除的命令
            
        Returns:
            移除的命令数量
        """
        if not hasattr(self, 'command_queue') or self.command_queue.empty():
            self.logger.info(f"[BetterGI-队列] 队列为空，无法删除命令 '{command_to_remove}'")
            return 0
            
        temp_queue = asyncio.Queue()
        removed_count = 0
        queue_size = self.command_queue.qsize()
        
        self.logger.info(f"[BetterGI-队列] 开始从队列中删除命令 '{command_to_remove}'，当前队列大小: {queue_size}")
        
        was_processing = self.queue_processing
        self.queue_processing = False
        
        try:
            while not self.command_queue.empty():
                try:
                    command_info = self.command_queue.get_nowait()
                    
                    if command_info["command"] == command_to_remove:
                        removed_count += 1
                        self.logger.info(f"[BetterGI-队列] 已删除命令: '{command_to_remove}'")
                    else:
                        await temp_queue.put(command_info)
                    
                    self.command_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            while not temp_queue.empty():
                try:
                    command_info = temp_queue.get_nowait()
                    await self.command_queue.put(command_info)
                    temp_queue.task_done()
                except asyncio.QueueEmpty:
                    break
            
            self.logger.info(f"[BetterGI-队列] 删除命令完成，成功删除 {removed_count} 个命令，剩余队列大小: {self.command_queue.qsize()}")
        finally:
            if was_processing:
                self.queue_processing = True
                if not hasattr(self, 'queue_task') or self.queue_task.done():
                    self.logger.warning(f"[BetterGI-队列] 队列处理任务需要外部调用提供参数才能完全重启")
            
        return removed_count
    
    async def get_status(self) -> Dict[str, Any]:
        """
        获取当前服务状态。
        
        Returns:
            状态报告字典，包含:
            - task_running: 定时任务是否运行
            - active_tasks_count: 活动任务数量
            - active_tasks: 活动任务名称列表
            - queue_enabled: 队列是否启用
            - queue_size: 队列大小
            - current_command: 当前执行的命令
            - queue_processing: 队列是否正在处理
        """
        status_report = {
            "task_running": self.task_running,
            "active_tasks_count": len(self.active_tasks),
            "active_tasks": [f"{task.get_name()}" for task in self.active_tasks if not task.done()],
            "queue_enabled": getattr(self, 'queue_processing', False),
            "queue_size": getattr(self, 'command_queue', asyncio.Queue()).qsize(),
            "current_command": getattr(self, 'current_command', None),
            "queue_processing": getattr(self, 'queue_processing', False)
        }
        
        return status_report


bettergi_service = BettergiService()
