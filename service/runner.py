import asyncio
import os
from typing import Any

try:
    from astrbot.api import logger
except ImportError:
    import logging

    logger = logging.getLogger("bettergi")


def build_command(template_key: str, config_name: str) -> list[str]:
    """根据模板类型和配置名组装 BetterGI 命令参数。

    Args:
        template_key: 模板类型 ("dragon" 或 "group")
        config_name: 配置名称

    Returns:
        命令参数列表（不含 exe 路径），如 ["--startOneDragon", "每日自动"]
    """
    if template_key == "dragon":
        args = ["--startOneDragon"]
        if config_name:
            args.append(config_name)
        return args
    elif template_key == "group":
        args = ["--startGroups"]
        if config_name:
            args.append(config_name)
        return args
    else:
        return [config_name] if config_name else []


class LocalRunner:
    """本地模式命令执行器，直接通过子进程执行 BetterGI.exe。"""

    def __init__(self, bettergi_dir: str, timeout: int = 3600):
        self._bettergi_dir = bettergi_dir
        self._timeout = timeout
        self._process: asyncio.subprocess.Process | None = None
        self._current_command: str = ""
        self._lock = asyncio.Lock()

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.returncode is None

    @property
    def current_command(self) -> str:
        return self._current_command

    async def run(self, args: list[str]) -> bool:
        """启动 BetterGI 执行命令（非阻塞，立即返回）。

        任务完成通过 Webhook 事件通知，不依赖进程返回码。
        """
        async with self._lock:
            if self.is_running:
                logger.warning("[BetterGI-Runner] 已有任务正在运行，请先停止")
                return False

            exe_path = self._find_executable()
            if not exe_path:
                logger.error(
                    f"[BetterGI-Runner] 未找到 BetterGI.exe: {self._bettergi_dir}"
                )
                return False

            cmd = [exe_path] + args
            self._current_command = " ".join(args)
            logger.info(f"[BetterGI-Runner] 执行命令: {' '.join(cmd)}")

            try:
                env = os.environ.copy()
                env.pop("PYTHONPATH", None)
                env.pop("PYTHONHOME", None)

                self._process = await asyncio.create_subprocess_exec(
                    *cmd,
                    cwd=self._bettergi_dir,
                    stdout=asyncio.subprocess.PIPE,
                    stderr=asyncio.subprocess.PIPE,
                    env=env,
                )
                logger.info(f"[BetterGI-Runner] 进程已启动 (PID: {self._process.pid})")
                return True
            except FileNotFoundError:
                logger.error(f"[BetterGI-Runner] 可执行文件不存在: {exe_path}")
                return False
            except PermissionError:
                logger.error("[BetterGI-Runner] 权限不足，可能需要管理员权限")
                return False
            except Exception as e:
                logger.error(f"[BetterGI-Runner] 启动进程失败: {e}", exc_info=True)
                return False

    async def stop(self) -> bool:
        """停止当前运行的 BetterGI 进程。"""
        async with self._lock:
            if not self._process or self._process.returncode is not None:
                self._process = None
                self._current_command = ""
                return False

            pid = self._process.pid
            logger.info(f"[BetterGI-Runner] 正在停止进程 PID: {pid}")

            try:
                self._process.terminate()
                try:
                    await asyncio.wait_for(self._process.wait(), timeout=5)
                    logger.info(f"[BetterGI-Runner] 进程 {pid} 已正常终止")
                except asyncio.TimeoutError:
                    logger.warning(f"[BetterGI-Runner] 进程 {pid} 超时，强制终止")
                    self._process.kill()
                    await asyncio.wait_for(self._process.wait(), timeout=3)
            except ProcessLookupError:
                pass
            except Exception as e:
                logger.error(f"[BetterGI-Runner] 停止进程失败: {e}")
                try:
                    self._process.kill()
                except Exception:
                    pass

            self._process = None
            self._current_command = ""
            return True

    async def get_status(self) -> dict[str, Any]:
        """获取当前运行状态。"""
        return {
            "is_running": self.is_running,
            "current_command": self._current_command,
            "pid": self._process.pid if self._process else None,
            "mode": "local",
        }

    async def cleanup(self) -> None:
        """清理资源。"""
        await self.stop()

    def _find_executable(self) -> str | None:
        """查找 BetterGI 可执行文件。"""
        for name in ("BetterGI.exe", "BetterGI.bat", "BetterGI.cmd"):
            path = os.path.join(self._bettergi_dir, name)
            if os.path.exists(path):
                return path
        return None

    async def check_env(self) -> tuple[bool, str]:
        """检查本地环境是否可用。"""
        if not self._bettergi_dir:
            return False, "BetterGI 安装目录未配置"
        if not os.path.exists(self._bettergi_dir):
            return False, f"目录不存在: {self._bettergi_dir}"
        exe = self._find_executable()
        if not exe:
            return False, f"未找到 BetterGI.exe: {self._bettergi_dir}"
        return True, "OK"


class RemoteRunner:
    """远程模式命令执行器，通过 SSE 连接的辅助程序执行命令。

    辅助程序主动连接到 AstrBot 主端口的 SSE 端点，
    插件通过 SSE 下发命令，辅助程序执行后通过 POST /result 上报结果。
    """

    def __init__(self, manager, timeout: int = 3600):
        self._manager = manager  # RemoteConnectionManager
        self._timeout = timeout
        self._current_command: str = ""

    @property
    def is_running(self) -> bool:
        # 简化：有当前命令就认为在运行（实际状态通过 Webhook 事件更新）
        return bool(self._current_command)

    @property
    def current_command(self) -> str:
        return self._current_command

    async def run(self, args: list[str]) -> bool:
        """通过辅助程序执行 BetterGI 命令。"""
        if not self._manager.is_connected:
            logger.error("[BetterGI-Remote] 辅助程序未连接")
            return False

        command_str = " ".join(args)
        logger.info("[BetterGI-Remote] 下发命令: %s", command_str)

        success, message = await self._manager.submit_task(args, timeout=self._timeout)
        if success:
            self._current_command = command_str
            logger.info("[BetterGI-Remote] 命令已执行: %s", command_str)
        else:
            self._current_command = ""
            logger.error("[BetterGI-Remote] 命令执行失败: %s", message)
        return success

    async def stop(self) -> bool:
        """发送停止指令。"""
        if not self._manager.is_connected:
            return False

        success, message = await self._manager.submit_stop()
        if success:
            self._current_command = ""
            logger.info("[BetterGI-Remote] 已停止: %s", message)
        else:
            logger.error("[BetterGI-Remote] 停止失败: %s", message)
        return success

    async def get_status(self) -> dict[str, Any]:
        """获取运行状态。"""
        return {
            "is_running": self.is_running,
            "current_command": self._current_command,
            "pid": None,
            "mode": "remote",
            "connected": self._manager.is_connected,
        }

    async def cleanup(self) -> None:
        pass

    async def check_env(self) -> tuple[bool, str]:
        """检查辅助程序是否已连接。"""
        if self._manager.is_connected:
            return True, "辅助程序已连接"
        return False, "辅助程序未连接，请确认辅助程序正在运行且地址配置正确"


def create_runner(config: dict, remote_manager=None) -> LocalRunner | RemoteRunner:
    """根据配置创建对应的命令执行器。

    Args:
        config: 插件配置
        remote_manager: 远程连接管理器（远程模式时必须提供）
    """
    mode = config.get("mode", "local")
    timeout = config.get("command_timeout", 3600)

    if mode == "remote":
        logger.debug("[BetterGI-Runner] 创建 RemoteRunner（SSE模式）")
        if remote_manager is None:
            raise ValueError("远程模式需要提供 remote_manager")
        return RemoteRunner(
            manager=remote_manager,
            timeout=timeout,
        )
    else:
        bettergi_dir = config.get("bettergi_dir", "")
        logger.debug(
            "[BetterGI-Runner] 创建 LocalRunner: bettergi_dir=%s, timeout=%d",
            bettergi_dir, timeout,
        )
        return LocalRunner(
            bettergi_dir=bettergi_dir,
            timeout=timeout,
        )
