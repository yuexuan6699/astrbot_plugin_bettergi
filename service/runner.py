import asyncio
import os
from typing import Any

import aiohttp

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
    """远程模式命令执行器，通过 HTTP API 调用辅助程序。"""

    def __init__(self, url: str, token: str = "", timeout: int = 30):
        self._url = url.rstrip("/")
        self._token = token
        self._timeout = aiohttp.ClientTimeout(total=timeout)
        self._current_command: str = ""

    @property
    def is_running(self) -> bool:
        return bool(self._current_command)

    @property
    def current_command(self) -> str:
        return self._current_command

    async def run(self, args: list[str]) -> bool:
        """通过辅助程序 API 启动 BetterGI 命令。"""
        command_str = " ".join(args)
        self._current_command = command_str

        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                payload: dict[str, Any] = {"args": args}
                if self._token:
                    payload["token"] = self._token

                async with session.post(
                    f"{self._url}/api/run",
                    json=payload,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        logger.info(f"[BetterGI-Remote] 命令已发送: {command_str}")
                        return data.get("success", False)
                    else:
                        text = await resp.text()
                        logger.error(
                            f"[BetterGI-Remote] 启动失败 ({resp.status}): {text}"
                        )
                        self._current_command = ""
                        return False
        except aiohttp.ClientConnectorError:
            logger.error(f"[BetterGI-Remote] 无法连接辅助程序: {self._url}")
            self._current_command = ""
            return False
        except asyncio.TimeoutError:
            logger.error("[BetterGI-Remote] 请求超时")
            self._current_command = ""
            return False
        except Exception as e:
            logger.error(f"[BetterGI-Remote] 请求失败: {e}", exc_info=True)
            self._current_command = ""
            return False

    async def stop(self) -> bool:
        """通过辅助程序 API 停止当前任务。"""
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                payload: dict[str, Any] = {}
                if self._token:
                    payload["token"] = self._token

                async with session.post(
                    f"{self._url}/api/stop",
                    json=payload,
                ) as resp:
                    success = resp.status == 200
                    if success:
                        logger.info("[BetterGI-Remote] 停止命令已发送")
                    self._current_command = ""
                    return success
        except Exception as e:
            logger.error(f"[BetterGI-Remote] 停止失败: {e}")
            self._current_command = ""
            return False

    async def get_status(self) -> dict[str, Any]:
        """通过辅助程序 API 查询状态。"""
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                params = {}
                if self._token:
                    params["token"] = self._token

                async with session.get(
                    f"{self._url}/api/status",
                    params=params,
                ) as resp:
                    if resp.status == 200:
                        data = await resp.json()
                        self._current_command = data.get("current_command", "")
                        return {
                            "is_running": data.get("is_running", False),
                            "current_command": self._current_command,
                            "pid": data.get("pid"),
                            "mode": "remote",
                        }
        except Exception:
            pass
        return {
            "is_running": False,
            "current_command": "",
            "pid": None,
            "mode": "remote",
        }

    async def cleanup(self) -> None:
        pass

    async def check_env(self) -> tuple[bool, str]:
        """检查辅助程序是否可达。"""
        try:
            async with aiohttp.ClientSession(timeout=self._timeout) as session:
                params = {}
                if self._token:
                    params["token"] = self._token

                async with session.get(
                    f"{self._url}/api/status",
                    params=params,
                ) as resp:
                    if resp.status == 200:
                        return True, "辅助程序连接正常"
                    return False, f"辅助程序返回 {resp.status}"
        except aiohttp.ClientConnectorError:
            return False, f"无法连接辅助程序: {self._url}"
        except Exception as e:
            return False, f"连接异常: {e}"


def create_runner(config: dict) -> LocalRunner | RemoteRunner:
    """根据配置创建对应的命令执行器。"""
    mode = config.get("mode", "local")
    timeout = config.get("command_timeout", 3600)

    if mode == "remote":
        remote_config = config.get("remote", {})
        logger.debug("[BetterGI-Runner] 创建 RemoteRunner: url=%s", remote_config.get("url", "http://127.0.0.1:9099"))
        return RemoteRunner(
            url=remote_config.get("url", "http://127.0.0.1:9099"),
            token=remote_config.get("token", ""),
            timeout=30,
        )
    else:
        bettergi_dir = config.get("bettergi_dir", "")
        logger.debug("[BetterGI-Runner] 创建 LocalRunner: bettergi_dir=%s, timeout=%d", bettergi_dir, timeout)
        return LocalRunner(
            bettergi_dir=bettergi_dir,
            timeout=timeout,
        )
