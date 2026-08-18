"""BetterGI 辅助程序 - 分离式模式

运行在 BetterGI 所在电脑上，提供 HTTP API 供远程 AstrBot 插件调用。
所有配置从同目录下的 config.yaml 读取。

用法:
    直接运行: python server.py
    后台运行: pythonw server.py
    开机自启: 通过 运行功能.bat 注册 Windows 计划任务
"""

import asyncio
import logging
import os
import sys

import psutil
import uvicorn
import yaml
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_SCRIPT_DIR, "config.yaml")
_LOG_PATH = os.path.join(_SCRIPT_DIR, "assistant.log")

app = FastAPI(title="BetterGI Assistant", version="2.0.0")

_config: dict = {}
_process: asyncio.subprocess.Process | None = None
_current_command: str = ""
_logger = logging.getLogger("bettergi-assistant")


class RunRequest(BaseModel):
    args: list[str] = []
    token: str = ""


class StopRequest(BaseModel):
    token: str = ""


def _setup_logging(level: str = "INFO") -> None:
    """配置日志，同时输出到文件和控制台。"""
    log_level = getattr(logging, level.upper(), logging.INFO)

    formatter = logging.Formatter(
        "%(asctime)s [%(levelname)s] %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    file_handler = logging.FileHandler(_LOG_PATH, encoding="utf-8")
    file_handler.setFormatter(formatter)
    file_handler.setLevel(log_level)

    root_logger = logging.getLogger()
    root_logger.setLevel(log_level)
    root_logger.addHandler(file_handler)

    if sys.stdout:
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)


def _load_config() -> dict:
    """从 config.yaml 加载配置。"""
    if not os.path.exists(_CONFIG_PATH):
        print(f"错误: 配置文件不存在: {_CONFIG_PATH}")
        print("请确保 config.yaml 与 server.py 在同一目录下")
        sys.exit(1)

    try:
        with open(_CONFIG_PATH, encoding="utf-8") as f:
            return yaml.safe_load(f) or {}
    except yaml.YAMLError as e:
        print(f"错误: 配置文件格式不正确: {e}")
        sys.exit(1)


def _verify_token(token: str | None) -> None:
    expected = _config.get("token", "")
    if expected and token != expected:
        raise HTTPException(status_code=401, detail="unauthorized")


_ALLOWED_PREFIXES = ("--startOneDragon", "--startGroups")


def _validate_args(args: list[str]) -> bool:
    """校验命令参数，只允许 --startOneDragon 或 --startGroups 开头。"""
    return bool(args) and args[0] in _ALLOWED_PREFIXES


def _find_executable() -> str | None:
    bettergi_dir = _config.get("bettergi_dir", "")
    if not bettergi_dir or not os.path.exists(bettergi_dir):
        return None
    for name in ("BetterGI.exe", "BetterGI.bat", "BetterGI.cmd"):
        path = os.path.join(bettergi_dir, name)
        if os.path.exists(path):
            return path
    return None


@app.post("/api/run")
async def run_command(req: RunRequest):
    global _process, _current_command

    _verify_token(req.token)

    if not _validate_args(req.args):
        _logger.warning(f"拒绝执行非法命令: {req.args}")
        return {
            "success": False,
            "error": "命令必须以 --startOneDragon 或 --startGroups 开头",
        }

    if _process and _process.returncode is None:
        return {"success": False, "error": "已有任务正在运行"}

    exe = _find_executable()
    if not exe:
        return {
            "success": False,
            "error": f"未找到 BetterGI.exe: {_config.get('bettergi_dir', '')}",
        }

    cmd = [exe] + req.args
    _current_command = " ".join(req.args)
    _logger.info(f"执行命令: {' '.join(cmd)}")

    try:
        env = os.environ.copy()
        env.pop("PYTHONPATH", None)
        env.pop("PYTHONHOME", None)

        _process = await asyncio.create_subprocess_exec(
            *cmd,
            cwd=_config["bettergi_dir"],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
        )
        _logger.info(f"进程已启动 (PID: {_process.pid})")
        return {"success": True, "pid": _process.pid}
    except Exception as e:
        _current_command = ""
        _logger.error(f"启动进程失败: {e}", exc_info=True)
        return {"success": False, "error": str(e)}


@app.post("/api/stop")
async def stop_command(req: StopRequest):
    global _process, _current_command

    _verify_token(req.token)

    if not _process or _process.returncode is not None:
        _process = None
        _current_command = ""
        return {"success": False, "error": "没有正在运行的任务"}

    pid = _process.pid
    _logger.info(f"正在停止进程 PID: {pid}")

    try:
        _process.terminate()
        try:
            await asyncio.wait_for(_process.wait(), timeout=5)
            _logger.info(f"进程 {pid} 已正常终止")
        except asyncio.TimeoutError:
            _logger.warning(f"进程 {pid} 超时，强制终止")
            _process.kill()
            await asyncio.wait_for(_process.wait(), timeout=3)

        _kill_child_processes(pid)
    except Exception as e:
        _logger.error(f"停止进程失败: {e}")
        return {"success": False, "error": str(e)}

    _process = None
    _current_command = ""
    return {"success": True}


@app.get("/api/status")
async def get_status(token: str | None = None):
    _verify_token(token)

    running = _process is not None and _process.returncode is None
    return {
        "is_running": running,
        "current_command": _current_command,
        "pid": _process.pid if _process and running else None,
        "bettergi_dir": _config.get("bettergi_dir", ""),
        "bettergi_found": _find_executable() is not None,
    }


@app.get("/health")
async def health():
    return {"status": "ok", "service": "bettergi-assistant"}


def _kill_child_processes(pid: int) -> None:
    try:
        parent = psutil.Process(pid)
        for child in parent.children(recursive=True):
            try:
                child.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except (psutil.NoSuchProcess, psutil.AccessDenied):
        pass


def main():
    global _config

    _config = _load_config()

    log_level = _config.get("log_level", "INFO")
    _setup_logging(log_level)

    host = _config.get("host", "0.0.0.0")
    port = _config.get("port", 9099)
    bettergi_dir = _config.get("bettergi_dir", "")
    token = _config.get("token", "")

    _logger.info("=" * 50)
    _logger.info("BetterGI 辅助程序启动")
    _logger.info(f"  监听: {host}:{port}")
    _logger.info(f"  BetterGI 目录: {bettergi_dir or '未指定'}")
    _logger.info(f"  令牌: {'已设置' if token else '未设置'}")
    _logger.info(f"  日志文件: {_LOG_PATH}")
    _logger.info("=" * 50)

    if not bettergi_dir:
        _logger.warning("未配置 bettergi_dir，运行命令时将失败")

    if not _find_executable() and bettergi_dir:
        _logger.warning(f"在 {bettergi_dir} 中未找到 BetterGI.exe，请检查路径")

    uvicorn.run(app, host=host, port=port, log_level="warning")


if __name__ == "__main__":
    main()
