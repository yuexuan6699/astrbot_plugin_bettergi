import asyncio
import json
import time
import uuid
from collections.abc import AsyncGenerator

from astrbot.api import logger


class PendingTask:
    """待执行任务。"""

    def __init__(self, task_id: str, args: list[str]):
        self.task_id = task_id
        self.args = args
        self.event = asyncio.Event()
        self.result: dict | None = None
        self.error: str | None = None


class RemoteConnectionManager:
    """管理远程辅助程序的 SSE 连接和任务调度。

    辅助程序通过 SSE 连接上来，插件通过 SSE 下发任务指令，
    辅助程序执行后通过 POST /result 上报结果。
    """

    def __init__(self, token: str = ""):
        self._token = token
        self._connected = False
        self._pending_tasks: dict[str, PendingTask] = {}
        self._current_task: PendingTask | None = None
        self._lock = asyncio.Lock()
        self._connect_event = asyncio.Event()
        self._command_queue: asyncio.Queue[dict] = asyncio.Queue()

    @property
    def is_connected(self) -> bool:
        return self._connected

    async def wait_for_connection(self, timeout: float = 30) -> bool:
        """等待辅助程序连接。"""
        if self._connected:
            return True
        try:
            await asyncio.wait_for(self._connect_event.wait(), timeout=timeout)
            return True
        except asyncio.TimeoutError:
            return False

    async def sse_stream(self) -> AsyncGenerator[str, None]:
        """SSE 事件流，供辅助程序连接。"""
        if self._connected:
            logger.warning("[BetterGI-Remote] 已有辅助程序连接，拒绝新连接")
            yield "event: error\ndata: {\"message\": \"already connected\"}\n\n"
            return

        self._connected = True
        self._connect_event.set()
        logger.info("[BetterGI-Remote] 辅助程序已连接")

        try:
            # 发送连接成功事件
            yield "event: connected\ndata: {\"status\": \"ok\"}\n\n"

            # 持续从队列中取任务并下发
            while True:
                task = await self._command_queue.get()
                data = json.dumps(task, ensure_ascii=False)
                logger.debug("[BetterGI-Remote] 下发任务: %s", task.get("type"))
                yield f"event: command\ndata: {data}\n\n"
        except asyncio.CancelledError:
            logger.info("[BetterGI-Remote] SSE 连接已断开")
        except Exception as e:
            logger.error("[BetterGI-Remote] SSE 连接异常: %s", e)
        finally:
            self._connected = False
            self._connect_event.clear()
            # 清理所有未完成的任务
            async with self._lock:
                for task in self._pending_tasks.values():
                    task.error = "连接断开"
                    task.event.set()
                self._pending_tasks.clear()
                self._current_task = None
            logger.info("[BetterGI-Remote] 辅助程序已断开")

    async def submit_task(self, args: list[str], timeout: float = 3600) -> tuple[bool, str]:
        """提交任务并等待结果。返回 (成功, 消息)。"""
        if not self._connected:
            return False, "辅助程序未连接"

        task_id = str(uuid.uuid4())
        pending = PendingTask(task_id, args)

        async with self._lock:
            self._pending_tasks[task_id] = pending

        try:
            await self._command_queue.put({
                "type": "run",
                "task_id": task_id,
                "args": args,
                "timestamp": time.time(),
            })

            # 等待结果
            await asyncio.wait_for(pending.event.wait(), timeout=timeout)

            if pending.error:
                return False, pending.error
            if pending.result:
                success = pending.result.get("success", False)
                message = pending.result.get("message", "")
                return success, message
            return False, "未知结果"

        except asyncio.TimeoutError:
            async with self._lock:
                self._pending_tasks.pop(task_id, None)
            return False, "任务执行超时"
        except Exception as e:
            async with self._lock:
                self._pending_tasks.pop(task_id, None)
            return False, f"任务执行失败: {e}"

    async def handle_result(self, result_data: dict) -> bool:
        """处理辅助程序上报的结果。"""
        task_id = result_data.get("task_id", "")
        success = result_data.get("success", False)

        logger.debug(
            "[BetterGI-Remote] 收到任务结果: task_id=%s, success=%s",
            task_id, success,
        )

        async with self._lock:
            pending = self._pending_tasks.pop(task_id, None)

        if not pending:
            logger.warning("[BetterGI-Remote] 收到未知任务的结果: %s", task_id)
            return False

        pending.result = result_data
        pending.event.set()
        return True

    async def submit_stop(self) -> tuple[bool, str]:
        """发送停止指令。"""
        if not self._connected:
            return False, "辅助程序未连接"

        task_id = f"stop_{int(time.time())}"
        pending = PendingTask(task_id, [])

        async with self._lock:
            self._pending_tasks[task_id] = pending

        try:
            await self._command_queue.put({
                "type": "stop",
                "task_id": task_id,
                "timestamp": time.time(),
            })

            await asyncio.wait_for(pending.event.wait(), timeout=30)

            if pending.error:
                return False, pending.error
            return True, "已停止"
        except asyncio.TimeoutError:
            async with self._lock:
                self._pending_tasks.pop(task_id, None)
            return False, "停止超时"

    async def get_status(self) -> dict:
        """获取远程状态（通过 ping 或直接返回连接状态）。"""
        if not self._connected:
            return {"mode": "remote", "is_running": False, "error": "未连接"}

        # 简单返回连接状态，详细状态可以通过额外的 ping 命令获取
        return {"mode": "remote", "is_running": False, "connected": True}
