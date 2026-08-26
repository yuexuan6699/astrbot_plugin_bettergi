import asyncio
from datetime import datetime, timedelta
from typing import Any

try:
    from astrbot.api import logger
except ImportError:
    import logging

    logger = logging.getLogger("bettergi")


class Scheduler:
    """定时任务调度器，每天在指定时间执行一次命令。

    修复了旧版本的以下问题：
    - 插件在计划时间后启动时会立即执行任务
    - 时间计算不精确，轮询间隔过大
    - 异常处理不完善，定时器可能中断
    - 配置时间未传入调度器，永远使用 04:00
    - 字符串时间解析可能出现非数字错误
    """

    def __init__(self):
        self._task: asyncio.Task | None = None
        self._running = False
        self._last_run_date: str | None = None
        self._run_hour: int = 4
        self._run_minute: int = 0

    @property
    def is_running(self) -> bool:
        return self._running

    @property
    def last_run_date(self) -> str | None:
        return self._last_run_date

    def start(self, run_func, run_hour: int = 4, run_minute: int = 0) -> bool:
        """启动定时任务。

        Args:
            run_func: 异步函数，无参数，执行定时任务逻辑
            run_hour: 执行小时（0-23），超出范围会被自动夹紧
            run_minute: 执行分钟（0-59），超出范围会被自动夹紧
        """
        if self._running:
            logger.warning("[BetterGI-Scheduler] 定时任务已在运行")
            return False

        self._run_hour = max(0, min(23, run_hour))
        self._run_minute = max(0, min(59, run_minute))

        today = datetime.now().strftime("%Y-%m-%d")
        self._last_run_date = today

        self._running = True
        self._task = asyncio.create_task(self._loop(run_func))
        logger.info(
            "[BetterGI-Scheduler] 定时任务已启动，每天 %02d:%02d 执行",
            self._run_hour, self._run_minute,
        )
        return True

    async def stop(self) -> None:
        """停止定时任务。"""
        if not self._running:
            return

        self._running = False
        if self._task and not self._task.done():
            self._task.cancel()
            try:
                await asyncio.wait_for(self._task, timeout=5)
            except (asyncio.CancelledError, asyncio.TimeoutError):
                pass
        self._task = None
        logger.info("[BetterGI-Scheduler] 定时任务已停止")

    async def _loop(self, run_func) -> None:
        """定时任务主循环。

        使用精确等待方式：计算到下一次执行时间的秒数，精确等待。
        """
        logger.info("[BetterGI-Scheduler] 定时任务循环已启动")

        while self._running:
            try:
                wait_seconds = self._calc_wait_seconds(self._run_hour, self._run_minute)
                if wait_seconds > 0:
                    logger.info(
                        f"[BetterGI-Scheduler] 下次执行需等待 {wait_seconds:.0f} 秒"
                        f"（约 {wait_seconds / 3600:.1f} 小时）"
                    )
                    # 分段等待，便于响应取消
                    while wait_seconds > 0 and self._running:
                        sleep_time = min(wait_seconds, 60)
                        await asyncio.sleep(sleep_time)
                        wait_seconds -= sleep_time

                if not self._running:
                    break

                logger.info("[BetterGI-Scheduler] 开始执行定时任务")
                self._last_run_date = datetime.now().strftime("%Y-%m-%d")

                try:
                    await run_func()
                    logger.info("[BetterGI-Scheduler] 定时任务执行完成")
                except Exception as e:
                    logger.error(
                        f"[BetterGI-Scheduler] 定时任务执行失败: {e}",
                        exc_info=True,
                    )

                # 执行完后短暂等待，避免重复触发
                await asyncio.sleep(60)

            except asyncio.CancelledError:
                logger.info("[BetterGI-Scheduler] 定时任务被取消")
                break
            except Exception as e:
                logger.error(
                    f"[BetterGI-Scheduler] 循环异常（定时器继续运行）: {e}",
                    exc_info=True,
                )
                await asyncio.sleep(60)

        self._running = False
        logger.info("[BetterGI-Scheduler] 定时任务循环已退出")

    def _calc_wait_seconds(self, hour: int, minute: int) -> float:
        """计算到下一次执行时间的等待秒数。

        如果今天的执行时间还没到，等到今天的时间点。
        如果今天的执行时间已过，等到明天的同一时间。

        Args:
            hour: 执行小时（已在 start 中校验为 0-23）
            minute: 执行分钟（已在 start 中校验为 0-59）
        """
        now = datetime.now()
        today_target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)

        if now < today_target:
            target = today_target
        else:
            target = today_target + timedelta(days=1)

        return (target - now).total_seconds()

    def get_status(self) -> dict[str, Any]:
        """获取定时任务状态。"""
        return {
            "is_running": self._running,
            "last_run_date": self._last_run_date,
        }
