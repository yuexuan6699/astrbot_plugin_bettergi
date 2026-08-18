import asyncio
import json
import os
import time
from collections import deque
from typing import Any

try:
    from astrbot.api import logger
except ImportError:
    import logging

    logger = logging.getLogger("bettergi")

MAX_EVENTS = 200
EVENTS_FILE = "bettergi_events.json"


class EventStore:
    """存储和查询 BetterGI Webhook 事件。"""

    def __init__(self, data_dir: str = ""):
        self._events: deque = deque(maxlen=MAX_EVENTS)
        self._lock = asyncio.Lock()
        self._data_dir = data_dir
        self._file_path = os.path.join(data_dir, EVENTS_FILE) if data_dir else ""
        self._load_from_file()

    def _load_from_file(self):
        if not self._file_path or not os.path.exists(self._file_path):
            return
        try:
            with open(self._file_path, encoding="utf-8") as f:
                events = json.load(f)
            if isinstance(events, list):
                self._events.extend(events[-MAX_EVENTS:])
                logger.debug(f"[BetterGI] 从文件加载了 {len(events)} 条历史事件")
        except Exception as e:
            logger.warning(f"[BetterGI] 加载历史事件失败: {e}")

    def _save_to_file(self):
        if not self._file_path:
            return
        try:
            os.makedirs(os.path.dirname(self._file_path), exist_ok=True)
            with open(self._file_path, "w", encoding="utf-8") as f:
                json.dump(list(self._events), f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.warning(f"[BetterGI] 保存事件失败: {e}")

    async def add(self, event_data: dict) -> None:
        """添加一条事件记录。"""
        record = {
            "event": event_data.get("event", "unknown"),
            "result": event_data.get("result", ""),
            "timestamp": event_data.get("timestamp", ""),
            "message": event_data.get("message", ""),
            "send_to": event_data.get("send_to", ""),
            "received_at": time.strftime("%Y-%m-%d %H:%M:%S"),
        }
        if event_data.get("screenshot"):
            record["has_screenshot"] = True

        async with self._lock:
            self._events.append(record)
            self._save_to_file()
        logger.info(f"[BetterGI] 事件已存储: {record['event']} - {record['message']}")

    async def get_recent(self, count: int = 10) -> list[dict[str, Any]]:
        """获取最近的 N 条事件。"""
        async with self._lock:
            return list(self._events)[-count:]

    async def get_by_event_type(self, event_type: str, count: int = 10) -> list[dict]:
        """按事件类型过滤获取事件。"""
        async with self._lock:
            filtered = [e for e in self._events if e.get("event") == event_type]
            return filtered[-count:]

    async def clear(self) -> int:
        """清空所有事件，返回被清除的数量。"""
        async with self._lock:
            count = len(self._events)
            self._events.clear()
            self._save_to_file()
            return count

    async def get_last_completion(self) -> dict[str, Any] | None:
        """获取最近一条任务完成事件（dragon.end 或 group.end）。"""
        async with self._lock:
            for event in reversed(self._events):
                if event.get("event") in ("dragon.end", "group.end"):
                    return event
            return None

    @staticmethod
    def format_event(event: dict) -> str:
        """将单条事件格式化为可读文本。"""
        lines = [
            f"事件: {event.get('event', 'unknown')}",
            f"结果: {event.get('result', 'N/A')}",
            f"时间: {event.get('timestamp') or event.get('received_at', 'N/A')}",
        ]
        msg = event.get("message", "")
        if msg:
            lines.append(f"消息: {msg}")
        return "\n".join(lines)

    @staticmethod
    def format_events(events: list[dict]) -> str:
        """将多条事件格式化为可读文本。"""
        if not events:
            return "暂无事件记录"
        parts = []
        for i, event in enumerate(reversed(events), 1):
            parts.append(f"--- 第{i}条 ---\n{EventStore.format_event(event)}")
        return "\n\n".join(parts)
