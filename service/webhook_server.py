from collections.abc import Awaitable, Callable
from typing import Any

try:
    from astrbot.api import logger
    from astrbot.api.web import error_response, json_response, request
except ImportError:
    import logging

    logger = logging.getLogger("bettergi")

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]

PLUGIN_NAME = "bettergi"


class WebhookServer:
    """接收 BetterGI Webhook 事件通知（复用 AstrBot 主端口）。

    通过 context.register_web_api() 注册路由，不再单独开端口。
    """

    def __init__(
        self,
        path: str = "/webhook",
        token: str = "",
    ):
        # 内部注册路径加上插件名前缀，如 /bettergi/webhook
        self._internal_path = (
            f"/{PLUGIN_NAME}/{path.lstrip('/')}" if path else f"/{PLUGIN_NAME}/webhook"
        )
        self._user_path = path if path.startswith("/") else f"/{path}"
        self._token = token
        self._handler: EventHandler | None = None
        self._started = False
        self._registered = False

    def set_handler(self, handler: EventHandler) -> None:
        """设置事件处理回调函数。"""
        self._handler = handler

    async def _handle_webhook(self):
        """处理 BetterGI 的 Webhook POST 请求。"""
        try:
            logger.debug("[BetterGI-Webhook] 收到 Webhook 请求")

            if self._token:
                auth = request.headers.get("Authorization", "")
                token_val = ""
                if auth.startswith("Bearer "):
                    token_val = auth[7:]
                elif auth:
                    token_val = auth
                query_token = request.query.get("token", "")
                if token_val != self._token and query_token != self._token:
                    logger.warning("[BetterGI-Webhook] 令牌验证失败")
                    return error_response("unauthorized", status_code=401)
                logger.debug("[BetterGI-Webhook] 令牌验证通过")

            event_data = await request.json(default={})
            logger.info("[BetterGI-Webhook] 收到事件: %s", event_data.get("event"))
            logger.debug("[BetterGI-Webhook] 事件字段: %s", list(event_data.keys()))

            if self._handler:
                try:
                    await self._handler(event_data)
                except Exception as e:
                    logger.error("[BetterGI-Webhook] 事件处理失败: %s", e, exc_info=True)

            return json_response({"status": "ok"})

        except Exception as e:
            logger.error("[BetterGI-Webhook] 处理请求失败: %s", e, exc_info=True)
            return error_response(str(e), status_code=500)

    async def _handle_health(self):
        """健康检查端点。"""
        return json_response({"status": "ok", "service": "bettergi-webhook"})

    def register(self, context) -> bool:
        """通过 AstrBot context 注册 Web API 路由。"""
        if self._registered:
            return True

        try:
            context.register_web_api(
                self._internal_path,
                self._handle_webhook,
                ["POST"],
                "BetterGI Webhook 接收接口",
            )
            context.register_web_api(
                self._internal_path + "/health",
                self._handle_health,
                ["GET"],
                "BetterGI Webhook 健康检查",
            )
            self._registered = True
            self._started = True
            logger.info("[BetterGI-Webhook] 路由已注册到 AstrBot 主端口")
            logger.info("[BetterGI-Webhook] 注册路径: %s", self._internal_path)
            logger.info(
                "[BetterGI-Webhook] 完整地址: http://<AstrBot地址>:<端口>/api/v1/plugins/extensions%s",
                self._internal_path,
            )
            logger.info("[BetterGI-Webhook] 请在 BetterGI 中填写上述完整地址")
            return True
        except Exception as e:
            logger.error("[BetterGI-Webhook] 注册路由失败: %s", e, exc_info=True)
            return False

    def unregister(self) -> None:
        """取消注册（AstrBot 框架会在插件卸载时自动处理，此处标记状态）。"""
        self._started = False
        self._registered = False
        logger.info("[BetterGI-Webhook] 路由已注销")

    async def start(self) -> bool:
        """兼容旧接口，直接返回 True（路由在 register 时已注册）。"""
        return self._started

    async def stop(self) -> None:
        """停止（兼容旧接口）。"""
        self.unregister()

    @property
    def is_running(self) -> bool:
        return self._started

    @property
    def webhook_url(self) -> str:
        return f"http://<AstrBot地址>:<端口>/api/v1/plugins/extensions{self._internal_path}"
