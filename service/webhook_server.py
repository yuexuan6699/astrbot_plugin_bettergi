import json
from collections.abc import Awaitable, Callable
from typing import Any

from aiohttp import web

try:
    from astrbot.api import logger
except ImportError:
    import logging

    logger = logging.getLogger("bettergi")

EventHandler = Callable[[dict[str, Any]], Awaitable[None]]


class WebhookServer:
    """接收 BetterGI Webhook 事件通知的 HTTP 服务器。"""

    def __init__(
        self,
        host: str = "0.0.0.0",
        port: int = 8088,
        path: str = "/bettergi",
        token: str = "",
    ):
        self._host = host
        self._port = port
        self._path = path if path.startswith("/") else f"/{path}"
        self._token = token
        self._handler: EventHandler | None = None
        self._runner: web.AppRunner | None = None
        self._site: web.TCPSite | None = None
        self._started = False

    def set_handler(self, handler: EventHandler) -> None:
        """设置事件处理回调函数。"""
        self._handler = handler

    async def _handle_webhook(self, request: web.Request) -> web.Response:
        """处理 BetterGI 的 Webhook POST 请求。"""
        try:
            logger.debug("[BetterGI-Webhook] 收到请求: method=%s, path=%s", request.method, request.path)

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
                    return web.json_response({"error": "unauthorized"}, status=401)
                logger.debug("[BetterGI-Webhook] 令牌验证通过")

            body = await request.read()
            logger.debug("[BetterGI-Webhook] 请求体大小: %d 字节", len(body))
            if not body:
                logger.warning("[BetterGI-Webhook] 请求体为空")
                return web.json_response({"error": "empty body"}, status=400)

            event_data = json.loads(body)
            logger.info("[BetterGI-Webhook] 收到事件: %s", event_data.get("event"))
            logger.debug("[BetterGI-Webhook] 事件字段: %s", list(event_data.keys()))

            if self._handler:
                try:
                    await self._handler(event_data)
                except Exception as e:
                    logger.error("[BetterGI-Webhook] 事件处理失败: %s", e, exc_info=True)

            return web.json_response({"status": "ok"})

        except json.JSONDecodeError:
            logger.warning("[BetterGI-Webhook] 请求体不是有效的 JSON: %s", body[:200])
            return web.json_response({"error": "invalid json"}, status=400)
        except Exception as e:
            logger.error("[BetterGI-Webhook] 处理请求失败: %s", e, exc_info=True)
            return web.json_response({"error": str(e)}, status=500)

    async def _handle_health(self, request: web.Request) -> web.Response:
        """健康检查端点。"""
        return web.json_response({"status": "ok", "service": "bettergi-webhook"})

    def _create_app(self) -> web.Application:
        app = web.Application()
        app.router.add_post(self._path, self._handle_webhook)
        app.router.add_get("/health", self._handle_health)
        return app

    async def start(self) -> bool:
        """启动 HTTP 服务器。"""
        if self._started:
            logger.warning("[BetterGI-Webhook] 服务器已在运行")
            return True

        try:
            app = self._create_app()
            self._runner = web.AppRunner(app)
            await self._runner.setup()
            self._site = web.TCPSite(self._runner, self._host, self._port)
            await self._site.start()
            self._started = True
            webhook_url = f"http://<IP>:{self._port}{self._path}"
            logger.info(
                f"[BetterGI-Webhook] 服务器已启动，监听 {self._host}:{self._port}"
            )
            logger.info(f"[BetterGI-Webhook] Webhook 地址: {webhook_url}")
            logger.info("[BetterGI-Webhook] 请在 BetterGI 设置中配置此地址")
            return True
        except OSError as e:
            logger.error(f"[BetterGI-Webhook] 端口 {self._port} 被占用或无法绑定: {e}")
            return False
        except Exception as e:
            logger.error(f"[BetterGI-Webhook] 启动失败: {e}", exc_info=True)
            return False

    async def stop(self) -> None:
        """停止 HTTP 服务器。"""
        if not self._started:
            return

        if self._site:
            await self._site.stop()
            self._site = None
        if self._runner:
            await self._runner.cleanup()
            self._runner = None
        self._started = False
        logger.info("[BetterGI-Webhook] 服务器已停止")

    @property
    def is_running(self) -> bool:
        return self._started

    @property
    def webhook_url(self) -> str:
        return f"http://<IP>:{self._port}{self._path}"
