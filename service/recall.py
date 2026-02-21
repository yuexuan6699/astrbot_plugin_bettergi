import asyncio
from asyncio import sleep

from astrbot.api.event import AstrMessageEvent
from astrbot.api import logger
from astrbot.core.message.message_event_result import MessageChain

"""
撤回消息服务
使用方式:
from .service.recall import recall_send
delay = 60  # 撤回延迟时间，单位秒
await recall_send(delay, event, message_chain)
"""

async def _recall_message_after_delay(event: AstrMessageEvent, message_id: int, delay: int) -> None:
    await sleep(delay)
    try:
        logger.debug(f"[recall] 尝试撤回消息: {message_id}, 平台: {event.get_platform_name()}")
        if event.get_platform_name() != "aiocqhttp":
            logger.debug(f"[recall] 平台不支持撤回: {event.get_platform_name()}")
            return
        from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
        if not isinstance(event, AiocqhttpMessageEvent):
            logger.debug(f"[recall] 事件类型不匹配")
            return
        await event.bot.api.call_action('delete_msg', message_id=message_id)
        logger.debug(f"[recall] 消息已撤回: {message_id}")
    except Exception as e:
        logger.warning(f"[recall] 撤回消息失败: {e}")


async def recall_send(delay: int, event: AstrMessageEvent, message_chain: MessageChain) -> None:
    logger.debug(f"[recall] 发送消息，撤回延迟: {delay}秒")
    if delay <= 0:
        await event.send(message_chain)
        return
    
    try:
        platform_name = event.get_platform_name()
        logger.debug(f"[recall] 平台: {platform_name}")
        if platform_name != "aiocqhttp":
            logger.debug(f"[recall] 非aiocqhttp平台，使用普通发送")
            await event.send(message_chain)
            return
        
        from astrbot.core.platform.sources.aiocqhttp.aiocqhttp_message_event import AiocqhttpMessageEvent
        if not isinstance(event, AiocqhttpMessageEvent):
            logger.debug(f"[recall] 事件类型不是AiocqhttpMessageEvent")
            await event.send(message_chain)
            return
        
        bot = event.bot
        is_group = bool(event.get_group_id())
        session_id = event.get_group_id() if is_group else event.get_sender_id()
        
        messages = await AiocqhttpMessageEvent._parse_onebot_json(message_chain)
        if not messages:
            logger.warning(f"[recall] 消息解析失败")
            return
        
        try:
            session_id_int = int(session_id)
        except (ValueError, TypeError):
            logger.warning(f"[recall] session_id无法转换为整数: {session_id}")
            await event.send(message_chain)
            return
        
        logger.debug(f"[recall] 发送消息到 {'群' if is_group else '私聊'}: {session_id_int}")
        
        if is_group:
            result = await bot.api.call_action('send_group_msg', group_id=session_id_int, message=messages)
        else:
            result = await bot.api.call_action('send_private_msg', user_id=session_id_int, message=messages)
        
        logger.debug(f"[recall] 发送结果: {result}")
        
        message_id = None
        if result:
            if 'data' in result and 'message_id' in result['data']:
                message_id = result['data']['message_id']
            elif 'message_id' in result:
                message_id = result['message_id']
        
        if message_id:
            logger.debug(f"[recall] 消息ID: {message_id}, 将在{delay}秒后撤回")
            asyncio.create_task(_recall_message_after_delay(event, message_id, delay))
        else:
            logger.warning(f"[recall] 未获取到message_id，无法撤回")
            
    except Exception as e:
        logger.error(f"[recall] 发送消息失败: {e}", exc_info=True)
        await event.send(message_chain)