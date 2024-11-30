import asyncio
import json
from abc import ABC, abstractmethod
from functools import cache
from typing import (AsyncGenerator, Awaitable, Callable, NoReturn, Optional,
                    TypeAlias)

import redis.asyncio as aioredis
from fastapi import WebSocket
from pydantic import BaseModel


class BrokerMessage(BaseModel):
    id: str
    user_id: str    
    content: str
    file: Optional[str] = None
    
WebSocketPool: TypeAlias = list[WebSocket]
AsyncMessageGenerator: TypeAlias = AsyncGenerator[BrokerMessage, NoReturn]
AwaitableMessageHandler: TypeAlias = Callable[[BrokerMessage, WebSocketPool], Awaitable[None]]


class Topic:
    
    def __init__(self, id: str, mes_gen: AsyncMessageGenerator, 
                 mes_handler: AwaitableMessageHandler,
                 ws_pool: list[WebSocket] = []) -> None:
        self.id = id
        self.ws_pool = ws_pool
        self.mes_gen = mes_gen
        self.mes_handler = mes_handler

    def __contains__(self, topic_id: str) -> bool:
        return self.id == topic_id

    def __len__(self) -> int:
        return len(self.ws_pool)

    async def _broadcast_task(self, mes_gen: AsyncMessageGenerator,
                              mes_handler: AwaitableMessageHandler) -> NoReturn:
        async for message in mes_gen:
            await mes_handler(message, self.ws_pool)
            
    async def add_ws(self, ws: WebSocket) -> None:
        if ws in self.ws_pool: return
        if len(self) == 0:
            await self.task_start()
        self.ws_pool.append(ws)

    async def del_ws(self, ws: WebSocket) -> None:
        try:
            await ws.close()
        except: pass
        self.ws_pool.remove(ws)
        if len(self) == 0: 
            await self.task_stop()
        
    async def task_start(self) -> None:
        self.task = asyncio.create_task(
            self._broadcast_task(
                mes_gen=self.mes_gen, 
                mes_handler=self.mes_handler
                )
            )
        
    async def task_stop(self) -> None:
        self.task.cancel()
        await self.task
        
        
        
        
class BaseAsyncBroker(ABC):
    
    @abstractmethod
    def __init__(self, mes_handler: AwaitableMessageHandler, **kwargs) -> None: ...

    @abstractmethod
    def get_topic_by_id(self, topic_id) -> Topic | None: ...
    
    @abstractmethod
    def create_topic(self, topic_id: str) -> Topic: ...
    
    @abstractmethod
    async def add_ws(self, topic_id: str, ws: WebSocket) -> None: ...
    
    @abstractmethod
    async def del_ws(self, topic_id: str, ws: WebSocket) -> None: ...
    
    @abstractmethod
    async def publish(self, topic_id: str, message: BrokerMessage) -> None: ...
    
    @abstractmethod
    async def _message_gen(self, topic_id: str) -> AsyncMessageGenerator: ...
    
    
class RedisAsyncBroker(BaseAsyncBroker):
    
    def __init__(self, mes_handle: AwaitableMessageHandler,
                 host: str = "localhost", port: int = 6380) -> None:
        self.redis = aioredis.Redis(host=host, port=port)
        self.topics: list[Topic] = []
        self.mes_handle = mes_handle

    @cache
    def get_topic_by_id(self, topic_id: str) -> Topic | None:
        for topic in self.topics:
            if topic.id == topic_id: 
                return topic
    
    def create_topic(self, topic_id: str) -> Topic:
        topic = Topic(
            id=topic_id,
            mes_gen=self._message_gen(topic_id),
            mes_handler=self.mes_handle
            )
        self.topics.append(topic)
        self.get_topic_by_id.cache_clear()
        return topic
    
    async def add_ws(self, topic_id: str, ws: WebSocket) -> None:
        topic = self.get_topic_by_id(topic_id)
        if not topic: 
            topic = self.create_topic(topic_id)
        await topic.add_ws(ws)

    async def del_ws(self, topic_id: str, ws: WebSocket) -> None:
        await self.get_topic_by_id(topic_id).del_ws(ws)  # type: ignore[reportOptionalMemberAccess]

    async def publish(self, topic_id: str, message: BrokerMessage) -> None:
        topic = self.get_topic_by_id(topic_id)
        if not topic: 
            topic = self.create_topic(topic_id)
        await self.redis.publish(topic.id, json.dumps(message.model_dump()))

    async def _message_gen(self, topic_id: str) -> AsyncMessageGenerator:
        pubsub = self.redis.pubsub()
        await pubsub.subscribe(topic_id)
        try:
            while True:
                message = await pubsub.get_message(ignore_subscribe_messages=True)
                if message and message["type"] == "message":
                    yield BrokerMessage(**json.loads(message["data"]))
        finally:
            await pubsub.unsubscribe(topic_id)
            await pubsub.close()
            
            
        
        
class AsyncBrokerSession:
    
    def __init__(self, broker: BaseAsyncBroker, 
                 topic_id: str, ws: WebSocket):
        self.broker = broker
        self.topic_id = topic_id
        self.ws = ws
    
    async def create_connection(self) -> None:
        await self.broker.add_ws(self.topic_id, self.ws)
    
    async def publish(self, message: BrokerMessage) -> None:
        await self.broker.publish(
            topic_id=self.topic_id,
            message=message
        )
        
    async def close_connection(self) -> None:
        await self.broker.del_ws(
            topic_id=self.topic_id,
            ws=self.ws
        )
        

GetSessionWrap: TypeAlias = Callable[[str, WebSocket], AsyncBrokerSession]

def get_async_session(broker: BaseAsyncBroker) -> Callable[[], GetSessionWrap]:
    def wrap() -> GetSessionWrap:
        def get_session(topic_id: str, ws: WebSocket) -> AsyncBrokerSession:
            return AsyncBrokerSession(
                broker=broker,
                topic_id=topic_id,
                ws=ws
            )
        return get_session
    return wrap