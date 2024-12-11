import asyncio
import json
from abc import ABC, abstractmethod
from functools import cache
from typing import (AsyncGenerator, Awaitable, Callable, NewType, NoReturn, Optional,
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
UserId: TypeAlias = str
TopicId: TypeAlias = str

class UniqueSessionError(Exception): ...

class Topic:
    
    def __init__(self, id: TopicId, mes_gen: AsyncMessageGenerator, 
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
        print('1. add_ws', len(self.ws_pool))
        if ws in self.ws_pool: raise UniqueSessionError
        if len(self) == 0:
            await self.task_start()
        self.ws_pool.append(ws)

    async def del_ws(self, ws: WebSocket) -> None:
        print('1. del_ws', len(self.ws_pool))
        try:
            await ws.close()
        except: pass
        try:
            self.ws_pool.remove(ws)
        except ValueError: pass
        if len(self) == 0: 
            try:
                await self.task_stop()
            except: pass
        
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
    def get_topic_by_id(self, topic_id: TopicId) -> Topic | None: ...
    
    @abstractmethod
    def create_topic(self, topic_id: TopicId) -> Topic: ...
    
    @abstractmethod
    async def add_ws(self, topic_id: TopicId, ws: WebSocket, user_id: UserId) -> None: ...
    
    @abstractmethod
    async def del_ws(self, topic_id: TopicId, ws: WebSocket, user_id: UserId) -> None: ...
    
    @abstractmethod
    async def publish(self, topic_id: TopicId, message: BrokerMessage) -> None: ...
    
    @abstractmethod
    async def _message_gen(self, topic_id: TopicId) -> AsyncMessageGenerator: ...
    
    
class RedisAsyncBroker(BaseAsyncBroker):
    
    def __init__(self, mes_handle: AwaitableMessageHandler,
                 host: str = "localhost", port: int = 6380) -> None:
        self.redis = aioredis.Redis(host=host, port=port)
        self.topics: list[Topic] = []
        self.mes_handle = mes_handle

    @cache
    def get_topic_by_id(self, topic_id: TopicId) -> Topic | None:  # type: ignore[reportIncompatibleMethodOverride]
        for topic in self.topics:
            if topic.id == topic_id: 
                return topic
    
    def create_topic(self, topic_id: TopicId) -> Topic:
        topic = Topic(
            id=topic_id,
            mes_gen=self._message_gen(topic_id),
            mes_handler=self.mes_handle
            )
        self.topics.append(topic)
        self.get_topic_by_id.cache_clear()
        return topic
    
    async def add_ws(self, topic_id: TopicId, ws: WebSocket, user_id: UserId) -> None:
        topic = self.get_topic_by_id(topic_id)
        if not topic: 
            topic = self.create_topic(topic_id)
        if await self.redis.sismember(topic_id, user_id) == 1:  # type: ignore[reportGeneralTypeIssues]
            raise UniqueSessionError('The user has an active connection to this topic')
        await self.redis.sadd(topic_id, user_id)  # type: ignore[reportGeneralTypeIssues]
        await topic.add_ws(ws)

    async def del_ws(self, topic_id: TopicId, ws: WebSocket, user_id: UserId) -> None:
        try:
            await self.redis.srem(topic_id, user_id)  # type: ignore[reportGeneralTypeIssues]
        except: pass
        await self.get_topic_by_id(topic_id).del_ws(ws)  # type: ignore[reportOptionalMemberAccess]

    async def publish(self, topic_id: TopicId, message: BrokerMessage) -> None:
        topic = self.get_topic_by_id(topic_id)
        if not topic: 
            topic = self.create_topic(topic_id)
        await self.redis.publish(topic.id, json.dumps(message.model_dump()))

    async def _message_gen(self, topic_id: TopicId) -> AsyncMessageGenerator:
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
                 topic_id: TopicId, ws: WebSocket, 
                 user_id: UserId) -> None:
        self.broker = broker
        self.topic_id = topic_id
        self.ws = ws
        self.user_id = user_id
    
    async def create_connection(self) -> None:
        await self.broker.add_ws(
            topic_id=self.topic_id, 
            ws=self.ws, user_id=self.user_id
            )
    
    async def publish(self, message: BrokerMessage) -> None:
        await self.broker.publish(
            topic_id=self.topic_id,
            message=message
        )
        
    async def close_connection(self) -> None:
        await self.broker.del_ws(
            topic_id=self.topic_id,
            ws=self.ws, user_id=self.user_id
        )
        

GetSessionWrap: TypeAlias = Callable[[TopicId, WebSocket, UserId], AsyncBrokerSession]

def get_async_session(broker: BaseAsyncBroker) -> Callable[[], GetSessionWrap]:
    def wrap() -> GetSessionWrap:
        def get_session(topic_id: TopicId, ws: WebSocket, user_id: UserId) -> AsyncBrokerSession:
            return AsyncBrokerSession(
                broker=broker,
                topic_id=topic_id,
                ws=ws, user_id=user_id
            )
        return get_session
    return wrap