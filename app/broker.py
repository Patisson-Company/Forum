"""
This module provides an implementation of an asynchronous message broker and topic-based WebSocket management using Redis.

Classes:
    - BrokerMessage: A model representing a message sent through the broker.
    - UniqueSessionError: Custom exception raised when a user attempts to create multiple sessions for the same topic.
    - Topic: Represents a topic that manages WebSocket connections and handles broadcasting messages.
    - BaseAsyncBroker: Abstract base class defining the interface for an asynchronous broker.
    - RedisAsyncBroker: Concrete implementation of BaseAsyncBroker using Redis for message distribution and user tracking.
    - AsyncBrokerSession: Manages the lifecycle of a WebSocket session with the broker.

Type Aliases:
    - WebSocketPool: A list of WebSocket connections.
    - AsyncMessageGenerator: Asynchronous generator for broker messages.
    - AwaitableMessageHandler: A callable handling broker messages and a WebSocket pool.
    - UserId: Alias for a user identifier string.
    - TopicId: Alias for a topic identifier string.
    - GetSessionWrap: A callable returning an AsyncBrokerSession instance.

Functions:
    - get_async_session: Factory function to create a callable for obtaining broker sessions.

Details:
    - BrokerMessage:
        - Fields: id, user_id, content, optional file.
        - Represents the structure of messages exchanged in topics.

    - Topic:
        - Manages WebSocket connections (`ws_pool`) for a specific topic.
        - Handles broadcasting messages from an asynchronous generator (`mes_gen`) using a handler (`mes_handler`).
        - Automatically starts and stops broadcasting tasks based on the presence of WebSocket connections.

    - RedisAsyncBroker:
        - Extends BaseAsyncBroker using Redis for message storage and user tracking.
        - Manages topics and WebSocket connections while ensuring unique sessions for users.
        - Publishes messages to Redis channels and processes incoming messages via an asynchronous generator.

    - AsyncBrokerSession:
        - Encapsulates operations to create and manage WebSocket connections for a specific topic.
        - Supports publishing messages and closing connections.
"""

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
        if ws in self.ws_pool: raise UniqueSessionError
        if len(self) == 0:
            await self.task_start()
        self.ws_pool.append(ws)

    async def del_ws(self, ws: WebSocket) -> None:
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