from typing import Optional

from api.deps import (BrokerSession, WS_ServiceJWT_ForumAccess,
                      WS_UseChat_UserJWT)
from broker import BrokerMessage, UniqueSessionError
from config import SelfService, logger
from db.base import get_session
from db.crud import check_topic, create_message, create_topic
from fastapi import APIRouter, WebSocket, WebSocketException, status
from patisson_request.service_routes import InternalMediaRoute
from pydantic import BaseModel
from starlette.websockets import WebSocketDisconnect

router = APIRouter()

class ReceiveMessage(BaseModel):
    content: str
    file: Optional[bytes] = None
    
@router.websocket("/topic/{topic_id}")
async def websocket_route(service: WS_ServiceJWT_ForumAccess,
                          user: WS_UseChat_UserJWT, 
                          broker_session_wrap: BrokerSession, 
                          topic_id: str, websocket: WebSocket) -> None:
    
    async with get_session() as session_:
        topic = await check_topic(session=session_, id=topic_id)
        if not topic:
            is_valid, body = await create_topic(
                session=session_, id=topic_id, check=False
                )
            if not is_valid:
                e = 'invalid topic received'
                logger.info(e) 
                raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason=e)

    broker_session = broker_session_wrap(topic_id, websocket, user.sub)
    try:
        await broker_session.create_connection()
        logger.debug(f'create connection by {service.sub} for {user.sub}')
    except UniqueSessionError as e:
        logger.info(e)    
        raise WebSocketException(code=status.WS_1008_POLICY_VIOLATION, reason=str(e))
    
    try:
        await websocket.accept()
        while True:
            message_dict = await websocket.receive_json()
            message = ReceiveMessage(**message_dict)
            logger.debug(f'publish message: {message}')
            
            if message.file:
                response = await SelfService.post_request(
                    *-InternalMediaRoute.api.v1.upload(message.file)
                )
                file_id = response.body.id
            else:
                file_id = None
            
            async with get_session() as session_:
                is_valid, body = await create_message(
                    session=session_,
                    content=message.content,
                    topic_id=topic_id,
                    user_id=user.sub
                )
            if not is_valid:
                raise Exception(body)
            
            await broker_session.publish(BrokerMessage(
                id=body.id,  # type: ignore[reportArgumentType]
                user_id=user.sub,
                content=message.content,
                file=file_id
            ))
            
    except WebSocketDisconnect:
        await broker_session.close_connection()
        logger.debug(f'close connection by {service.sub} for {user.sub}')
        
    except Exception as e:
        await broker_session.close_connection()
        logger.critical(e)
        raise WebSocketException(code=status.WS_1011_INTERNAL_ERROR)