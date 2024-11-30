from typing import Optional

from api.deps import BrokerSession, UseChat_UserJWT
from broker import BrokerMessage
from config import SelfService, logger
from db.base import get_session
from db.crud import check_topic, create_message, create_topic
from fastapi import (APIRouter, HTTPException, WebSocket, WebSocketDisconnect,
                     status)
from patisson_request.service_routes import InternalMediaRoute
from pydantic import BaseModel

router = APIRouter()

class ReceiveMessage(BaseModel):
    content: str
    file: Optional[bytes] = None
    
@router.websocket("/topic/{topic_id}")
async def websocket_route(user: UseChat_UserJWT, broker_session_wrap: BrokerSession, 
                          topic_id: str, websocket: WebSocket) -> None:
    
    async with get_session() as session_:
        topic = await check_topic(session=session_, id=topic_id)
        if not topic:
            is_valid, body = await create_topic(
                session=session_, id=topic_id, check=False
                )
            if not is_valid:
                raise HTTPException(
                    status_code=status.HTTP_400_BAD_REQUEST,
                    detail=[body.model_dump()]
                )

    broker_session = broker_session_wrap(topic_id, websocket)
    await websocket.accept()
    try:
        await broker_session.create_connection()
        logger.debug(f'create connection {websocket}')
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
        logger.debug(f'close connection {websocket}')
        
    except Exception as e:
        await broker_session.close_connection()
        logger.critical(e)
        raise e