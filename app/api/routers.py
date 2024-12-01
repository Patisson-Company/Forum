import datetime

from api.deps import SessionDep, UseChat_UserJWT
from config import logger
from db.crud import get_message_by_topic
from fastapi import APIRouter
from pydantic import BaseModel

router = APIRouter()

class MessagePydantic(BaseModel):
    id: str
    user_id: str
    content: str
    timestamp: datetime.datetime
    topic_id: str
    is_delete: bool


@router.get('/messages')
async def messages_route(user: UseChat_UserJWT, session: SessionDep, topic_id: str,
                         limit: int = 10, offset: int = 0) -> list[MessagePydantic]:
    async with session as session_:
        messages = await get_message_by_topic(
            session=session_, topic_id=topic_id,
            limit=limit, offset=offset
            )
    logger.debug(f'user {user.sub} requested {limit} messages, offset {offset} from {topic_id}')
    return [
        MessagePydantic(
            id=message.id,  # type: ignore[reportArgumentType]
            user_id=message.user_id,  # type: ignore[reportArgumentType]
            content=message.content,  # type: ignore[reportArgumentType]
            timestamp=message.timestamp,  # type: ignore[reportArgumentType]
            topic_id=message.topic_id,  # type: ignore[reportArgumentType]
            is_delete=message.is_delete  # type: ignore[reportArgumentType]
        ) for message in messages
        ]