from typing import Literal

from config import SelfService
from db.models import Message, Topic
from patisson_request.errors import ErrorCode, ErrorSchema, UniquenessError
from patisson_request.graphql.queries import QBook
from patisson_request.service_routes import BooksRoute
from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from sqlalchemy.ext.asyncio import AsyncSession


async def create_message(session: AsyncSession, content: str,
                         topic_id: str, user_id: str) -> (
                            tuple[Literal[True], Message]
                            | tuple[Literal[False], ErrorSchema]
                            ):
    try:
        message = Message(
            user_id=user_id,
            content=content,
            topic_id=topic_id
        )
        session.add(message)
        await session.commit()
        return True, message

    except SQLAlchemyError as e:
        await session.rollback()
        return False, ErrorSchema(
            error=ErrorCode.INVALID_PARAMETERS,
            extra=str(e)
        )
        

async def create_topic(session: AsyncSession, id: str, check: bool = True) -> (
    tuple[Literal[True], Topic] | tuple[Literal[False], ErrorSchema]):
    
    try:
        if check:
            topic = await create_topic(session=session, id=id)
            if topic:
                raise UniquenessError('This topic has already been created')
            
        response = await SelfService.post_request(
            *-BooksRoute.graphql.books(
                fields=[QBook.id], limit=1, ids=[id]
            )
        )
        print(response.body.data.books)
        if len(response.body.data.books) == 0:
            raise ValueError('Incorrect topic id')
        
        topic = Topic(id=id)
        session.add(topic)
        await session.commit()
        
        return True, topic

    except SQLAlchemyError as e:
        await session.rollback()
        return False, ErrorSchema(
            error=ErrorCode.INVALID_PARAMETERS,
            extra=str(e)
        )
        
    except (UniquenessError, ValueError) as e:
        await session.rollback()
        return False, ErrorSchema(
            error=ErrorCode.VALIDATE_ERROR,
            extra=str(e)
        )
    

async def check_topic(session: AsyncSession, id: str) -> Topic | Literal[False]:
    topic = await session.execute(select(Topic).filter_by(id=id))
    return topic.scalar_one_or_none()
    
