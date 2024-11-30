from typing import Annotated

import config
from broker import (BrokerMessage, GetSessionWrap, RedisAsyncBroker,
                    WebSocketPool, get_async_session)
from config import logger
from db.base import get_session
from fastapi import Depends, Header, HTTPException, status
from opentelemetry import trace
from patisson_request.depends import (dep_jaeger_client_decorator,
                                      verify_client_token_dep)
from patisson_request.errors import ErrorCode, ErrorSchema, InvalidJWT
from patisson_request.jwt_tokens import ClientAccessTokenPayload
from sqlalchemy.ext.asyncio import AsyncSession

tracer = trace.get_tracer(__name__)

async def websockets_broadcast(message: BrokerMessage, ws_pool: WebSocketPool) -> None:
    logger.debug(f'broadcasting has started (message: {message})')
    for ws in ws_pool:
        try:
            await ws.send_json(message.model_dump())
        except Exception as e:
            logger.warning(f'error: {e}, message: {message}')

broker = RedisAsyncBroker(
    mes_handle=websockets_broadcast
)


@dep_jaeger_client_decorator(tracer)
async def verify_user_token(
    X_Client_Token: str = Header(...)
    ) -> ClientAccessTokenPayload:
    try:
        payload = await verify_client_token_dep(
            self_service=config.SelfService,
            access_token=X_Client_Token
        )
    except InvalidJWT as e:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=[e.error_schema.model_dump()]
            )
    return payload


async def verify_user__use_chat__token(
    payload: ClientAccessTokenPayload = Depends(verify_user_token)
    ) -> ClientAccessTokenPayload:
    REQUIRED_PERM = [
        payload.role.permissions.use_chat
    ]
    if not all(REQUIRED_PERM):
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail=[ErrorSchema(
                error=ErrorCode.ACCESS_ERROR
                ).model_dump()]
            )
    return payload



SessionDep = Annotated[AsyncSession, Depends(get_session)]
BrokerSession = Annotated[GetSessionWrap, Depends(get_async_session(broker=broker))]

UserJWT = Annotated[ClientAccessTokenPayload, Depends(verify_user_token)]
UseChat_UserJWT = Annotated[ClientAccessTokenPayload, Depends(verify_user__use_chat__token)]