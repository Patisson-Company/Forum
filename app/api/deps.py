from typing import Annotated

from fastapi import WebSocket, security, WebSocketException
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

import config
from broker import (BrokerMessage, GetSessionWrap, RedisAsyncBroker,
                    WebSocketPool, get_async_session)
from config import logger, SelfService
from db.base import get_session
from fastapi import Depends, Header, HTTPException, status
from opentelemetry import trace
from patisson_request.depends import (dep_opentelemetry_client_decorator,
                                      dep_opentelemetry_service_decorator,
                                      verify_client_token_dep,
                                      verify_service_token_dep)
from patisson_request.errors import ErrorCode, ErrorSchema, InvalidJWT
from patisson_request.jwt_tokens import (ClientAccessTokenPayload,
                                         ServiceAccessTokenPayload)
from sqlalchemy.ext.asyncio import AsyncSession

tracer = trace.get_tracer(__name__)
security = HTTPBearer()

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


@dep_opentelemetry_service_decorator(tracer)
async def verify_service_token(
    credentials: HTTPAuthorizationCredentials = Depends(security)
    ) -> ServiceAccessTokenPayload:
    token = credentials.credentials
    try:
        payload = await verify_service_token_dep(
            self_service=config.SelfService, access_token=token)
    except InvalidJWT as e:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail=[e.error_schema.model_dump()]
            )
    return payload


@dep_opentelemetry_service_decorator(tracer)
async def ws_verify_service_token(ws: WebSocket) -> ServiceAccessTokenPayload:
    try:
        token_header = ws.headers.get('authorization')
        if not token_header: raise InvalidJWT(
            ErrorSchema(
                error=ErrorCode.JWT_INVALID,
                extra='the Authorization token is missing'
            )
        )
        payload = await verify_service_token_dep(
            self_service=config.SelfService, 
            access_token=SelfService.extract_token_from_header(token_header))
    except InvalidJWT as e:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=str(e.error_schema.model_dump())
        )
    return payload  # type: ignore[reportPossiblyUnboundVariable]


async def verify_serice__forum_access__token(
    payload: ServiceAccessTokenPayload = Depends(verify_service_token)
    ) -> ServiceAccessTokenPayload:
    REQUIRED_PERM = [
        payload.role.permissions.forum_access
    ]
    if not all(REQUIRED_PERM):
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=str(ErrorSchema(error=ErrorCode.ACCESS_ERROR).model_dump())
        )
    return payload


async def ws_verify_serice__forum_access__token(
    payload: ServiceAccessTokenPayload = Depends(ws_verify_service_token), 
    ) -> ServiceAccessTokenPayload:
    REQUIRED_PERM = [
        payload.role.permissions.forum_access
    ]
    if not all(REQUIRED_PERM):
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=str(ErrorSchema(
                error=ErrorCode.ACCESS_ERROR
                ).model_dump())
        )
    return payload




@dep_opentelemetry_client_decorator(tracer)
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


@dep_opentelemetry_client_decorator(tracer)
async def ws_verify_user_token(ws: WebSocket) -> ClientAccessTokenPayload:
    try:
        token = ws.headers.get('x-client-token')
        if not token: raise InvalidJWT(
            ErrorSchema(
                error=ErrorCode.JWT_INVALID,
                extra='the X-Client-Token token is missing'
            )
        )
        payload = await verify_client_token_dep(
            self_service=config.SelfService, access_token=token)
    except InvalidJWT as e:
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=str(e.error_schema.model_dump())
        )
    return payload  # type: ignore[reportPossiblyUnboundVariable]


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


async def ws_verify_user__use_chat__token(
    payload: ClientAccessTokenPayload = Depends(ws_verify_user_token)
    ) -> ClientAccessTokenPayload:
    REQUIRED_PERM = [
        payload.role.permissions.use_chat
    ]
    if not all(REQUIRED_PERM):
        logger.info(f'user {payload.sub} does not have access to the chat')
        raise WebSocketException(
            code=status.WS_1008_POLICY_VIOLATION,
            reason=str(ErrorSchema(error=ErrorCode.ACCESS_ERROR).model_dump())
        )
    return payload



SessionDep = Annotated[AsyncSession, Depends(get_session)]
BrokerSession = Annotated[GetSessionWrap, Depends(get_async_session(broker=broker))]

ServiceJWT = Annotated[ServiceAccessTokenPayload, Depends(verify_service_token)]
ServiceJWT_ForumAccess = Annotated[ServiceAccessTokenPayload, Depends(verify_serice__forum_access__token)]
WS_ServiceJWT = Annotated[ServiceAccessTokenPayload, Depends(ws_verify_service_token)]
WS_ServiceJWT_ForumAccess = Annotated[ServiceAccessTokenPayload, Depends(ws_verify_serice__forum_access__token)]

UserJWT = Annotated[ClientAccessTokenPayload, Depends(verify_user_token)]
UseChat_UserJWT = Annotated[ClientAccessTokenPayload, Depends(verify_user__use_chat__token)]
WS_UserJWT = Annotated[ClientAccessTokenPayload, Depends(ws_verify_user_token)]
WS_UseChat_UserJWT = Annotated[ClientAccessTokenPayload, Depends(ws_verify_user__use_chat__token)]
