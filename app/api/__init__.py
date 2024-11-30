from api.websockets import router as ws_router
from fastapi import APIRouter

router = APIRouter()

router.include_router(ws_router, prefix="/api/v1")