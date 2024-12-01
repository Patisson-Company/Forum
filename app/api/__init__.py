from api.routers import router as router_v1
from api.websockets import router as ws_router
from fastapi import APIRouter

router = APIRouter()

router.include_router(router_v1, prefix='/api/v1')
router.include_router(ws_router, prefix="/ws")