import asyncio
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from config import DATABASE_URL
from sqlalchemy.ext.asyncio import AsyncSession, create_async_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

engine = create_async_engine(DATABASE_URL, echo=True, future=True)
Base = declarative_base()

async_session = sessionmaker(  # type: ignore[reportCallIssue]
    bind=engine,  # type: ignore[reportArgumentType]
    class_=AsyncSession,
    expire_on_commit=False
)

@asynccontextmanager
async def get_session() -> AsyncGenerator[AsyncSession, None]:
    async with async_session() as session:  # type: ignore[reportGeneralTypeIssues]
        yield session
        
def _db_init():
    async def create_tables():
        async with engine.begin() as conn:
            await conn.run_sync(Base.metadata.create_all)
    loop = asyncio.get_event_loop()
    if loop.is_running():
        return loop.create_task(create_tables())
    else:
        loop.run_until_complete(create_tables())