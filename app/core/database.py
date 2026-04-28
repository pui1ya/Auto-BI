from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy import create_engine
from sqlalchemy.orm import declarative_base, sessionmaker
from app.core.config import settings

# Async engine for FastAPI
async_engine = create_async_engine(settings.DATABASE_URL, echo=settings.DEBUG)
AsyncSessionLocal = async_sessionmaker(async_engine, expire_on_commit=False, class_=AsyncSession)

# Sync engine for ETL / analytics (pandas, sklearn etc.)
sync_engine = create_engine(settings.DATABASE_URL_SYNC, echo=False)
SyncSessionLocal = sessionmaker(bind=sync_engine)

Base = declarative_base()


async def get_db() -> AsyncSession:
    async with AsyncSessionLocal() as session:
        yield session


async def init_db():
    """Create all tables on startup."""
    async with async_engine.begin() as conn:
        from app.models import schemas  # noqa: F401 – registers models
        await conn.run_sync(Base.metadata.create_all)
