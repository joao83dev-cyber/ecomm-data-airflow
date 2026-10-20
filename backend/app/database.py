from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

from app.config import settings


class Base(DeclarativeBase):
    pass


engine = create_engine(
    settings.database_url,
    pool_size=settings.max_pool_size,
    max_overflow=settings.max_overflow,
    pool_pre_ping=True,
    pool_timeout=10,
)
SessionLocal = sessionmaker(bind=engine, expire_on_commit=False)
