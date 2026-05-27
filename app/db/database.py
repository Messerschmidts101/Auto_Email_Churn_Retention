from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base, Session
from collections.abc import Generator

# Global and Shared
DATABASE_URL = "postgresql+psycopg2://postgres:postgres@127.0.0.1:5432/postgres"
objEngine = create_engine(
    DATABASE_URL,
    pool_size=10,
    max_overflow=20,
    pool_pre_ping=True,
    pool_recycle=1800,
    echo=False
)
SessionLocal = sessionmaker(
    autocommit=False,
    autoflush=False,
    bind=objEngine
)
objBase = declarative_base()

def connect_db() -> Generator[Session, None, None]:
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()