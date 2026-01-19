from __future__ import annotations
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm import Session
from app.config.settings import DATABASE_URL

# pool_pre_ping evita conexões "mortas"
engine = create_engine(
    DATABASE_URL,
    pool_pre_ping=True,
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


def get_db():
    """
    Dependency do FastAPI.
    Abre sessão e garante fechamento.
    """
    db: Session = SessionLocal()
    try:
        yield db
    finally:
        db.close()
