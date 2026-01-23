from __future__ import annotations
from fastapi import FastAPI
from app.api.routes.upload_endpoints import router as upload_router
from app.api.creditos_endpoints import router as creditos_router
from app.api.routes.workflow_endpoints import router as workflow_router
from app.api.versao_resumo_endpoints import router as versao_resumo_router
from app.api.empresa_resumo_endpoints import router as empresa_resumo_router
from app.api.export_endpoints import router as export_router
from app.api.routes.browse_endpoints import router as browse_router
from app.api.apontamentos_endpoints import router as apontamentos_router
from app.db.models.base import Base
from app.db.session import engine
from app.api.empresa_endpoints import router as empresa_router
from app.api.routes.revision_endpoints import router as revision_router
import app.db.models.models_all # noqa  ✅ garante que todos os models foram carregados
from app.api.routes.c170_endpoints import router as c170_router



APP_TITLE = "SPED Créditos"
APP_VERSION = "0.1.0"

app = FastAPI(
    title=APP_TITLE,
    version=APP_VERSION,
    docs_url="/docs",
    redoc_url="/redoc",
)

app.include_router(upload_router)
app.include_router(creditos_router)
app.include_router(workflow_router)
app.include_router(export_router)
app.include_router(browse_router)
app.include_router(apontamentos_router)
app.include_router(versao_resumo_router)
app.include_router(empresa_resumo_router)

app.include_router(empresa_router)
app.include_router(revision_router)
app.include_router(c170_router)


@app.on_event("startup")
def on_startup() -> None:

    Base.metadata.create_all(bind=engine)


@app.get("/health", tags=["Health"])
def health() -> dict:
    return {"status": "ok", "app": APP_TITLE, "version": APP_VERSION}
