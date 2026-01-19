from __future__ import annotations
from datetime import datetime
from typing import List, Optional
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from sqlalchemy.orm import Session
from app.db.session import get_db
from app.db.models import Empresa, EfdArquivo, EfdVersao

router = APIRouter(prefix="/browse", tags=["Browse"])


# -------------------------
# Schemas (response)
# -------------------------

class EmpresaOut(BaseModel):
    id: int
    cnpj: str
    razao_social: Optional[str] = None

class ArquivoOut(BaseModel):
    id: int
    empresa_id: int
    nome_arquivo: Optional[str] = None
    periodo: str
    data_upload: Optional[datetime] = None
    status: str
    line_ending: str

class VersaoOut(BaseModel):
    id: int
    arquivo_id: int
    numero: int
    data_geracao: Optional[datetime] = None
    observacao: Optional[str] = None
    status: str


# -------------------------
# Endpoints
# -------------------------

@router.get("/empresas", response_model=List[EmpresaOut])
def listar_empresas(db: Session = Depends(get_db)):
    empresas = (
        db.query(Empresa)
        .order_by(Empresa.id.desc())
        .all()
    )
    return [
        EmpresaOut(
            id=int(e.id),
            cnpj=e.cnpj,
            razao_social=e.razao_social,
        )
        for e in empresas
    ]


@router.get("/empresas/{empresa_id}/arquivos", response_model=List[ArquivoOut])
def listar_arquivos_da_empresa(empresa_id: int, db: Session = Depends(get_db)):
    empresa = db.get(Empresa, empresa_id)
    if not empresa:
        raise HTTPException(status_code=404, detail="Empresa não encontrada")

    arquivos = (
        db.query(EfdArquivo)
        .filter(EfdArquivo.empresa_id == empresa_id)
        .order_by(EfdArquivo.periodo.desc(), EfdArquivo.id.desc())
        .all()
    )

    return [
        ArquivoOut(
            id=int(a.id),
            empresa_id=int(a.empresa_id),
            nome_arquivo=a.nome_arquivo,
            periodo=a.periodo,
            data_upload=a.data_upload,
            status=str(a.status),
            line_ending=str(a.line_ending),
        )
        for a in arquivos
    ]


@router.get("/arquivos/{arquivo_id}/versoes", response_model=List[VersaoOut])
def listar_versoes_do_arquivo(arquivo_id: int, db: Session = Depends(get_db)):
    arquivo = db.get(EfdArquivo, arquivo_id)
    if not arquivo:
        raise HTTPException(status_code=404, detail="Arquivo não encontrado")

    versoes = (
        db.query(EfdVersao)
        .filter(EfdVersao.arquivo_id == arquivo_id)
        .order_by(EfdVersao.numero.desc(), EfdVersao.id.desc())
        .all()
    )

    return [
        VersaoOut(
            id=int(v.id),
            arquivo_id=int(v.arquivo_id),
            numero=int(v.numero),
            data_geracao=v.data_geracao,
            observacao=v.observacao,
            status=str(v.status),
        )
        for v in versoes
    ]


@router.get("/versoes/{versao_id}", response_model=VersaoOut)
def detalhe_versao(versao_id: int, db: Session = Depends(get_db)):
    v = db.get(EfdVersao, versao_id)
    if not v:
        raise HTTPException(status_code=404, detail="Versão não encontrada")

    return VersaoOut(
        id=int(v.id),
        arquivo_id=int(v.arquivo_id),
        numero=int(v.numero),
        data_geracao=v.data_geracao,
        observacao=v.observacao,
        status=str(v.status),
    )
