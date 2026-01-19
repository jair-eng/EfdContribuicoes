# SPED Créditos (MVP)

Backend para ingestão/validação e análise de EFD (ex.: EFD Contribuições), com fluxo de **upload → preview → confirmação**, persistência em **MySQL**, regras fiscais, edição linha a linha e export (quando habilitado).

## Stack
- Python 3.12
- FastAPI
- Uvicorn
- MySQL
- SQLAlchemy

## Estrutura (alto nível)
sped-creditos/
├── app/
│ ├── main.py
│ ├── config/
│ ├── db/
│ ├── sped/
│ └── fiscal/
├── .env
├── requirements.txt
├── start.ps1
└── README.md

markdown
Copiar código

## Requisitos
- Windows 10/11
- Python 3.12 instalado
- MySQL instalado e **rodando**
- (Opcional) PyCharm Community

## Configuração inicial

### 1) Criar ambiente virtual
Na raiz do projeto:

```bash
py -3.12 -m venv .venv
Ativar:

powershell
Copiar código
.\.venv\Scripts\activate
2) Instalar dependências
bash
Copiar código
pip install -r requirements.txt
3) Criar arquivo .env
Crie um .env na raiz com as variáveis do banco:

env
Copiar código
DB_HOST=localhost
DB_PORT=3306
DB_NAME=sped_creditos
DB_USER=root
DB_PASSWORD=SUASENHA
Importante: o MySQL precisa estar rodando (serviço MySQL80, por exemplo).

Como rodar
Opção recomendada (script)
No PowerShell, na raiz do projeto:

powershell
Copiar código
.\start.ps1
Abrirá automaticamente:

Swagger: http://127.0.0.1:8000/docs

Parar o servidor:

CTRL + C

Opções do script
Pular instalação do requirements:

powershell
Copiar código
.\start.ps1 -NoInstall
Trocar porta:

powershell
Copiar código
.\start.ps1 -Port 8001
Não abrir o Swagger:

powershell
Copiar código
.\start.ps1 -OpenDocs:$false
Endpoints
Acesse a documentação interativa:

GET /docs

Healthcheck:

GET / (ou /health, se estiver configurado)

Troubleshooting
“python não encontrado”
No Windows, use py:

bash
Copiar código
py --version
Script bloqueado no PowerShell
Rode uma vez:

powershell
Copiar código
Set-ExecutionPolicy -Scope CurrentUser RemoteSigned
Erro de conexão com MySQL
Confirme se o serviço do MySQL está rodando

Verifique credenciais e DB_HOST/DB_PORT no .env

Erro de encoding em SPED
Arquivos SPED costumam vir em latin-1. Se aparecer caracteres quebrados:

confira se o parser está usando encoding="latin-1".

Roadmap (MVP)
 Upload com preview + confirmação

 Persistência por empresa/período

 Regras fiscais mínimas (créditos PIS/COFINS)

 Edição linha a linha e reprocessamento

 Export do arquivo (mesmas condições de entrada)

 Auditoria e logs