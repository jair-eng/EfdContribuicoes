@echo off
setlocal
title SPED Créditos - Start
cls

echo ==========================================
echo   SPED CREDITOS - START
echo ==========================================
echo.

cd /d "%~dp0"

REM 1) .env (avisa, mas nao para)
if exist ".env" goto ENV_OK
echo [WARN] Arquivo .env nao encontrado na raiz.
echo [WARN] Configure DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
echo.
goto VENV_CHECK
:ENV_OK
echo [OK] .env encontrado
echo.

:VENV_CHECK
REM 2) Venv
if exist ".venv\Scripts\activate.bat" goto VENV_OK
echo [ERRO] Nao encontrei .venv\Scripts\activate.bat
echo [DICA] Se sua venv chama "venv" (sem ponto), renomeie ou ajuste o caminho.
pause
exit /b 1

:VENV_OK
echo [INFO] Ativando ambiente virtual (.venv)...
call ".venv\Scripts\activate.bat"
echo [OK] Ambiente virtual ativado
echo.

REM 3) Dependencias (se houver requirements.txt)
if not exist "requirements.txt" goto START_API
echo [INFO] Instalando/atualizando dependencias...
pip install -r requirements.txt
echo.

:START_API
REM 4) Abrir Swagger
start "" "http://127.0.0.1:8000/docs"

REM 5) Subir API
echo [INFO] Subindo API...
echo [INFO] CTRL+C para parar
echo.
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000

pause
