@echo off
cd /d C:\Users\anish\PycharmProjects\E2T-QualificationPhase
call .venv\Scripts\activate
python -m app.crm_loader_local

REM Optional: trigger Netlify build after CRM sync
curl -X POST "https://api.netlify.com/build_hooks/68e4fc2f22dc4bd389f05837"
