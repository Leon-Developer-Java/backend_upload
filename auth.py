import os
from pathlib import Path

import jwt
from dotenv import load_dotenv
from fastapi.responses import JSONResponse

load_dotenv(Path(__file__).resolve().parent / ".env")

JWT_SECRET = os.getenv("JWT_SECRET", "dev-secret-change-me").strip()
WHITELIST = {"/", "/api/health", "/docs", "/openapi.json"}
STATIC_PREFIXES = ("/data/", "/outputs/")


def install_auth(app, rules) -> None:
    @app.middleware("http")
    async def check_token(request, call_next):
        path = request.url.path
        if request.method == "OPTIONS" or path in WHITELIST:
            return await call_next(request)
        required = next((role for prefix, role in rules if path.startswith(prefix)), None)
        if required is None:
            return await call_next(request)
        header = request.headers.get("authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        if not token and path.startswith(STATIC_PREFIXES):
            token = request.query_params.get("token", "")
        try:
            payload = jwt.decode(token, JWT_SECRET, algorithms=["HS256"])
        except jwt.PyJWTError:
            return JSONResponse({"code": 401, "detail": "token 无效或已过期"}, status_code=401)
        if payload.get("role", 0) < required:
            return JSONResponse({"code": 403, "detail": "权限不足"}, status_code=403)
        return await call_next(request)
