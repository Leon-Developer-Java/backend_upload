import hmac
import os
import sys
from pathlib import Path

import jwt
from dotenv import load_dotenv
from fastapi import Request
from fastapi.responses import JSONResponse


BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = BASE_DIR.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

load_dotenv(BASE_DIR / ".env")

from DB.config import create_database_engine
from DB.repository import get_active_user


JWT_SECRET = os.getenv("JWT_SECRET", "").strip()
if len(JWT_SECRET.encode("utf-8")) < 32:
    raise RuntimeError("JWT_SECRET 必须配置为至少 32 字节的随机密钥")
WHITELIST = {"/", "/api/health", "/docs", "/openapi.json"}
STATIC_PREFIXES = ("/data/", "/outputs/")
engine = create_database_engine()


def request_user(request: Request) -> dict:
    user = getattr(request.state, "user", None)
    if not user:
        raise RuntimeError("authenticated user was not attached to the request")
    return user


def install_auth(app, rules) -> None:
    @app.middleware("http")
    async def check_token(request, call_next):
        path = request.url.path
        if path.startswith("/api/internal/terrain-assets/"):
            expected = os.getenv("TERRAIN_ARTIFACT_SERVICE_TOKEN", "").strip()
            supplied = request.headers.get("x-weather-service-token", "").strip()
            if len(expected) < 32 or not hmac.compare_digest(supplied, expected):
                return JSONResponse({"code": 401, "detail": "内部资产服务凭据无效"}, status_code=401)
            request.state.service = "backend_wrf"
            return await call_next(request)
        if request.method == "OPTIONS" or path in WHITELIST:
            return await call_next(request)
        required_rule = next((role for prefix, role in rules if path.startswith(prefix)), None)
        if required_rule is None:
            return await call_next(request)
        if isinstance(required_rule, dict):
            required = required_rule.get(request.method, required_rule.get("*"))
            if required is None:
                return await call_next(request)
        else:
            required = required_rule
        header = request.headers.get("authorization", "")
        token = header[7:] if header.startswith("Bearer ") else ""
        if not token and path.startswith(STATIC_PREFIXES):
            token = request.query_params.get("token", "")
        try:
            payload = jwt.decode(
                token,
                JWT_SECRET,
                algorithms=["HS256"],
                options={"require": ["sub", "exp", "token_version"]},
            )
        except jwt.PyJWTError:
            return JSONResponse({"code": 401, "detail": "token 无效或已过期"}, status_code=401)

        user = get_active_user(engine, str(payload.get("sub") or ""))
        if user is None:
            return JSONResponse({"code": 401, "detail": "用户不存在或已被禁用"}, status_code=401)
        if int(payload.get("token_version", 0)) != int(user.get("token_version") or 0):
            return JSONResponse({"code": 401, "detail": "token 已失效，请重新登录"}, status_code=401)
        if int(user.get("role") or 0) < int(required):
            return JSONResponse({"code": 403, "detail": "权限不足"}, status_code=403)

        request.state.user = user
        request.state.auth_payload = payload
        return await call_next(request)
