"""气象数据上传后端（独立服务，端口 8003）。

负责分片上传 + 断点续传，把上传的气象文件落盘到
`backend/data/{业务类型}/wait_process/`。
"""

import hashlib
import shutil
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel


# 最终落盘根目录，复用现有展示后端的 data 目录
TARGET_DATA_DIR = Path(r"D:\weather_prediction_system\backend\data")
# 分片临时目录
TEMP_DIR = Path(__file__).resolve().parent / "tmp_chunks"

# 前端显示名 -> backend/data 子目录名
TYPE_TO_FOLDER = {
    "ERA5": "ERA5",
    "GFS/ECMWF": "GFS",
    "CMA": "CMA",
    "雷达": "Radar",
    "葵花": "Himawari",
    "WRF": "WRF",
}

WAIT_PROCESS = "wait_process"


app = FastAPI(title="Weather Data Upload Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:5174",
        "http://127.0.0.1:5174",
        "http://localhost:5177",
        "http://127.0.0.1:5177",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def ok(data: Any = None, message: str = "success") -> dict[str, Any]:
    return {"code": 0, "data": data, "message": message}


def safe_file_id(file_id: str) -> str:
    """把组合键（文件名+大小+修改时间）转成安全的目录名。"""
    if not file_id:
        raise HTTPException(status_code=400, detail="file_id 不能为空。")
    return hashlib.md5(file_id.encode("utf-8")).hexdigest()


def chunk_dir(file_id: str) -> Path:
    return TEMP_DIR / safe_file_id(file_id)


def resolve_folder(data_type: str) -> str:
    folder = TYPE_TO_FOLDER.get(data_type)
    if folder is None:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的数据类型：{data_type!r}，可选 {list(TYPE_TO_FOLDER)}。",
        )
    return folder


@app.get("/")
def root() -> dict[str, Any]:
    return ok({"service": "weather-data-upload-backend", "docs": "/docs"})


@app.get("/api/health")
def health() -> dict[str, Any]:
    return ok({"status": "online"})


@app.get("/api/upload/status")
def upload_status(file_id: str) -> dict[str, Any]:
    """返回已落盘的分片索引集合，供前端断点续传跳过。"""
    target = chunk_dir(file_id)
    uploaded: list[int] = []
    if target.is_dir():
        for part in target.glob("*.part"):
            try:
                uploaded.append(int(part.stem))
            except ValueError:
                continue
    uploaded.sort()
    return ok({"uploaded": uploaded})


@app.post("/api/upload/chunk")
async def upload_chunk(
    file_id: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    chunk: UploadFile = File(...),
) -> dict[str, Any]:
    """接收单个分片，幂等写入临时目录。"""
    if chunk_index < 0 or chunk_index >= total_chunks:
        raise HTTPException(status_code=400, detail="chunk_index 越界。")

    target_dir = chunk_dir(file_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    part_path = target_dir / f"{chunk_index}.part"

    # 已存在则视为已传，保证可重传幂等
    if not part_path.exists():
        tmp_path = part_path.with_suffix(".part.tmp")
        with tmp_path.open("wb") as out:
            shutil.copyfileobj(chunk.file, out)
        tmp_path.replace(part_path)

    return ok({"received": chunk_index})


class CompletePayload(BaseModel):
    file_id: str
    file_name: str
    total_chunks: int
    data_type: str


@app.post("/api/upload/complete")
def upload_complete(payload: CompletePayload) -> dict[str, Any]:
    """校验分片齐全后按序合并落盘，并清理临时分片。"""
    folder = resolve_folder(payload.data_type)

    target_dir = chunk_dir(payload.file_id)
    missing = [
        i
        for i in range(payload.total_chunks)
        if not (target_dir / f"{i}.part").exists()
    ]
    if missing:
        raise HTTPException(
            status_code=400,
            detail={"message": "分片缺失，无法合并。", "missing": missing},
        )

    safe_name = Path(payload.file_name).name
    if not safe_name:
        raise HTTPException(status_code=400, detail="file_name 非法。")

    dest_dir = TARGET_DATA_DIR / folder / WAIT_PROCESS
    dest_dir.mkdir(parents=True, exist_ok=True)
    dest_path = dest_dir / safe_name

    tmp_dest = dest_path.with_name(dest_path.name + ".merging")
    with tmp_dest.open("wb") as out:
        for i in range(payload.total_chunks):
            part = target_dir / f"{i}.part"
            with part.open("rb") as src:
                shutil.copyfileobj(src, out)
    tmp_dest.replace(dest_path)

    shutil.rmtree(target_dir, ignore_errors=True)

    return ok(
        {
            "file_name": safe_name,
            "directory": str(dest_dir).replace("\\", "/") + "/",
            "data_type": folder,
        }
    )
