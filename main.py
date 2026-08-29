"""Weather data upload service (port 8003).

The service receives resumable chunks, stores one private raw file and writes a
pending row to public_info. Parsing is intentionally handled by Adapter Worker.
"""

from __future__ import annotations

import hashlib
import json
import logging
import os
import re
import shutil
import sys
import uuid
from copy import deepcopy
from collections import Counter
from contextlib import asynccontextmanager, contextmanager
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any

from fastapi import FastAPI, File, Form, HTTPException, Query, Request, Response, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from pydantic import BaseModel, Field
from sqlalchemy.exc import IntegrityError, SQLAlchemyError

from auth import install_auth, request_user


BASE_DIR = Path(__file__).resolve().parent
WORKSPACE_ROOT = BASE_DIR.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from acquisition import AcquisitionService
from catalog_reconcile import reconcile_catalog
from DB.migrate import init_database
from DB.repository import (
    create_or_get_satellite_collection,
    create_satellite_collection_upload_task,
    create_upload_task,
    find_user_duplicate,
    get_collection_member_task_by_role,
    get_display_resource,
    get_upload_task_by_session,
    get_user_satellite_collection,
    get_user_task,
    list_display_resources,
    list_user_satellite_collections,
    list_user_tasks,
    retry_user_satellite_collection,
    retry_user_task,
    summarize_user_tasks,
)


RAW_STORAGE_ROOT = Path(
    os.getenv("RAW_STORAGE_ROOT", str(WORKSPACE_ROOT / "storage" / "raw"))
).resolve()
TMP_STORAGE_ROOT = Path(
    os.getenv("TMP_STORAGE_ROOT", str(WORKSPACE_ROOT / "storage" / "tmp"))
).resolve()
TEMP_DIR = TMP_STORAGE_ROOT / "uploads"
PRODUCT_DATA_ROOT = Path(
    os.getenv("PRODUCT_DATA_ROOT", str(WORKSPACE_ROOT / "backend" / "data"))
).resolve()


def utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


MAX_CHUNK_SIZE = int(os.getenv("MAX_UPLOAD_CHUNK_SIZE", str(8 * 1024 * 1024)))
MAX_FILE_SIZE = int(os.getenv("MAX_UPLOAD_FILE_SIZE", str(20 * 1024 * 1024 * 1024)))
MAX_TOTAL_CHUNKS = int(os.getenv("MAX_UPLOAD_CHUNKS", "10000"))

TYPE_ALIASES = {
    "ERA5": "ERA5",
    "GFS": "GFS",
    "ECMWF": "ECMWF",
    "CMA": "CMA",
    "RADAR": "Radar",
    "雷达": "Radar",
    "WRF": "WRF",
    "FY3": "FY3",
    "FY-3": "FY3",
    "HIMAWARI": "Himawari",
    "葵花": "Himawari",
}

ALLOWED_SUFFIXES = {
    "ERA5": {".nc", ".nc4", ".netcdf"},
    "GFS": {".grib", ".grib2", ".grb", ".grb2"},
    "ECMWF": {".grib", ".grib2", ".grb", ".grb2"},
    "CMA": {".nc", ".nc4", ".grib", ".grib2", ".grb", ".h5", ".hdf"},
    "Radar": {".nc", ".cinrad", ".radar", ".bz2"},
    "WRF": {"", ".nc"},
    "FY3": {".hdf", ".h5"},
    "Himawari": {".hsd", ".dat", ".bz2"},
}

QUEUED_UPLOAD_TYPES = {"ERA5", "GFS", "ECMWF", "CMA", "Radar", "WRF", "FY3", "Himawari"}
SATELLITE_UPLOAD_TYPES = {"FY3", "Himawari"}
HIMAWARI_DEFAULT_BANDS = ("B01", "B02", "B03", "B13")
HIMAWARI_DEFAULT_SEGMENTS = max(1, int(os.getenv("HIMAWARI_EXPECTED_SEGMENTS", "10")))
FY3_SCIENCE_RE = re.compile(
    r"^FY3(?P<sat>[A-Z])_MERSI_GBAL_L1_(?P<date>\d{8})_(?P<time>\d{4})_1000M_MS\.HDF$",
    re.IGNORECASE,
)
FY3_GEO_RE = re.compile(
    r"^FY3(?P<sat>[A-Z])_MERSI_GBAL_L1_(?P<date>\d{8})_(?P<time>\d{4})_GEO1K_MS\.HDF$",
    re.IGNORECASE,
)
HIMAWARI_HSD_RE = re.compile(
    r"^HS_H(?P<sat>\d{2})_(?P<date>\d{8})_(?P<time>\d{4})_"
    r"(?P<band>B\d{2})_(?P<region>[A-Z0-9]+)_(?P<resolution>R\d{2})_"
    r"S(?P<segment>\d{2})(?P<total>\d{2})\.DAT(?:\.bz2)?$",
    re.IGNORECASE,
)

EXPECTED_STEP_SECONDS = {
    "ERA5": 3600,
    "GFS": 3600,
    "ECMWF": 10800,
    # CMA products use different cadences; infer the step from each series.
    "CMA": None,
    "RADAR": 600,
    "WRF": 3600,
    "FY3": 300,
    "HIMAWARI": 600,
}

NOWCAST_FRAME_COUNT = 5
NOWCAST_STEP_SECONDS = 360
NOWCAST_VARIABLE = "observation.prdt_crf_raw_log"


engine, _ = init_database(import_users=True)
acquisition_service: AcquisitionService | None = None
acquisition_startup_error = ""


@asynccontextmanager
async def lifespan(_: FastAPI):
    start_acquisition_scheduler()
    try:
        yield
    finally:
        stop_acquisition_scheduler()


app = FastAPI(title="Weather Data Upload Backend", version="0.2.0", lifespan=lifespan)

# Data catalog is readable by signed-in users; uploads still require role 2.
install_auth(
    app,
    [
        ("/api/catalog", {"GET": 1, "POST": 2}),
        ("/api/acquisition", {"GET": 1, "POST": 2}),
        ("/api", 2),
    ],
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=os.getenv(
        "CORS_ORIGINS",
        "http://localhost:5173,http://127.0.0.1:5173,"
        "http://localhost:5174,http://127.0.0.1:5174,"
        "http://localhost:5177,http://127.0.0.1:5177",
    ).split(","),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


def ok(data: Any = None, message: str = "success") -> dict[str, Any]:
    return {"code": 0, "data": data, "message": message}


def start_acquisition_scheduler() -> None:
    global acquisition_service, acquisition_startup_error
    try:
        acquisition_service = AcquisitionService(engine, RAW_STORAGE_ROOT)
        acquisition_service.start()
        acquisition_startup_error = ""
    except SQLAlchemyError:
        acquisition_service = None
        acquisition_startup_error = "自动采集数据库结构不兼容，请更新共享 DB；数据目录和上传不受影响。"
        logging.getLogger(__name__).exception(acquisition_startup_error)


def stop_acquisition_scheduler() -> None:
    if acquisition_service is not None:
        acquisition_service.stop()


def _require_acquisition_service() -> AcquisitionService:
    if acquisition_service is None:
        raise HTTPException(status_code=503, detail=acquisition_startup_error or "自动采集服务尚未就绪。")
    return acquisition_service


def _acquisition_type(value: str) -> str:
    normalized = normalize_data_type(value)
    if normalized not in SATELLITE_UPLOAD_TYPES:
        raise HTTPException(status_code=400, detail="自动采集仅支持 FY3 和 Himawari。")
    return normalized


@app.get("/api/acquisition/{data_type}/status")
def acquisition_status(data_type: str) -> dict[str, Any]:
    return ok(_require_acquisition_service().status(_acquisition_type(data_type)))


@app.get("/api/acquisition/{data_type}/scenes")
def acquisition_scenes(
    data_type: str,
    limit: int = Query(default=100, ge=1, le=200),
) -> dict[str, Any]:
    normalized = _acquisition_type(data_type)
    items = _require_acquisition_service().scenes(normalized, limit=limit)
    return ok({"data_type": normalized, "items": items, "total": len(items)})


@app.post("/api/acquisition/{data_type}/sync")
def acquisition_sync(data_type: str) -> dict[str, Any]:
    normalized = _acquisition_type(data_type)
    service = _require_acquisition_service()
    try:
        return ok(service.sync(normalized), "自动采集同步完成")
    except ValueError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=409, detail=str(exc)) from exc
    except Exception as exc:
        raise HTTPException(status_code=502, detail=f"自动采集失败：{exc}") from exc


def safe_file_id(file_id: str) -> str:
    if not file_id or len(file_id) > 1024:
        raise HTTPException(status_code=400, detail="file_id 非法。")
    return hashlib.sha256(file_id.encode("utf-8")).hexdigest()


def normalize_data_type(value: str) -> str:
    raw = str(value or "").strip()
    normalized = TYPE_ALIASES.get(raw.upper()) or TYPE_ALIASES.get(raw)
    if normalized is None:
        raise HTTPException(
            status_code=400,
            detail=f"不支持的数据类型：{value!r}，可选 ERA5、GFS、ECMWF、CMA、雷达、WRF、FY-3、葵花。",
        )
    return normalized


def chunk_dir(user_uuid: str, file_id: str) -> Path:
    return TEMP_DIR / user_uuid / safe_file_id(file_id)


def _session_file(target: Path) -> Path:
    return target / "session.json"


def _load_session(target: Path) -> dict[str, Any] | None:
    path = _session_file(target)
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=409, detail="上传会话元数据损坏，请重新上传。") from exc


def _write_session(target: Path, data: dict[str, Any]) -> None:
    path = _session_file(target)
    temporary = path.with_suffix(".json.tmp")
    temporary.write_text(json.dumps(data, ensure_ascii=False), encoding="utf-8")
    temporary.replace(path)


def _validate_total_chunks(total_chunks: int) -> None:
    if total_chunks < 1 or total_chunks > MAX_TOTAL_CHUNKS:
        raise HTTPException(status_code=400, detail="total_chunks 超出允许范围。")


def _safe_original_name(file_name: str, data_type: str) -> str:
    safe_name = Path(str(file_name or "")).name.strip()
    if not safe_name or safe_name in {".", ".."} or len(safe_name) > 255:
        raise HTTPException(status_code=400, detail="file_name 非法。")
    if any(ord(char) < 32 for char in safe_name):
        raise HTTPException(status_code=400, detail="file_name 包含控制字符。")
    suffix = Path(safe_name).suffix.lower()
    if suffix not in ALLOWED_SUFFIXES[data_type]:
        raise HTTPException(status_code=400, detail=f"{data_type} 不支持扩展名 {suffix or '(无扩展名)'}。")
    if data_type == "WRF" and not suffix and not safe_name.lower().startswith("wrfout"):
        raise HTTPException(status_code=400, detail="无扩展名的 WRF 文件名必须以 wrfout 开头。")
    return safe_name


def _validate_queued_upload_type(data_type: str) -> None:
    if data_type not in QUEUED_UPLOAD_TYPES:
        raise HTTPException(
            status_code=400,
            detail=(
                f"{data_type} 当前必须使用 8002 的多文件 raw 场景上传接口，"
                "不能进入单文件解析队列。"
            ),
        )


def _satellite_descriptor(file_name: str, data_type: str) -> dict[str, Any]:
    name = Path(file_name).name
    if data_type == "FY3":
        match = FY3_SCIENCE_RE.match(name) or FY3_GEO_RE.match(name)
        if match is None:
            raise HTTPException(
                status_code=400,
                detail="FY-3 文件名必须是同一时次的 1000M_MS.HDF 或 GEO1K_MS.HDF。",
            )
        role = "science" if FY3_SCIENCE_RE.match(name) else "geo"
        return {
            "scene_key": (
                f"FY3{match.group('sat').upper()}_{match.group('date')}_{match.group('time')}_"
                "MERSI_GBAL_1000M"
            ),
            "role": role,
            "role_group": role,
            "group_roles": [role],
        }
    if data_type == "Himawari":
        match = HIMAWARI_HSD_RE.match(name)
        if match is None:
            raise HTTPException(
                status_code=400,
                detail="葵花文件名必须符合 HS_Hxx_YYYYMMDD_HHMM_Bxx_REGION_Rxx_Sxxxx.DAT[.bz2]。",
            )
        band = match.group("band").upper()
        if band not in HIMAWARI_DEFAULT_BANDS:
            raise HTTPException(
                status_code=400,
                detail=f"集合 Worker 当前只接收默认通道：{','.join(HIMAWARI_DEFAULT_BANDS)}。",
            )
        segment = int(match.group("segment"))
        total = int(match.group("total"))
        if segment < 1 or total < 1 or segment > total:
            raise HTTPException(status_code=400, detail="HSD 分段编号非法。")
        return {
            "scene_key": (
                f"H{match.group('sat')}_{match.group('date')}_{match.group('time')}_"
                f"{match.group('region').upper()}_{match.group('resolution').upper()}"
            ),
            "role": f"{band}:S{segment:02d}",
            "role_group": f"{band}:",
            "group_roles": [f"{band}:S{index:02d}" for index in range(1, total + 1)],
        }
    raise HTTPException(status_code=400, detail="只有 FY-3 与葵花需要集合准备。")


def _initial_satellite_roles(data_type: str) -> list[str]:
    if data_type == "FY3":
        return ["science", "geo"]
    return [
        f"{band}:S{index:02d}"
        for band in HIMAWARI_DEFAULT_BANDS
        for index in range(1, HIMAWARI_DEFAULT_SEGMENTS + 1)
    ]


def _task_payload(
    task: dict[str, Any],
    *,
    duplicate_content: bool | None = None,
    collection: dict[str, Any] | None = None,
) -> dict[str, Any]:
    payload = {
        "file_uuid": task.get("file_uuid"),
        "collection_uuid": task.get("collection_uuid"),
        "file_name": task.get("original_file_name"),
        "file_type": task.get("file_type"),
        "file_size": task.get("file_size"),
        "file_hash": task.get("file_hash"),
        "data_type": task.get("data_type"),
        "ingest_status": task.get("ingest_status"),
        "parse_status": task.get("parse_status"),
        "parse_attempts": task.get("parse_attempts"),
        "parse_error": task.get("parse_error"),
        "meta_path": task.get("meta_path"),
        "default_webp_url": task.get("default_webp_url"),
        "webp_count": task.get("webp_count"),
        "create_time": task.get("create_time"),
        "update_time": task.get("update_time"),
        "parse_started_at": task.get("parse_started_at"),
        "parse_finished_at": task.get("parse_finished_at"),
    }
    if duplicate_content is not None:
        payload["duplicate_content"] = duplicate_content
    if collection is not None:
        payload["collection"] = collection
    return payload


def _resource_meta_path(resource: dict[str, Any]) -> Path:
    relative = str(resource.get("meta_path") or "").replace("\\", "/").lstrip("/")
    if not relative:
        raise HTTPException(status_code=409, detail="该数据缺少 meta.json 路径。")
    candidate = (PRODUCT_DATA_ROOT / relative).resolve()
    try:
        candidate.relative_to(PRODUCT_DATA_ROOT)
    except ValueError as exc:
        raise HTTPException(status_code=409, detail="meta.json 路径超出数据目录。") from exc
    if not candidate.is_file():
        raise HTTPException(status_code=409, detail="该数据的 meta.json 不存在。")
    return candidate


def _nowcast_input_path(resource: dict[str, Any]) -> tuple[Path, dict[str, Any]]:
    if str(resource.get("data_type") or "").upper() != "RADAR":
        raise HTTPException(status_code=400, detail="短临降水预报只接受雷达数据。")

    meta_path = _resource_meta_path(resource)
    try:
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise HTTPException(status_code=409, detail="雷达数据的 meta.json 无法读取。") from exc

    if str(meta.get("file_format") or "") != "RADAR_NC_CAP_FMT":
        raise HTTPException(status_code=409, detail="该雷达数据不是模型支持的 CAP_FMT NetCDF。")

    raw_variables = {
        str(item.get("raw_name") or "")
        for item in meta.get("variables") or []
        if isinstance(item, dict)
    }
    if NOWCAST_VARIABLE not in raw_variables:
        raise HTTPException(
            status_code=409,
            detail=f"雷达数据缺少短临模型变量 {NOWCAST_VARIABLE}。",
        )

    candidates: list[Any] = [meta.get("source_file")]
    candidates.extend(
        item.get("netcdf")
        for item in meta.get("variables") or []
        if isinstance(item, dict)
    )
    for value in candidates:
        if not value:
            continue
        source = Path(str(value))
        source = source.resolve() if source.is_absolute() else (PRODUCT_DATA_ROOT / source).resolve()
        try:
            source.relative_to(PRODUCT_DATA_ROOT)
        except ValueError:
            continue
        if source.is_file() and source.suffix.lower() == ".nc":
            return source, meta

    raw_source = _resolve_existing_raw(str(resource.get("source_path") or ""))
    if raw_source is not None and raw_source.suffix.lower() == ".nc":
        return raw_source, meta

    raise HTTPException(status_code=409, detail="雷达数据对应的模型输入 NetCDF 不存在。")


def _select_latest_nowcast_window(
    candidates: list[dict[str, Any]],
    *,
    frame_count: int = NOWCAST_FRAME_COUNT,
    step_seconds: int = NOWCAST_STEP_SECONDS,
) -> list[dict[str, Any]]:
    by_time: dict[datetime, dict[str, Any]] = {}
    for item in candidates:
        timestamp = _parse_resource_time(str(item.get("valid_time") or ""))
        if timestamp is not None and timestamp not in by_time:
            by_time[timestamp] = item

    for end_time in sorted(by_time, reverse=True):
        expected = [
            end_time - timedelta(seconds=step_seconds * offset)
            for offset in range(frame_count - 1, -1, -1)
        ]
        if all(timestamp in by_time for timestamp in expected):
            return [by_time[timestamp] for timestamp in expected]
    return []


def _resource_times(resource: dict[str, Any]) -> list[str]:
    values = {
        value
        for asset in resource.get("assets") or []
        if (value := _canonical_resource_time(asset.get("valid_time")))
    }
    if not values:
        try:
            meta = json.loads(_resource_meta_path(resource).read_text(encoding="utf-8"))
            values.update(_meta_times(meta))
        except (HTTPException, OSError, json.JSONDecodeError):
            pass
    return sorted(values)


def _canonical_resource_time(value: Any) -> str | None:
    text = str(value or "").strip()
    if len(text) >= 19 and text[10] == "_" and text[4] == "-" and text[7] == "-":
        text = f"{text[:10]}T{text[11:]}"
    parsed = _parse_resource_time(text)
    return parsed.isoformat() + "Z" if parsed else None


def _meta_times(meta: dict[str, Any]) -> set[str]:
    candidates: list[Any] = [meta.get("observation_time"), meta.get("time")]
    candidates.extend(meta.get("times") if isinstance(meta.get("times"), list) else [])
    for frame in meta.get("frames") if isinstance(meta.get("frames"), list) else []:
        if isinstance(frame, dict):
            candidates.append(frame.get("valid_time") or frame.get("time") or frame.get("time_label"))
    for layer in (meta.get("variable_layers") or {}).values() if isinstance(meta.get("variable_layers"), dict) else []:
        if isinstance(layer, dict):
            candidates.extend(layer.get("times") if isinstance(layer.get("times"), list) else [])
    for variable in meta.get("variables") if isinstance(meta.get("variables"), list) else []:
        if isinstance(variable, dict):
            candidates.extend(variable.get("times") if isinstance(variable.get("times"), list) else [])
    weather = meta.get("weather_info") if isinstance(meta.get("weather_info"), dict) else {}
    candidates.append(weather.get("time"))
    return {value for item in candidates if (value := _canonical_resource_time(item))}


def _parse_resource_time(value: str) -> datetime | None:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed.tzinfo is not None:
            parsed = parsed.astimezone(UTC).replace(tzinfo=None)
        return parsed
    except (TypeError, ValueError):
        return None


def _is_duplicate_content(task: dict[str, Any]) -> bool:
    return bool(task.get("source_file_uuid"))


def _continuity(data_type: str, times: list[str]) -> dict[str, Any]:
    parsed = [item for item in (_parse_resource_time(value) for value in times) if item]
    if len(parsed) < 2:
        return {
            "continuous": False,
            "playable": False,
            "expected_step_seconds": EXPECTED_STEP_SECONDS.get(data_type),
            "gaps": [],
            "reason": "仅有 1 帧数据，至少需要 2 帧才能播放。" if parsed else "数据缺少有效时间，无法播放。",
        }

    deltas = [int((right - left).total_seconds()) for left, right in zip(parsed, parsed[1:])]
    positive = [delta for delta in deltas if delta > 0]
    default_step = EXPECTED_STEP_SECONDS.get(data_type)
    if default_step:
        expected = default_step
    elif positive:
        counts = Counter(positive)
        expected = min(counts, key=lambda value: (-counts[value], value))
    else:
        expected = default_step

    gaps = []
    if expected:
        for index, delta in enumerate(deltas):
            if delta != expected:
                gaps.append({
                    "after": times[index],
                    "before": times[index + 1],
                    "actual_step_seconds": delta,
                    "expected_step_seconds": expected,
                })
    continuous = bool(expected and not gaps)
    return {
        "continuous": continuous,
        "playable": continuous,
        "expected_step_seconds": expected,
        "gaps": gaps,
        "reason": "" if continuous else "时间序列存在缺帧，请补齐缺失时次后再播放。",
    }


def _resource_payload(resource: dict[str, Any], *, include_meta: bool = False) -> dict[str, Any]:
    data_type = str(resource.get("data_type") or "").upper()
    assets = resource.get("assets") or []
    times = _resource_times(resource)
    elements = sorted({
        str(asset.get("element_key"))
        for asset in resource.get("assets") or []
        if asset.get("element_key")
    })
    resolutions = sorted({
        str(asset.get("resolution_key"))
        for asset in resource.get("assets") or []
        if asset.get("resolution_key")
    })
    attribute_fields = {
        "elements": "element_key",
        "levels": "level_value",
        "level_types": "level_type",
        "resolutions": "resolution_key",
        "datasets": "dataset_id",
    }
    type_fields = {
        "ERA5": {
            "product_types": "product_type",
            "data_streams": "data_stream",
            "step_types": "step_type",
            "grid_types": "grid_type",
        },
        "GFS": {
            "run_times": "run_time",
            "cycle_hours": "cycle_hour",
            "forecast_hours": "forecast_hour",
            "step_types": "step_type",
            "level_types": "type_of_level",
            "product_categories": "product_category",
        },
        "ECMWF": {
            "run_times": "run_time",
            "cycle_hours": "cycle_hour",
            "forecast_hours": "forecast_hour",
            "step_types": "step_type",
            "level_types": "type_of_level",
            "streams": "stream",
            "product_classes": "product_class",
        },
        "CMA": {
            "product_types": "product_type",
            "product_names": "product_name",
        },
        "RADAR": {
            "radar_names": "radar_name",
            "station_codes": "station_code",
            "radar_types": "radar_type",
            "product_codes": "product_code",
            "elevations": "elevation",
        },
        "WRF": {
            "domains": "domain",
            "forecast_reference_times": "forecast_reference_time",
            "forecast_hours": "forecast_hour",
            "source_resolutions": "source_resolution",
        },
        "FY3": {
            "satellites": "satellite",
            "instruments": "instrument",
            "bands": "band",
            "source_resolutions": "source_resolution",
            "file_roles": "file_role",
        },
        "HIMAWARI": {
            "satellites": "satellite",
            "regions": "region",
            "bands": "band",
        },
    }
    attribute_fields.update(type_fields.get(data_type, {}))
    attributes = {
        name: sorted(
            {
                str(asset.get(field))
                for asset in assets
                if asset.get(field) is not None and str(asset.get(field)).strip()
            }
        )
        for name, field in attribute_fields.items()
    }
    payload = {
        "file_uuid": resource.get("file_uuid"),
        "data_type": data_type,
        "file_name": resource.get("original_file_name"),
        "visibility": resource.get("visibility"),
        "acquisition_type": resource.get("acquisition_type"),
        "owner_uuid": resource.get("user_uuid"),
        "meta_path": resource.get("meta_path"),
        "default_webp_url": resource.get("default_webp_url"),
        "webp_count": resource.get("webp_count"),
        "file_size": resource.get("file_size"),
        "create_time": resource.get("create_time"),
        "parse_finished_at": resource.get("parse_finished_at"),
        "times": times,
        "time_start": times[0] if times else None,
        "time_end": times[-1] if times else None,
        "frame_count": len(times),
        "elements": elements,
        "resolutions": resolutions,
        "attributes": attributes,
        **_continuity(data_type, times),
    }
    if include_meta:
        meta_path = _resource_meta_path(resource)
        try:
            payload["meta"] = json.loads(meta_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError) as exc:
            raise HTTPException(status_code=409, detail="该数据的 meta.json 无法读取。") from exc
    return payload


def _wrf_series_key(payload: dict[str, Any]) -> tuple[str, str, str, str]:
    name = str(payload.get("file_name") or "")
    lowered = name.lower()
    domain = next((part for part in ("d01", "d02", "d03", "d04") if f"wrfout_{part}" in lowered), "unknown")
    day = str(payload.get("time_start") or "")[:10]
    return (
        str(payload.get("owner_uuid") or "public"),
        str(payload.get("visibility") or "private"),
        domain,
        day,
    )


def _merge_wrf_series(items: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(items, key=lambda item: (item.get("time_start") or "", item.get("file_uuid") or ""))
    base = deepcopy(ordered[0])
    member_ids = [str(item["file_uuid"]) for item in ordered]
    meta = deepcopy(base.get("meta") or {})
    times = sorted({time for item in ordered for time in item.get("times") or []})
    meta_times = sorted(
        {
            str(value)
            for item in ordered
            for value in ((item.get("meta") or {}).get("times") or [])
            if _canonical_resource_time(value)
        },
        key=lambda value: _canonical_resource_time(value) or value,
    )
    webp_files: list[str] = []
    source_files: list[str] = []
    for item in ordered:
        item_meta = item.get("meta") or {}
        webp_files.extend(str(path) for path in item_meta.get("webp_files") or [])
        source = item_meta.get("source_file")
        source_files.extend(str(path) for path in (source if isinstance(source, list) else [source]) if path)

    products = deepcopy(meta.get("resolution_products") or {})
    for resolution_key, product in products.items():
        if not isinstance(product, dict):
            continue
        merged_files = []
        for item in ordered:
            item_product = (item.get("meta") or {}).get("resolution_products", {}).get(resolution_key, {})
            merged_files.extend(str(path) for path in item_product.get("webp_files") or [])
        product["webp_files"] = merged_files

    meta["times"] = meta_times
    meta["webp_files"] = webp_files
    meta["source_file"] = source_files
    if products:
        meta["resolution_products"] = products

    series_key = "|".join(_wrf_series_key(base))
    series_id = "wrf-series-" + hashlib.sha256(series_key.encode("utf-8")).hexdigest()[:16]
    continuity = _continuity("WRF", times)
    domain = _wrf_series_key(base)[2]
    day = _wrf_series_key(base)[3]
    attributes: dict[str, list[str]] = {}
    for item in ordered:
        for name, values in (item.get("attributes") or {}).items():
            attributes.setdefault(name, []).extend(str(value) for value in values)
    attributes = {name: sorted(set(values)) for name, values in attributes.items()}
    attributes["domains"] = [domain]
    return {
        **base,
        "file_uuid": series_id,
        "file_uuids": member_ids,
        "file_name": f"WRF {domain} {day}",
        "meta_path": None,
        "meta": meta,
        "times": times,
        "time_start": times[0] if times else None,
        "time_end": times[-1] if times else None,
        "frame_count": len(times),
        "webp_count": sum(int(item.get("webp_count") or 0) for item in ordered),
        "elements": sorted({value for item in ordered for value in item.get("elements") or []}),
        "resolutions": sorted({value for item in ordered for value in item.get("resolutions") or []}),
        "attributes": attributes,
        "members": [
            {
                "file_uuid": item.get("file_uuid"),
                "time_start": item.get("time_start"),
                "time_end": item.get("time_end"),
            }
            for item in ordered
        ],
        **continuity,
    }


def _cma_series_key(payload: dict[str, Any]) -> tuple[Any, ...]:
    attributes = payload.get("attributes") or {}
    return (
        str(payload.get("owner_uuid") or "public"),
        str(payload.get("visibility") or "private"),
        tuple(payload.get("elements") or []),
        tuple(payload.get("resolutions") or []),
        tuple(attributes.get("product_types") or []),
        tuple(attributes.get("product_names") or []),
    )


def _series_step_seconds(items: list[dict[str, Any]], data_type: str) -> int | None:
    parsed = sorted({
        value
        for item in items
        if (value := _parse_resource_time(str(item.get("time_start") or ""))) is not None
    })
    default_step = EXPECTED_STEP_SECONDS.get(data_type)
    if default_step:
        return int(default_step)
    deltas = [
        int((right - left).total_seconds())
        for left, right in zip(parsed, parsed[1:])
        if right > left
    ]
    if not deltas:
        return None
    counts = Counter(deltas)
    return min(counts, key=lambda value: (-counts[value], value))


def _split_cma_series_runs(items: list[dict[str, Any]]) -> list[list[dict[str, Any]]]:
    by_time: dict[str, dict[str, Any]] = {}
    for item in sorted(
        items,
        key=lambda value: (
            value.get("parse_finished_at") or value.get("create_time") or "",
            value.get("file_uuid") or "",
        ),
        reverse=True,
    ):
        time_key = str(item.get("time_start") or "")
        if time_key and time_key not in by_time:
            by_time[time_key] = item
    ordered = sorted(by_time.values(), key=lambda item: item.get("time_start") or "")
    if len(ordered) < 2:
        return [ordered] if ordered else []

    expected_step = _series_step_seconds(ordered, "CMA")
    if not expected_step:
        return [[item] for item in ordered]
    runs: list[list[dict[str, Any]]] = [[ordered[0]]]
    for item in ordered[1:]:
        previous_time = _parse_resource_time(str(runs[-1][-1].get("time_start") or ""))
        current_time = _parse_resource_time(str(item.get("time_start") or ""))
        delta = int((current_time - previous_time).total_seconds()) if previous_time and current_time else None
        if delta == expected_step:
            runs[-1].append(item)
        else:
            runs.append([item])
    return runs


def _merge_cma_series(items: list[dict[str, Any]]) -> dict[str, Any]:
    ordered = sorted(items, key=lambda item: (item.get("time_start") or "", item.get("file_uuid") or ""))
    base = deepcopy(ordered[-1])
    times = sorted({time for item in ordered for time in item.get("times") or []})
    member_ids = [str(item["file_uuid"]) for item in ordered]
    attributes: dict[str, list[str]] = {}
    for item in ordered:
        for name, values in (item.get("attributes") or {}).items():
            attributes.setdefault(name, []).extend(str(value) for value in values)
    attributes = {name: sorted(set(values)) for name, values in attributes.items()}
    identity = json.dumps(
        [_cma_series_key(base), times[0] if times else "", times[-1] if times else ""],
        ensure_ascii=True,
        separators=(",", ":"),
    )
    series_id = "cma-series-" + hashlib.sha256(identity.encode("utf-8")).hexdigest()[:16]
    return {
        **base,
        "file_uuid": series_id,
        "file_uuids": member_ids,
        "file_name": f"CMA {times[0]} - {times[-1]}" if times else "CMA 时间序列",
        "times": times,
        "time_start": times[0] if times else None,
        "time_end": times[-1] if times else None,
        "frame_count": len(times),
        "webp_count": sum(int(item.get("webp_count") or 0) for item in ordered),
        "elements": sorted({value for item in ordered for value in item.get("elements") or []}),
        "resolutions": sorted({value for item in ordered for value in item.get("resolutions") or []}),
        "attributes": attributes,
        "members": [
            {
                "file_uuid": item.get("file_uuid"),
                "time_start": item.get("time_start"),
                "time_end": item.get("time_end"),
            }
            for item in ordered
        ],
        **_continuity("CMA", times),
    }


def _catalog_payloads(resources: list[dict[str, Any]], data_type: str) -> list[dict[str, Any]]:
    if data_type == "CMA":
        groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
        for resource in resources:
            payload = _resource_payload(resource)
            groups.setdefault(_cma_series_key(payload), []).append(payload)
        series = [
            _merge_cma_series(run)
            for items in groups.values()
            for run in _split_cma_series_runs(items)
        ]
        return sorted(
            series,
            key=lambda item: (item.get("time_end") or "", item.get("file_uuid") or ""),
            reverse=True,
        )
    if data_type != "WRF":
        return [_resource_payload(item) for item in resources]
    groups: dict[tuple[str, str, str, str], list[dict[str, Any]]] = {}
    for resource in resources:
        payload = _resource_payload(resource, include_meta=True)
        groups.setdefault(_wrf_series_key(payload), []).append(payload)
    return [_merge_wrf_series(items) for items in groups.values()]


@contextmanager
def _merge_lock(target: Path):
    target.mkdir(parents=True, exist_ok=True)
    lock_path = target / ".merge.lock"
    descriptor: int | None = None
    try:
        descriptor = os.open(lock_path, os.O_CREAT | os.O_EXCL | os.O_WRONLY)
    except FileExistsError as exc:
        raise HTTPException(status_code=409, detail="该文件正在合并，请稍后重试。") from exc
    try:
        os.write(descriptor, str(os.getpid()).encode("ascii"))
        os.close(descriptor)
        descriptor = None
        yield
    finally:
        if descriptor is not None:
            os.close(descriptor)
        try:
            lock_path.unlink()
        except FileNotFoundError:
            pass


def _raw_destination(user_uuid: str, data_type: str, file_uuid: str, suffix: str) -> Path:
    now = utcnow()
    destination = (
        RAW_STORAGE_ROOT
        / "user_upload"
        / user_uuid
        / data_type
        / f"{now:%Y}"
        / f"{now:%m}"
        / file_uuid
        / f"{file_uuid}{suffix.lower()}"
    ).resolve()
    try:
        destination.relative_to(RAW_STORAGE_ROOT)
    except ValueError as exc:
        raise RuntimeError("generated raw path escaped RAW_STORAGE_ROOT") from exc
    return destination


def _resolve_existing_raw(source_path: str) -> Path | None:
    candidate = (RAW_STORAGE_ROOT / source_path).resolve()
    try:
        candidate.relative_to(RAW_STORAGE_ROOT)
    except ValueError:
        return None
    return candidate if candidate.is_file() else None


@app.get("/")
def root() -> dict[str, Any]:
    return ok({"service": "weather-data-upload-backend", "docs": "/docs"})


@app.get("/api/health")
def health() -> dict[str, Any]:
    try:
        with engine.connect() as conn:
            conn.exec_driver_sql("SELECT 1")
        database = "online"
    except Exception:
        database = "offline"
    return ok({"status": "online", "database": database})


class CatalogSeriesPayload(BaseModel):
    file_uuids: list[str] = Field(min_length=1, max_length=200)


@app.get("/api/catalog/resources")
def catalog_resources(
    request: Request,
    data_type: str = Query(...),
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
    time_start: datetime | None = Query(default=None),
    time_end: datetime | None = Query(default=None),
) -> dict[str, Any]:
    user = request_user(request)
    normalized = normalize_data_type(data_type).upper()
    normalized_start = time_start.astimezone(UTC).replace(tzinfo=None) if time_start and time_start.tzinfo else time_start
    normalized_end = time_end.astimezone(UTC).replace(tzinfo=None) if time_end and time_end.tzinfo else time_end
    if normalized_start and normalized_end and normalized_end < normalized_start:
        raise HTTPException(status_code=400, detail="结束时间不能早于开始时间。")
    items, total = list_display_resources(
        engine,
        user["uuid"],
        int(user.get("role") or 0),
        normalized,
        limit=limit,
        offset=offset,
        time_start=normalized_start,
        time_end=normalized_end,
    )
    catalog_items = _catalog_payloads(items, normalized)
    for item in catalog_items:
        item.pop("meta", None)
    return ok({
        "items": catalog_items,
        "total": total,
        "limit": limit,
        "offset": offset,
    })


@app.post("/api/catalog/reconcile")
def catalog_reconcile(
    data_type: str = Query(...),
    apply: bool = Query(default=False),
) -> dict[str, Any]:
    try:
        return ok(reconcile_catalog(engine, PRODUCT_DATA_ROOT, data_type, apply=apply))
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc


@app.get("/api/catalog/resources/{file_uuid}")
def catalog_resource(request: Request, file_uuid: str) -> dict[str, Any]:
    user = request_user(request)
    resource = get_display_resource(
        engine,
        user["uuid"],
        int(user.get("role") or 0),
        file_uuid,
    )
    if resource is None:
        raise HTTPException(status_code=404, detail="数据不存在或无权访问。")
    return ok(_resource_payload(resource, include_meta=True))


@app.get("/api/catalog/nowcast/latest")
def latest_nowcast_inputs(request: Request) -> dict[str, Any]:
    """Return the newest five model-ready radar frames across all users."""
    user = request_user(request)
    if int(user.get("role") or 0) < 2:
        raise HTTPException(status_code=403, detail="权限不足。")

    # Short-term forecasting is a shared operational workflow: role >= 2 can
    # select any successfully parsed radar resource, regardless of its owner.
    resources, _ = list_display_resources(
        engine,
        str(user["uuid"]),
        3,
        "RADAR",
        limit=500,
        offset=0,
    )
    candidates: list[dict[str, Any]] = []
    rejected: list[dict[str, str]] = []
    for resource in resources:
        try:
            source, meta = _nowcast_input_path(resource)
            payload = _resource_payload(resource)
            times = payload.get("times") or []
            if len(times) != 1:
                raise HTTPException(status_code=409, detail="短临输入必须是一文件一时次。")
            candidates.append(
                {
                    "file_uuid": str(resource["file_uuid"]),
                    "file_name": str(resource.get("original_file_name") or source.name),
                    "valid_time": str(times[0]),
                    "owner_uuid": str(resource.get("user_uuid") or ""),
                    "file_size": int(source.stat().st_size),
                    "variable": NOWCAST_VARIABLE,
                    "extent": meta.get("bbox") or (meta.get("weather_info") or {}).get("extent"),
                }
            )
        except HTTPException as exc:
            rejected.append(
                {
                    "file_uuid": str(resource.get("file_uuid") or ""),
                    "reason": str(exc.detail),
                }
            )

    selected = _select_latest_nowcast_window(candidates)
    if not selected:
        latest_time = max((item["valid_time"] for item in candidates), default=None)
        raise HTTPException(
            status_code=409,
            detail={
                "message": "数据库中没有找到连续5帧、间隔6分钟的模型可用雷达数据。",
                "model_ready_count": len(candidates),
                "latest_time": latest_time,
                "rejected_count": len(rejected),
            },
        )

    return ok(
        {
            "model_id": "precipitation_nowcasting",
            "run_mode": "forecast",
            "frames": selected,
            "frame_count": NOWCAST_FRAME_COUNT,
            "step_seconds": NOWCAST_STEP_SECONDS,
            "forecast_frames": 20,
            "forecast_minutes": 120,
        },
        message="已选择数据库中最新的连续5帧雷达数据。",
    )


@app.get("/api/catalog/nowcast/resources/{file_uuid}/input")
def nowcast_resource_input(request: Request, file_uuid: str) -> FileResponse:
    """Stream one validated CAP_FMT NetCDF to the dedicated model backend."""
    user = request_user(request)
    if int(user.get("role") or 0) < 2:
        raise HTTPException(status_code=403, detail="权限不足。")
    resource = get_display_resource(
        engine,
        str(user["uuid"]),
        3,
        file_uuid,
    )
    if resource is None:
        raise HTTPException(status_code=404, detail="雷达数据不存在或尚未解析成功。")
    source, _ = _nowcast_input_path(resource)
    original_name = Path(str(resource.get("original_file_name") or source.name)).name
    return FileResponse(
        path=source,
        media_type="application/x-netcdf",
        filename=original_name,
        headers={
            "X-Weather-File-Name": original_name,
            "X-Weather-File-UUID": file_uuid,
        },
    )


@app.post("/api/catalog/series")
def catalog_series(request: Request, payload: CatalogSeriesPayload) -> dict[str, Any]:
    user = request_user(request)
    file_uuids = list(dict.fromkeys(payload.file_uuids))
    resources = []
    for file_uuid in file_uuids:
        resource = get_display_resource(
            engine,
            user["uuid"],
            int(user.get("role") or 0),
            file_uuid,
        )
        if resource is None:
            raise HTTPException(status_code=404, detail="序列中的数据不存在或无权访问。")
        resources.append(resource)
    data_types = {str(item.get("data_type") or "").upper() for item in resources}
    items = [_resource_payload(resource, include_meta=True) for resource in resources]
    if data_types == {"WRF"}:
        keys = {_wrf_series_key(item) for item in items}
        if len(keys) != 1:
            raise HTTPException(status_code=400, detail="所选 WRF 文件不属于同一区域和日期。")
        return ok(_merge_wrf_series(items))
    if data_types == {"CMA"}:
        keys = {_cma_series_key(item) for item in items}
        if len(keys) != 1:
            raise HTTPException(status_code=400, detail="所选 CMA 文件不属于同一产品序列。")
        merged = _merge_cma_series(items)
        if not merged.get("continuous"):
            raise HTTPException(status_code=409, detail="所选 CMA 文件的时间间隔不连续。")
        return ok(merged)
    raise HTTPException(status_code=400, detail="当前版本仅支持合并 WRF 或 CMA 单帧序列。")


class CollectionPrepareFile(BaseModel):
    file_id: str = Field(min_length=1, max_length=1024)
    file_name: str = Field(min_length=1, max_length=255)
    file_size: int = Field(ge=0, le=MAX_FILE_SIZE)
    last_modified: int = Field(default=0, ge=0)


class CollectionPreparePayload(BaseModel):
    data_type: str
    files: list[CollectionPrepareFile] = Field(min_length=1, max_length=500)


@app.post("/api/upload/collections/prepare")
def prepare_upload_collections(
    request: Request,
    payload: CollectionPreparePayload,
) -> dict[str, Any]:
    user = request_user(request)
    data_type = normalize_data_type(payload.data_type)
    if data_type not in SATELLITE_UPLOAD_TYPES:
        raise HTTPException(status_code=400, detail="集合准备只适用于 FY-3 与葵花卫星文件。")

    grouped: dict[str, list[tuple[CollectionPrepareFile, dict[str, Any]]]] = {}
    for item in payload.files:
        safe_name = _safe_original_name(item.file_name, data_type)
        descriptor = _satellite_descriptor(safe_name, data_type)
        grouped.setdefault(descriptor["scene_key"], []).append((item, descriptor))

    assignments: list[dict[str, Any]] = []
    collections: list[dict[str, Any]] = []
    for scene_key, items in grouped.items():
        replacements: dict[str, list[str]] = {}
        for _item, descriptor in items:
            group = descriptor["role_group"]
            roles = descriptor["group_roles"]
            if group in replacements and replacements[group] != roles:
                raise HTTPException(
                    status_code=400,
                    detail=f"同一场景的 {group.rstrip(':')} 分段总数不一致。",
                )
            replacements[group] = roles
        expected_roles = set(_initial_satellite_roles(data_type))
        for prefix, roles in replacements.items():
            expected_roles = {role for role in expected_roles if not role.startswith(prefix)}
            expected_roles.update(roles)
        collection = create_or_get_satellite_collection(
            engine,
            user["uuid"],
            data_type,
            scene_key,
            sorted(expected_roles),
            role_replacements=replacements,
        )
        collections.append(collection)
        for item, descriptor in items:
            assignments.append({
                "file_id": item.file_id,
                "file_name": item.file_name,
                "collection_uuid": collection["collection_uuid"],
                "scene_key": scene_key,
                "role": descriptor["role"],
                "collection_status": collection["status"],
                "missing_roles": collection["missing_roles"],
            })
    return ok({"assignments": assignments, "collections": collections}, "集合准备完成")


@app.get("/api/upload/collections")
def upload_collections(
    request: Request,
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    user = request_user(request)
    items, total = list_user_satellite_collections(
        engine,
        user["uuid"],
        limit=limit,
        offset=offset,
    )
    return ok({"items": items, "total": total, "limit": limit, "offset": offset})


@app.get("/api/upload/collections/{collection_uuid}")
def upload_collection(request: Request, collection_uuid: str) -> dict[str, Any]:
    user = request_user(request)
    collection = get_user_satellite_collection(engine, user["uuid"], collection_uuid)
    if collection is None:
        raise HTTPException(status_code=404, detail="集合不存在或无权访问。")
    return ok(collection)


@app.post("/api/upload/collections/{collection_uuid}/retry")
def retry_upload_collection(request: Request, collection_uuid: str) -> dict[str, Any]:
    user = request_user(request)
    collection = retry_user_satellite_collection(engine, user["uuid"], collection_uuid)
    if collection is None:
        raise HTTPException(status_code=409, detail="集合不存在、尚未完整或当前状态不可重试。")
    return ok(collection, "集合已重新进入解析队列")


@app.get("/api/upload/status")
def upload_status(request: Request, file_id: str) -> dict[str, Any]:
    user = request_user(request)
    session_id = safe_file_id(file_id)
    completed = get_upload_task_by_session(engine, user["uuid"], session_id)
    if completed:
        collection = None
        if completed.get("collection_uuid"):
            collection = get_user_satellite_collection(
                engine,
                user["uuid"],
                str(completed["collection_uuid"]),
            )
        return ok({
            "uploaded": [],
            "completed": _task_payload(
                completed,
                duplicate_content=_is_duplicate_content(completed),
                collection=collection,
            ),
        })

    target = chunk_dir(user["uuid"], file_id)
    uploaded: list[int] = []
    if target.is_dir():
        for part in target.glob("*.part"):
            try:
                uploaded.append(int(part.stem))
            except ValueError:
                continue
    return ok({"uploaded": sorted(uploaded), "completed": None})


@app.post("/api/upload/chunk")
async def upload_chunk(
    request: Request,
    file_id: str = Form(...),
    chunk_index: int = Form(...),
    total_chunks: int = Form(...),
    chunk: UploadFile = File(...),
) -> dict[str, Any]:
    user = request_user(request)
    _validate_total_chunks(total_chunks)
    if chunk_index < 0 or chunk_index >= total_chunks:
        raise HTTPException(status_code=400, detail="chunk_index 越界。")

    target_dir = chunk_dir(user["uuid"], file_id)
    target_dir.mkdir(parents=True, exist_ok=True)
    session = _load_session(target_dir)
    if session and int(session.get("total_chunks", -1)) != total_chunks:
        raise HTTPException(status_code=409, detail="同一上传会话的 total_chunks 不一致。")
    if session is None:
        _write_session(target_dir, {"total_chunks": total_chunks, "updated_at": utcnow().isoformat()})

    part_path = target_dir / f"{chunk_index}.part"
    if part_path.exists():
        return ok({"received": chunk_index, "existing": True})

    tmp_path = part_path.with_suffix(".part.tmp")
    size = 0
    try:
        with tmp_path.open("wb") as output:
            while block := chunk.file.read(1024 * 1024):
                size += len(block)
                if size > MAX_CHUNK_SIZE:
                    raise HTTPException(status_code=413, detail="单个分片超过大小限制。")
                output.write(block)
        tmp_path.replace(part_path)
    finally:
        if tmp_path.exists():
            tmp_path.unlink()
    return ok({"received": chunk_index, "existing": False})


class CompletePayload(BaseModel):
    file_id: str
    file_name: str
    total_chunks: int = Field(ge=1)
    data_type: str
    collection_uuid: str | None = None
    collection_role: str | None = None


@app.post("/api/upload/complete")
def upload_complete(request: Request, payload: CompletePayload) -> dict[str, Any]:
    user = request_user(request)
    user_uuid = user["uuid"]
    data_type = normalize_data_type(payload.data_type)
    _validate_queued_upload_type(data_type)
    _validate_total_chunks(payload.total_chunks)
    safe_name = _safe_original_name(payload.file_name, data_type)
    upload_session_id = safe_file_id(payload.file_id)
    collection: dict[str, Any] | None = None
    collection_role: str | None = None
    if data_type in SATELLITE_UPLOAD_TYPES:
        if not payload.collection_uuid or not payload.collection_role:
            raise HTTPException(status_code=400, detail="卫星文件必须先准备集合并提交 collection_uuid 与 collection_role。")
        descriptor = _satellite_descriptor(safe_name, data_type)
        if payload.collection_role != descriptor["role"]:
            raise HTTPException(status_code=409, detail="collection_role 与文件名解析出的角色不一致。")
        collection = get_user_satellite_collection(engine, user_uuid, payload.collection_uuid)
        if collection is None:
            raise HTTPException(status_code=404, detail="集合不存在或无权访问。")
        if collection.get("data_type") != data_type or collection.get("scene_key") != descriptor["scene_key"]:
            raise HTTPException(status_code=409, detail="文件不属于指定的卫星集合。")
        if collection.get("status") not in {"collecting", "ready"}:
            raise HTTPException(status_code=409, detail="集合已开始解析或已经结束，不能再追加成员。")
        collection_role = payload.collection_role
    elif payload.collection_uuid or payload.collection_role:
        raise HTTPException(status_code=400, detail="非卫星文件不能指定集合信息。")

    completed = get_upload_task_by_session(engine, user_uuid, upload_session_id)
    if completed:
        completed_collection = None
        if completed.get("collection_uuid"):
            completed_collection = get_user_satellite_collection(
                engine,
                user_uuid,
                str(completed["collection_uuid"]),
            )
        return ok(
            _task_payload(
                completed,
                duplicate_content=_is_duplicate_content(completed),
                collection=completed_collection,
            )
        )

    target_dir = chunk_dir(user_uuid, payload.file_id)
    with _merge_lock(target_dir):
        completed = get_upload_task_by_session(engine, user_uuid, upload_session_id)
        if completed:
            completed_collection = None
            if completed.get("collection_uuid"):
                completed_collection = get_user_satellite_collection(
                    engine,
                    user_uuid,
                    str(completed["collection_uuid"]),
                )
            return ok(
                _task_payload(
                    completed,
                    duplicate_content=_is_duplicate_content(completed),
                    collection=completed_collection,
                )
            )

        session = _load_session(target_dir)
        if session and int(session.get("total_chunks", -1)) != payload.total_chunks:
            raise HTTPException(status_code=409, detail="完成请求与上传会话的分片数量不一致。")
        missing = [
            index
            for index in range(payload.total_chunks)
            if not (target_dir / f"{index}.part").exists()
        ]
        if missing:
            raise HTTPException(
                status_code=400,
                detail={"message": "分片缺失，无法合并。", "missing": missing[:100]},
            )

        merged = target_dir / "combined.merging"
        digest = hashlib.sha256()
        total_size = 0
        with merged.open("wb") as output:
            for index in range(payload.total_chunks):
                part = target_dir / f"{index}.part"
                with part.open("rb") as source:
                    while block := source.read(1024 * 1024):
                        total_size += len(block)
                        if total_size > MAX_FILE_SIZE:
                            raise HTTPException(status_code=413, detail="文件超过大小限制。")
                        digest.update(block)
                        output.write(block)

        if total_size == 0:
            merged.unlink(missing_ok=True)
            shutil.rmtree(target_dir, ignore_errors=True)
            raise HTTPException(status_code=400, detail="上传文件为空，未创建解析任务。")

        file_hash = digest.hexdigest()
        if collection is not None and collection_role is not None:
            existing_member = get_collection_member_task_by_role(
                engine,
                str(collection["collection_uuid"]),
                collection_role,
            )
            if existing_member is not None:
                if existing_member.get("file_hash") != file_hash:
                    raise HTTPException(
                        status_code=409,
                        detail=f"集合角色 {collection_role} 已存在且内容不同，不能覆盖。",
                    )
                merged.unlink(missing_ok=True)
                shutil.rmtree(target_dir, ignore_errors=True)
                refreshed = get_user_satellite_collection(
                    engine,
                    user_uuid,
                    str(collection["collection_uuid"]),
                )
                return ok(
                    _task_payload(
                        existing_member,
                        duplicate_content=True,
                        collection=refreshed,
                    ),
                    "相同集合成员已存在，已复用",
                )

        duplicate = (
            find_user_duplicate(engine, user_uuid, data_type, file_hash)
            if collection is None
            else None
        )
        if duplicate is not None:
            merged.unlink(missing_ok=True)
            shutil.rmtree(target_dir, ignore_errors=True)
            return ok(
                _task_payload(duplicate, duplicate_content=True),
                "相同数据已存在，未重复入库或解析",
            )

        file_uuid = str(uuid.uuid4())
        suffix = Path(safe_name).suffix
        destination = _raw_destination(user_uuid, data_type, file_uuid, suffix)
        destination.parent.mkdir(parents=True, exist_ok=False)
        try:
            merged.replace(destination)

            relative_source = destination.relative_to(RAW_STORAGE_ROOT).as_posix()
            task_values = {
                "file_uuid": file_uuid,
                "upload_session_id": upload_session_id,
                "user_uuid": user_uuid,
                "collection_uuid": payload.collection_uuid,
                "data_type": data_type,
                "file_type": suffix.lstrip(".").upper() or "WRF",
                "original_file_name": safe_name,
                "stored_file_name": destination.name,
                "source_path": relative_source,
                "file_size": total_size,
                "file_hash": file_hash,
                "source_file_uuid": None,
                "remark": None,
            }
            if collection is not None and collection_role is not None:
                task, collection = create_satellite_collection_upload_task(
                    engine,
                    task_values,
                    collection_uuid=str(collection["collection_uuid"]),
                    role=collection_role,
                )
            else:
                task = create_upload_task(engine, task_values)
        except IntegrityError:
            if destination.exists():
                destination.unlink()
            try:
                destination.parent.rmdir()
            except OSError:
                pass
            completed = get_upload_task_by_session(engine, user_uuid, upload_session_id)
            if completed:
                shutil.rmtree(target_dir, ignore_errors=True)
                completed_collection = None
                if completed.get("collection_uuid"):
                    completed_collection = get_user_satellite_collection(
                        engine,
                        user_uuid,
                        str(completed["collection_uuid"]),
                    )
                return ok(
                    _task_payload(
                        completed,
                        duplicate_content=_is_duplicate_content(completed),
                        collection=completed_collection,
                    )
                )
            raise
        except ValueError as exc:
            if destination.exists():
                destination.unlink()
            try:
                destination.parent.rmdir()
            except OSError:
                pass
            raise HTTPException(status_code=409, detail=str(exc)) from exc
        except Exception:
            if destination.exists():
                destination.unlink()
            try:
                destination.parent.rmdir()
            except OSError:
                pass
            raise

        shutil.rmtree(target_dir, ignore_errors=True)
        message = "上传完成，集合已进入解析队列" if collection and collection.get("status") == "ready" else "上传完成"
        if collection and collection.get("status") == "collecting":
            message = "上传完成，等待集合其余成员"
        elif collection is None:
            message = "上传完成，已进入解析队列"
        return ok(
            _task_payload(
                task,
                duplicate_content=False,
                collection=collection,
            ),
            message,
        )


@app.get("/api/upload/tasks")
def upload_tasks(
    request: Request,
    response: Response,
    limit: int = Query(default=100, ge=1, le=200),
    offset: int = Query(default=0, ge=0),
) -> dict[str, Any]:
    user = request_user(request)
    tasks, total = list_user_tasks(engine, user["uuid"], limit=limit, offset=offset)
    summary = summarize_user_tasks(engine, user["uuid"])
    items = []
    collections: dict[str, dict[str, Any] | None] = {}
    for task in tasks:
        collection = None
        collection_uuid = task.get("collection_uuid")
        if collection_uuid:
            key = str(collection_uuid)
            if key not in collections:
                collections[key] = get_user_satellite_collection(engine, user["uuid"], key)
            collection = collections[key]
        items.append(_task_payload(task, collection=collection))
    response.headers["Cache-Control"] = "no-store, no-cache, must-revalidate, max-age=0"
    response.headers["Pragma"] = "no-cache"
    return ok({
        "items": items,
        "total": total,
        "summary": summary,
        "limit": limit,
        "offset": offset,
    })


@app.get("/api/upload/tasks/{file_uuid}")
def upload_task(request: Request, file_uuid: str) -> dict[str, Any]:
    user = request_user(request)
    task = get_user_task(engine, user["uuid"], file_uuid)
    if task is None:
        raise HTTPException(status_code=404, detail="上传任务不存在。")
    collection = None
    if task.get("collection_uuid"):
        collection = get_user_satellite_collection(
            engine,
            user["uuid"],
            str(task["collection_uuid"]),
        )
    return ok(_task_payload(task, collection=collection))


@app.post("/api/upload/tasks/{file_uuid}/retry")
def retry_upload_task(request: Request, file_uuid: str) -> dict[str, Any]:
    user = request_user(request)
    existing = get_user_task(engine, user["uuid"], file_uuid)
    if existing and existing.get("collection_uuid"):
        raise HTTPException(
            status_code=409,
            detail="卫星集合成员不能单独重试，请使用集合重试接口。",
        )
    task = retry_user_task(engine, user["uuid"], file_uuid)
    if task is None:
        raise HTTPException(status_code=409, detail="仅失败任务可以重新进入解析队列。")
    return ok(_task_payload(task), "任务已重新进入解析队列")


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="127.0.0.1", port=8003)
