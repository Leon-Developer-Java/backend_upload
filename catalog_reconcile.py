from __future__ import annotations

import hashlib
import json
import sys
import uuid
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from sqlalchemy import delete, insert, select, update


WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
BACKEND_SYSTEM_ROOT = WORKSPACE_ROOT / "backend_system"
for value in (WORKSPACE_ROOT, BACKEND_SYSTEM_ROOT):
    if str(value) not in sys.path:
        sys.path.insert(0, str(value))

from DB.schema import DETAIL_TABLES, public_info
from services.asset_catalog import build_asset_catalog


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


def _meta_files(product_root: Path, data_type: str) -> list[Path]:
    root = product_root / ("Himawari" if data_type == "HIMAWARI" else data_type)
    if not root.is_dir():
        return []
    return sorted(
        path
        for path in root.rglob("*.meta.json")
        if path.is_file() and ".adapter_staging" not in path.parts
    )


def _final_dir(meta_path: Path) -> Path:
    return meta_path.parent.parent if meta_path.parent.name == "meta" else meta_path.parent


def reconcile_catalog(
    engine: Any,
    product_root: Path,
    data_type: str,
    *,
    apply: bool = False,
) -> dict[str, Any]:
    normalized = str(data_type or "").upper().replace("-", "")
    if normalized not in {"FY3", "HIMAWARI", "WRF"}:
        raise ValueError("目录校准仅支持 FY3、Himawari 和 WRF")
    product_root = product_root.resolve()
    detail_table = DETAIL_TABLES[normalized]
    files = _meta_files(product_root, normalized)
    candidates: list[dict[str, Any]] = []
    malformed: list[dict[str, str]] = []
    with engine.connect() as conn:
        existing_paths = {
            str(value)
            for value in conn.execute(
                select(public_info.c.meta_path).where(
                    public_info.c.data_type == ("Himawari" if normalized == "HIMAWARI" else normalized),
                    public_info.c.parse_status == "success",
                    public_info.c.is_deleted.is_(False),
                )
            ).scalars()
            if value
        }
    for meta_path in files:
        relative = meta_path.relative_to(product_root).as_posix()
        if relative in existing_paths:
            continue
        try:
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            if not isinstance(meta, dict):
                raise ValueError("metadata root is not an object")
            file_uuid = str(uuid.uuid5(uuid.NAMESPACE_URL, f"catalog-backfill:{relative}"))
            final_dir = _final_dir(meta_path)
            assets = build_asset_catalog(
                file_uuid=file_uuid,
                data_type=normalized,
                meta=meta,
                product_root=product_root,
                final_dir=final_dir,
            )
            if not assets:
                raise ValueError("metadata has no existing WebP assets")
            candidates.append({
                "file_uuid": file_uuid,
                "meta_path": relative,
                "meta": meta,
                "final_dir": final_dir,
                "assets": assets,
            })
        except (OSError, json.JSONDecodeError, ValueError) as exc:
            malformed.append({"meta_path": relative, "error": str(exc)})
    if apply:
        now = _utcnow()
        with engine.begin() as conn:
            for item in candidates:
                meta = item["meta"]
                assets = item["assets"]
                meta_path = product_root / item["meta_path"]
                webp_files = list(item["final_dir"].rglob("*.webp"))
                default = next((asset for asset in assets if asset.get("is_default")), assets[0])
                values = {
                    "user_uuid": None,
                    "acquisition_type": "manual_import",
                    "visibility": "public",
                    "data_type": "Himawari" if normalized == "HIMAWARI" else normalized,
                    "file_type": "WEBP",
                    "original_file_name": str(meta.get("scene_id") or meta.get("task_id") or meta_path.stem),
                    "stored_file_name": meta_path.name,
                    "source_path": f"legacy_display/{item['meta_path']}",
                    "file_size": sum(path.stat().st_size for path in webp_files) + meta_path.stat().st_size,
                    "file_hash": hashlib.sha256(meta_path.read_bytes()).hexdigest(),
                    "ingest_status": "success",
                    "parse_status": "success",
                    "parse_attempts": 0,
                    "parse_finished_at": now,
                    "meta_path": item["meta_path"],
                    "default_webp_url": default.get("webp_url"),
                    "webp_count": len(webp_files),
                    "adapter_name": "catalog_reconcile",
                    "adapter_version": "1.0",
                    "meta_schema_version": str(meta.get("schema_version") or "legacy"),
                    "is_pinned": False,
                    "is_deleted": False,
                    "download_count": 0,
                    "update_time": now,
                    "remark": "idempotent display catalog backfill",
                }
                existing = conn.execute(
                    select(public_info.c.file_uuid).where(public_info.c.file_uuid == item["file_uuid"])
                ).first()
                if existing is None:
                    conn.execute(insert(public_info).values(file_uuid=item["file_uuid"], create_time=now, **values))
                else:
                    conn.execute(update(public_info).where(public_info.c.file_uuid == item["file_uuid"]).values(**values))
                conn.execute(delete(detail_table).where(detail_table.c.file_uuid == item["file_uuid"]))
                allowed = set(detail_table.c.keys()) - {"id"}
                rows = []
                for asset in assets:
                    row = {key: asset.get(key) for key in allowed}
                    row["file_uuid"] = item["file_uuid"]
                    row["create_time"] = asset.get("create_time") or now
                    row["update_time"] = now
                    rows.append(row)
                conn.execute(insert(detail_table), rows)
    return {
        "data_type": normalized,
        "dry_run": not apply,
        "scanned": len(files),
        "already_registered": len(files) - len(candidates) - len(malformed),
        "candidate_count": len(candidates),
        "registered_count": len(candidates) if apply else 0,
        "malformed": malformed,
        "candidates": [
            {
                "file_uuid": item["file_uuid"],
                "meta_path": item["meta_path"],
                "webp_count": len(item["assets"]),
            }
            for item in candidates
        ],
    }
