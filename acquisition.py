from __future__ import annotations

import ftplib
import base64
import hashlib
import json
import os
import posixpath
import re
import sys
import threading
import time
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Callable
from urllib.parse import unquote, urlparse
from urllib.request import Request, urlopen

from sqlalchemy import insert, select, update

WORKSPACE_ROOT = Path(__file__).resolve().parent.parent
if str(WORKSPACE_ROOT) not in sys.path:
    sys.path.insert(0, str(WORKSPACE_ROOT))

from DB.repository import (
    create_or_get_satellite_collection,
    create_satellite_collection_upload_task,
    get_collection_member_task_by_role,
)
from DB.schema import auto_download_source, satellite_collection


FY3_RE = re.compile(
    r"^FY3(?P<sat>[A-Z])_MERSI_GBAL_L1_(?P<date>\d{8})_(?P<time>\d{4})_"
    r"(?P<role>1000M_MS|GEO1K_MS)\.HDF$",
    re.IGNORECASE,
)
HSD_RE = re.compile(
    r"^HS_H(?P<sat>\d{2})_(?P<date>\d{8})_(?P<time>\d{4})_"
    r"(?P<band>B\d{2})_(?P<region>[A-Z0-9]+)_(?P<resolution>R\d{2})_"
    r"S(?P<segment>\d{2})(?P<total>\d{2})\.DAT(?:\.bz2)?$",
    re.IGNORECASE,
)
HIMAWARI_BANDS = ("B01", "B02", "B03", "B13")


def _utcnow() -> datetime:
    return datetime.now(UTC).replace(tzinfo=None)


@dataclass(frozen=True)
class RemoteMember:
    name: str
    source_uri: str
    size: int | None = None
    sha256: str | None = None
    ftp_dir: str | None = None


@dataclass(frozen=True)
class RemoteScene:
    data_type: str
    scene_key: str
    members: tuple[RemoteMember, ...]


class AcquisitionService:
    def __init__(self, engine: Any, raw_root: Path):
        self.engine = engine
        self.raw_root = raw_root.resolve()
        self.enabled = os.getenv("SATELLITE_ACQUISITION_ENABLED", "0").strip().lower() in {"1", "true", "yes", "on"}
        self.interval_seconds = max(60, int(os.getenv("SATELLITE_ACQUISITION_INTERVAL_SECONDS", "600")))
        self._locks = {"FY3": threading.Lock(), "Himawari": threading.Lock()}
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._source_ids = {
            "FY3": str(uuid.uuid5(uuid.NAMESPACE_URL, "nsmc:FY3:MERSI:L1")),
            "Himawari": str(uuid.uuid5(uuid.NAMESPACE_URL, "jaxa:Himawari:AHI:HSD")),
        }
        self._ensure_sources()

    def _ensure_sources(self) -> None:
        now = _utcnow()
        definitions = {
            "FY3": ("FY-3 NSMC MERSI L1", "NSMC", os.getenv("FY3_NSMC_BASE_URL", "https://data.nsmc.org.cn")),
            "Himawari": ("Himawari JAXA HSD", "JAXA", os.getenv("HIMAWARI_FTP_HOST", "ftp.ptree.jaxa.jp")),
        }
        with self.engine.begin() as conn:
            # SQLite treats NULL values as distinct in a UNIQUE constraint, so
            # the existing user-owned identity does not protect source-owned
            # automatic collections. This partial index closes that gap.
            conn.exec_driver_sql(
                "CREATE UNIQUE INDEX IF NOT EXISTS uq_satellite_collection_source_generation "
                "ON satellite_collection(source_uuid, data_type, scene_key, generation) "
                "WHERE source_uuid IS NOT NULL"
            )
            for data_type, (name, provider, base_uri) in definitions.items():
                source_uuid = self._source_ids[data_type]
                row = conn.execute(
                    select(auto_download_source).where(auto_download_source.c.source_uuid == source_uuid)
                ).first()
                values = {
                    "source_name": name,
                    "data_type": data_type,
                    "provider": provider,
                    "source_base_uri": base_uri,
                    "credential_ref": "environment",
                    "schedule_expression": f"interval:{self.interval_seconds}s",
                    "enabled": self.enabled and self._provider_enabled(data_type),
                    "adapter_name": "satellite_collection",
                    "adapter_version": "1.0",
                    "target_visibility": "public",
                    "target_directory": f"auto_download/{source_uuid}/{data_type}",
                    "update_time": now,
                }
                if row is None:
                    conn.execute(insert(auto_download_source).values(source_uuid=source_uuid, create_time=now, **values))
                else:
                    conn.execute(update(auto_download_source).where(auto_download_source.c.source_uuid == source_uuid).values(**values))

    @staticmethod
    def _provider_enabled(data_type: str) -> bool:
        name = "FY3_NSMC_ENABLED" if data_type == "FY3" else "HIMAWARI_ACQUISITION_ENABLED"
        return os.getenv(name, "0").strip().lower() in {"1", "true", "yes", "on"}

    def start(self) -> None:
        if not self.enabled or self._thread is not None:
            return
        self._stop.clear()
        self._thread = threading.Thread(target=self._run_scheduler, name="satellite-acquisition", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=5)
        self._thread = None

    def _run_scheduler(self) -> None:
        while not self._stop.is_set():
            for data_type in ("FY3", "Himawari"):
                if self._provider_enabled(data_type):
                    try:
                        self.sync(data_type)
                    except Exception:
                        pass
            self._stop.wait(self.interval_seconds)

    def _source_row(self, data_type: str) -> dict[str, Any]:
        source_uuid = self._source_ids[data_type]
        with self.engine.connect() as conn:
            row = conn.execute(
                select(auto_download_source).where(auto_download_source.c.source_uuid == source_uuid)
            ).first()
        return dict(row._mapping) if row else {}

    def status(self, data_type: str) -> dict[str, Any]:
        row = self._source_row(data_type)
        configured = self._provider_enabled(data_type)
        return {
            "data_type": data_type,
            "provider": row.get("provider"),
            "enabled": bool(self.enabled and configured),
            "configured": configured,
            "status": "running" if self._locks[data_type].locked() else ("idle" if configured else "disabled"),
            "last_run_at": row.get("last_run_at"),
            "last_success_at": row.get("last_success_at"),
            "last_error": row.get("last_error"),
            "next_run_at": row.get("next_run_at"),
        }

    def scenes(self, data_type: str, limit: int = 100) -> list[dict[str, Any]]:
        source_uuid = self._source_ids[data_type]
        with self.engine.connect() as conn:
            rows = conn.execute(
                select(satellite_collection)
                .where(satellite_collection.c.source_uuid == source_uuid)
                .order_by(satellite_collection.c.create_time.desc())
                .limit(max(1, min(200, int(limit))))
            ).all()
        items = []
        for row in rows:
            item = dict(row._mapping)
            for key in ("expected_roles_json", "received_roles_json", "missing_roles_json"):
                try:
                    item[key.removesuffix("_json")] = json.loads(item.pop(key) or "[]")
                except json.JSONDecodeError:
                    item[key.removesuffix("_json")] = []
            items.append(item)
        return items

    def _mark_source(self, data_type: str, *, success: bool, error: str | None = None) -> None:
        now = _utcnow()
        values = {
            "last_run_at": now,
            "last_error": error,
            "next_run_at": now + timedelta(seconds=self.interval_seconds),
            "update_time": now,
        }
        if success:
            values["last_success_at"] = now
        with self.engine.begin() as conn:
            conn.execute(
                update(auto_download_source)
                .where(auto_download_source.c.source_uuid == self._source_ids[data_type])
                .values(**values)
            )

    def sync(self, data_type: str) -> dict[str, Any]:
        if data_type not in self._locks:
            raise ValueError("自动采集仅支持 FY3 和 Himawari")
        if not self._provider_enabled(data_type):
            raise ValueError(f"{data_type} 自动采集未配置或未启用")
        lock = self._locks[data_type]
        if not lock.acquire(blocking=False):
            raise RuntimeError(f"{data_type} 自动采集正在运行")
        try:
            scenes = self._discover_fy3() if data_type == "FY3" else self._discover_himawari()
            results = [self._ingest_scene(scene) for scene in scenes]
            self._mark_source(data_type, success=True)
            return {"data_type": data_type, "scene_count": len(scenes), "scenes": results}
        except Exception as exc:
            self._mark_source(data_type, success=False, error=str(exc))
            raise
        finally:
            lock.release()

    def _discover_fy3(self) -> list[RemoteScene]:
        manifest_location = os.getenv("FY3_NSMC_MANIFEST", "").strip()
        if not manifest_location:
            raise ValueError("请配置 FY3_NSMC_MANIFEST（NSMC 订单下载链接清单）")
        text = self._read_manifest(manifest_location)
        entries: list[dict[str, Any]] = []
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            payload = [line.strip() for line in text.splitlines() if line.strip() and not line.lstrip().startswith("#")]
        if isinstance(payload, dict):
            payload = payload.get("files") or payload.get("items") or []
        for item in payload if isinstance(payload, list) else []:
            entry = {"url": item} if isinstance(item, str) else dict(item)
            url = str(entry.get("url") or entry.get("source_uri") or "").strip()
            name = str(entry.get("name") or unquote(Path(urlparse(url).path).name)).strip()
            if url and FY3_RE.match(name):
                entries.append({**entry, "url": url, "name": name})
        grouped: dict[str, list[RemoteMember]] = {}
        for entry in entries:
            match = FY3_RE.match(entry["name"])
            assert match is not None
            scene_key = f"FY3{match.group('sat').upper()}_{match.group('date')}_{match.group('time')}_MERSI_GBAL_1000M"
            grouped.setdefault(scene_key, []).append(RemoteMember(
                name=entry["name"],
                source_uri=entry["url"],
                size=int(entry["size"]) if entry.get("size") is not None else None,
                sha256=str(entry.get("sha256") or "") or None,
            ))
        return [RemoteScene("FY3", key, tuple(members)) for key, members in sorted(grouped.items())]

    @staticmethod
    def _read_manifest(location: str) -> str:
        if location.startswith(("http://", "https://")):
            request = Request(location, headers=AcquisitionService._nsmc_headers())
            with urlopen(request, timeout=60) as response:
                return response.read().decode("utf-8")
        return Path(location).expanduser().read_text(encoding="utf-8")

    @staticmethod
    def _nsmc_headers() -> dict[str, str]:
        headers = {"User-Agent": "zhihuiqixiang-acquisition/1.0"}
        token = os.getenv("FY3_NSMC_TOKEN", "").strip()
        username = os.getenv("FY3_NSMC_USERNAME", "").strip()
        password = os.getenv("FY3_NSMC_PASSWORD", "")
        if token:
            headers["Authorization"] = f"Bearer {token}"
        elif username:
            encoded = base64.b64encode(f"{username}:{password}".encode("utf-8")).decode("ascii")
            headers["Authorization"] = f"Basic {encoded}"
        return headers

    def _discover_himawari(self) -> list[RemoteScene]:
        host = os.getenv("HIMAWARI_FTP_HOST", "ftp.ptree.jaxa.jp").strip()
        user = os.getenv("HIMAWARI_FTP_USER", "").strip()
        password = os.getenv("HIMAWARI_FTP_PASSWORD", "").strip()
        if not user or not password:
            raise ValueError("请配置 HIMAWARI_FTP_USER 和 HIMAWARI_FTP_PASSWORD")
        delay = max(0, int(os.getenv("HIMAWARI_LATEST_DELAY_MINUTES", "60")))
        lookback = max(1, int(os.getenv("SATELLITE_ACQUISITION_LOOKBACK_HOURS", "1")))
        latest = datetime.now(UTC) - timedelta(minutes=delay)
        latest = latest.replace(minute=latest.minute - latest.minute % 10, second=0, microsecond=0)
        root = os.getenv("HIMAWARI_FTP_ROOT", "/jma/hsd").strip().rstrip("/")
        ftp = ftplib.FTP()
        scenes: list[RemoteScene] = []
        try:
            ftp.connect(host, 21, timeout=60)
            ftp.login(user, password)
            try:
                ftp.voidcmd("TYPE I")
            except Exception:
                pass
            for offset in range(0, lookback * 6 + 1):
                slot = latest - timedelta(minutes=offset * 10)
                date, scene_time = slot.strftime("%Y%m%d"), slot.strftime("%H%M")
                dirs = [
                    posixpath.join(root, date[:6], date[6:8], scene_time[:2]),
                    posixpath.join(root, date[:6], date[6:8], scene_time),
                    posixpath.join(root, date, scene_time),
                ]
                remote_dir = None
                for candidate in dirs:
                    try:
                        ftp.cwd(candidate)
                        remote_dir = candidate
                        break
                    except ftplib.error_perm:
                        continue
                if not remote_dir:
                    continue
                members = []
                for remote_name in ftp.nlst():
                    name = Path(remote_name).name
                    match = HSD_RE.match(name)
                    if not match or match.group("date") != date or match.group("time") != scene_time:
                        continue
                    if match.group("band").upper() not in HIMAWARI_BANDS or match.group("region").upper() != "FLDK":
                        continue
                    try:
                        size = ftp.size(remote_name)
                    except Exception:
                        size = None
                    members.append(RemoteMember(
                        name=name,
                        source_uri=f"ftp://{host}{posixpath.join(remote_dir, name)}",
                        size=int(size) if size is not None else None,
                        ftp_dir=remote_dir,
                    ))
                if members:
                    match = HSD_RE.match(members[0].name)
                    assert match is not None
                    key = (
                        f"H{match.group('sat')}_{date}_{scene_time}_"
                        f"{match.group('region').upper()}_{match.group('resolution').upper()}"
                    )
                    scenes.append(RemoteScene("Himawari", key, tuple(members)))
            return scenes
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    @staticmethod
    def _descriptor(data_type: str, name: str) -> tuple[str, list[str]]:
        if data_type == "FY3":
            match = FY3_RE.match(name)
            if not match:
                raise ValueError(f"无法识别 FY-3 文件：{name}")
            return ("science" if match.group("role").upper() == "1000M_MS" else "geo", ["science", "geo"])
        match = HSD_RE.match(name)
        if not match:
            raise ValueError(f"无法识别 Himawari 文件：{name}")
        band = match.group("band").upper()
        segment = int(match.group("segment"))
        total = int(match.group("total"))
        return f"{band}:S{segment:02d}", [f"{band}:S{index:02d}" for index in range(1, total + 1)]

    def _ingest_scene(self, scene: RemoteScene) -> dict[str, Any]:
        expected = ["science", "geo"] if scene.data_type == "FY3" else []
        replacements: dict[str, list[str]] = {}
        if scene.data_type == "Himawari":
            for member in scene.members:
                role, roles = self._descriptor(scene.data_type, member.name)
                replacements[f"{role.split(':', 1)[0]}:"] = roles
            for band in HIMAWARI_BANDS:
                expected.extend(replacements.get(f"{band}:", [f"{band}:S{index:02d}" for index in range(1, 11)]))
        collection = create_or_get_satellite_collection(
            self.engine,
            None,
            scene.data_type,
            scene.scene_key,
            expected,
            role_replacements=replacements,
        )
        collection_uuid = str(collection["collection_uuid"])
        source_uuid = self._source_ids[scene.data_type]
        with self.engine.begin() as conn:
            conn.execute(
                update(satellite_collection)
                .where(satellite_collection.c.collection_uuid == collection_uuid)
                .values(source_uuid=source_uuid, remark="automatic acquisition")
            )
        downloaded = 0
        reused = 0
        for member in scene.members:
            role, _roles = self._descriptor(scene.data_type, member.name)
            if get_collection_member_task_by_role(self.engine, collection_uuid, role):
                reused += 1
                continue
            self._download_and_register(scene, collection_uuid, source_uuid, role, member)
            downloaded += 1
        return {
            "scene_key": scene.scene_key,
            "collection_uuid": collection_uuid,
            "member_count": len(scene.members),
            "downloaded": downloaded,
            "reused": reused,
        }

    def _download_and_register(
        self,
        scene: RemoteScene,
        collection_uuid: str,
        source_uuid: str,
        role: str,
        member: RemoteMember,
    ) -> None:
        file_uuid = str(uuid.uuid4())
        now = datetime.now(UTC)
        suffix = "".join(Path(member.name).suffixes[-2:]) or Path(member.name).suffix
        destination = (
            self.raw_root / "auto_download" / source_uuid / scene.data_type /
            now.strftime("%Y") / now.strftime("%m") / collection_uuid / f"{file_uuid}{suffix}"
        )
        destination.parent.mkdir(parents=True, exist_ok=True)
        part = destination.with_name(f"{destination.name}.part")
        try:
            digest, size = self._download_member_with_retries(member, part)
            if member.size is not None and size != member.size:
                raise IOError(f"下载大小不匹配：{member.name} 期望 {member.size}，实际 {size}")
            if member.sha256 and digest.lower() != member.sha256.lower():
                raise IOError(f"下载 SHA-256 不匹配：{member.name}")
            part.replace(destination)
            relative = destination.relative_to(self.raw_root).as_posix()
            values = {
                "file_uuid": file_uuid,
                "upload_session_id": None,
                "user_uuid": None,
                "source_uuid": source_uuid,
                "acquisition_type": "auto_download",
                "visibility": "public",
                "data_type": scene.data_type,
                "file_type": Path(member.name).suffix.lstrip(".").upper() or "RAW",
                "original_file_name": member.name,
                "stored_file_name": destination.name,
                "source_path": relative,
                "source_uri": member.source_uri,
                "file_size": size,
                "file_hash": digest,
                "remark": f"auto_download role={role}",
            }
            create_satellite_collection_upload_task(
                self.engine,
                values,
                collection_uuid=collection_uuid,
                role=role,
            )
        except Exception:
            part.unlink(missing_ok=True)
            destination.unlink(missing_ok=True)
            raise

    def _download_member_with_retries(self, member: RemoteMember, part: Path) -> tuple[str, int]:
        retries = max(1, min(10, int(os.getenv("SATELLITE_DOWNLOAD_RETRIES", "3"))))
        last_error: Exception | None = None
        for attempt in range(retries):
            try:
                return self._download_member(member, part)
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= retries:
                    break
                time.sleep(min(8, 2 ** attempt))
        assert last_error is not None
        raise last_error

    def _download_member(self, member: RemoteMember, part: Path) -> tuple[str, int]:
        if member.source_uri.startswith("ftp://"):
            return self._download_ftp(member, part)
        headers = self._nsmc_headers()
        resume_at = part.stat().st_size if part.exists() else 0
        if resume_at:
            headers["Range"] = f"bytes={resume_at}-"
        request = Request(member.source_uri, headers=headers)
        try:
            response = urlopen(request, timeout=120)
            append = resume_at > 0 and getattr(response, "status", None) == 206
        except Exception:
            if not resume_at:
                raise
            part.unlink(missing_ok=True)
            response = urlopen(Request(member.source_uri, headers={key: value for key, value in headers.items() if key != "Range"}), timeout=120)
            append = False
        with response, part.open("ab" if append else "wb") as output:
            while block := response.read(1024 * 1024):
                output.write(block)
        return self._hash_file(part)

    @staticmethod
    def _download_ftp(member: RemoteMember, part: Path) -> tuple[str, int]:
        parsed = urlparse(member.source_uri)
        user = os.getenv("HIMAWARI_FTP_USER", "")
        password = os.getenv("HIMAWARI_FTP_PASSWORD", "")
        ftp = ftplib.FTP()
        try:
            ftp.connect(parsed.hostname or "ftp.ptree.jaxa.jp", parsed.port or 21, timeout=60)
            ftp.login(user, password)
            resume_at = part.stat().st_size if part.exists() else 0
            if member.size is not None and resume_at > member.size:
                part.unlink()
                resume_at = 0
            try:
                with part.open("ab" if resume_at else "wb") as output:
                    ftp.retrbinary(f"RETR {parsed.path}", output.write, rest=resume_at or None)
            except Exception:
                if not resume_at:
                    raise
                with part.open("wb") as output:
                    ftp.retrbinary(f"RETR {parsed.path}", output.write)
            return AcquisitionService._hash_file(part)
        finally:
            try:
                ftp.quit()
            except Exception:
                ftp.close()

    @staticmethod
    def _hash_file(path: Path) -> tuple[str, int]:
        digest = hashlib.sha256()
        size = 0
        with path.open("rb") as source:
            while block := source.read(1024 * 1024):
                size += len(block)
                digest.update(block)
        if size <= 0:
            raise IOError("下载文件为空")
        return digest.hexdigest(), size
