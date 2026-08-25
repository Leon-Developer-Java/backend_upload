from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from io import BytesIO
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import unittest
import uuid

from fastapi import HTTPException, UploadFile
from sqlalchemy import func, select


_TEMPORARY_DIRECTORY = tempfile.TemporaryDirectory()
_ROOT = Path(_TEMPORARY_DIRECTORY.name)
os.environ["DATABASE_URL"] = f"sqlite:///{(_ROOT / 'weather.db').as_posix()}"
os.environ["RAW_STORAGE_ROOT"] = str(_ROOT / "raw")
os.environ["TMP_STORAGE_ROOT"] = str(_ROOT / "tmp")
os.environ["PRODUCT_DATA_ROOT"] = str(_ROOT / "products")
os.environ["JWT_SECRET"] = "era5-upload-test-secret"

import main as upload_main
from DB.schema import public_info, users


class Era5UploadFlowTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.user_uuid = str(uuid.uuid4())
        now = datetime.now(UTC).replace(tzinfo=None)
        with upload_main.engine.begin() as connection:
            connection.execute(
                users.insert().values(
                    uuid=cls.user_uuid,
                    username=f"era5-test-{uuid.uuid4().hex[:8]}",
                    password_hash="unused",
                    role=2,
                    status=1,
                    create_time=now,
                    update_time=now,
                    token_version=0,
                )
            )
        cls.request = SimpleNamespace(
            state=SimpleNamespace(user={"uuid": cls.user_uuid, "role": 2})
        )

    @classmethod
    def tearDownClass(cls) -> None:
        upload_main.engine.dispose()
        _TEMPORARY_DIRECTORY.cleanup()

    def _upload_chunk(self, file_id: str, content: bytes) -> None:
        result = asyncio.run(
            upload_main.upload_chunk(
                request=self.request,
                file_id=file_id,
                chunk_index=0,
                total_chunks=1,
                chunk=UploadFile(file=BytesIO(content), filename="chunk.bin"),
            )
        )
        self.assertEqual(result["code"], 0)

    def _complete(self, file_id: str, file_name: str = "sample.nc"):
        return upload_main.upload_complete(
            self.request,
            upload_main.CompletePayload(**{
                "file_id": file_id,
                "file_name": file_name,
                "total_chunks": 1,
                "data_type": "ERA5",
            }),
        )

    def test_empty_file_is_rejected_and_session_is_cleaned(self) -> None:
        file_id = f"empty-{uuid.uuid4()}"
        self._upload_chunk(file_id, b"")
        with self.assertRaises(HTTPException) as raised:
            self._complete(file_id, "empty.nc")

        self.assertEqual(raised.exception.status_code, 400)
        self.assertIn("上传文件为空", raised.exception.detail)
        self.assertFalse(upload_main.chunk_dir(self.user_uuid, file_id).exists())
        with upload_main.engine.connect() as connection:
            count = connection.execute(
                select(func.count()).select_from(public_info).where(
                    public_info.c.upload_session_id == upload_main.safe_file_id(file_id)
                )
            ).scalar_one()
        self.assertEqual(count, 0)

    def test_normal_and_same_user_duplicate_are_queued_once_each(self) -> None:
        content = b"synthetic-era5-content"
        first_id = f"normal-{uuid.uuid4()}"
        second_id = f"duplicate-{uuid.uuid4()}"

        self._upload_chunk(first_id, content)
        first = self._complete(first_id)
        first_data = first["data"]
        self.assertEqual(first_data["parse_status"], "pending")
        self.assertFalse(first_data["duplicate_content"])

        self._upload_chunk(second_id, content)
        second = self._complete(second_id, "copy.nc")
        second_data = second["data"]
        self.assertEqual(second_data["parse_status"], "pending")
        self.assertTrue(second_data["duplicate_content"])
        self.assertNotEqual(first_data["file_uuid"], second_data["file_uuid"])


if __name__ == "__main__":
    unittest.main()
