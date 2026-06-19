# 气象数据上传后端（backend_upload）

独立的 FastAPI 服务，负责前端「上传」页面的**分片上传 + 断点续传**。
上传完成后把文件落盘到现有展示后端的数据目录：

```
D:\weather_prediction_system\backend\data\{业务类型}\wait_process\
```

`wait_process` 目录不存在时自动创建。

## 目录结构

```text
backend_upload/
├─ main.py            # FastAPI 入口（端口 8003）
├─ requirements.txt
├─ README.md
└─ tmp_chunks/        # 分片临时目录（运行时自动生成，合并后清理）
```

## 启动

```powershell
cd D:\weather_prediction_system\backend_upload
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
uvicorn main:app --reload --host 127.0.0.1 --port 8003
```

接口文档：`http://127.0.0.1:8003/docs`

## 接口

统一响应壳：`{"code": 0, "data": ..., "message": "success"}`

### `GET /api/upload/status?file_id=...`
返回该文件已落盘的分片索引，供前端断点续传跳过。

```json
{ "code": 0, "data": { "uploaded": [0, 1, 2] }, "message": "success" }
```

### `POST /api/upload/chunk`（multipart/form-data）
字段：`file_id`、`chunk_index`、`total_chunks`、`chunk`（二进制分片）。
分片幂等写入临时目录，已存在则视为成功。

### `POST /api/upload/complete`（application/json）
字段：`file_id`、`file_name`、`total_chunks`、`data_type`。
校验分片齐全 → 按序合并落盘到 `wait_process/` → 清理临时分片。
缺片或非法 `data_type` 返回 400。

```json
{
  "code": 0,
  "data": {
    "file_name": "era5_t2m_20250610.nc",
    "directory": "D:/weather_prediction_system/backend/data/ERA5/wait_process/",
    "data_type": "ERA5"
  },
  "message": "success"
}
```

## 断点续传约定

- 文件唯一标识 `file_id` = 前端拼接的组合键 `文件名-大小-修改时间`，
  后端用其 MD5 作为临时目录名，避免非法路径字符。
- 续传：前端上传前先调 `/api/upload/status`，跳过已传分片，只补传缺失分片。

## 数据类型映射

| 前端显示名   | data 子目录 |
| ------------ | ----------- |
| ERA5         | ERA5        |
| GFS/ECMWF    | GFS         |
| CMA          | CMA         |
| 雷达         | Radar       |
| 葵花         | Himawari    |
| WRF          | WRF         |
