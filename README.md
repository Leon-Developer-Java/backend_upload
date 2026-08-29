# 上传、目录与自动采集服务（backend_upload，端口 8003）

负责断点续传、上传入队、用户任务查询、卫星集合、展示资源目录以及 FY-3/Himawari 自动采集。气象解析不在 HTTP 请求内执行，而是由 `backend` 的 Adapter Worker 从共享数据库异步领取。

## 运行环境与工作区要求

- Python 3.11 或 3.12
- 工作区根目录必须同时包含 `DB/`、`backend/` 和 `storage/`
- `DB` 是共享 Python 包；`catalog_reconcile.py` 还会复用 `backend.services.asset_catalog`

因此不要只复制或克隆 `backend_upload/` 后单独运行，否则会出现 `DB` 或 `backend` 模块缺失。

安装：

```powershell
cd D:\weather_prediction_system\backend_upload
python -m venv .venv
.\.venv\Scripts\Activate.ps1
python -m pip install --upgrade pip
python -m pip install -r requirements.txt
Copy-Item .env.example .env
```

启动：

```powershell
.\.venv\Scripts\python.exe main.py
```

- API：`http://127.0.0.1:8003`
- OpenAPI：`http://127.0.0.1:8003/docs`
- 健康检查：`http://127.0.0.1:8003/api/health`

也可从本目录运行：

```powershell
.\.venv\Scripts\python.exe -m uvicorn main:app --host 127.0.0.1 --port 8003
```

## 服务边界

```text
前端
  -> 分片上传到 backend_upload
  -> backend_upload 合并、计算 SHA-256、写 raw 文件和 public_info
  -> Adapter Worker 领取 pending/ready 任务
  -> backend Adapter 生成 meta/WebP 并写明细表
  -> backend_upload 目录接口选择已完成资源
  -> backend:8002 提供展示响应与 /data 文件
```

标准单文件支持 ERA5、GFS、ECMWF、CMA、Radar、WRF。FY-3 和 Himawari 使用 `satellite_collection`：集合完整前成员处于 `waiting_collection`，完整后由一个 leader 任务触发整组解析。

## 共享数据库

默认数据库是：

```text
D:\weather_prediction_system\DB\weather.db
```

`DB/weather.db` 是本机运行数据并被 Git 忽略，不应上传。服务导入时调用 `DB.migrate.init_database()`：首次启动会创建数据库，旧数据库会自动升级。当前卫星集合结构同时支持：

- 用户上传：填写 `user_uuid`，`source_uuid` 为空；
- 自动采集：填写 `source_uuid`，`user_uuid` 为空；
- 同一来源、数据类型、场景和 generation 的自动集合唯一。

迁移只调整共享表结构，不移动 `storage/`、`backend/data/` 或已上传文件。`backend_auth`、本服务和 Adapter Worker 必须使用相同的 `DATABASE_URL`。

## 存储目录

```text
D:\weather_prediction_system\storage\
├─ raw\
│  ├─ user_upload\{user_uuid}\{data_type}\{yyyy}\{mm}\...
│  └─ auto_download\{source_uuid}\{data_type}\{yyyy}\{mm}\...
└─ tmp\
   ├─ uploads\{user_uuid}\{upload_session_id}\...
   └─ adapter\...

D:\weather_prediction_system\backend\data\  # 可展示 meta/WebP
```

`RAW_STORAGE_ROOT` 不得通过静态目录公开。数据库保存相对于存储根目录的路径；API 不向前端返回 raw 的 Windows 绝对路径。

## 环境变量

基础配置：

| 变量 | 默认值/用途 |
| --- | --- |
| `DATABASE_URL` | 工作区 `DB/weather.db` |
| `JWT_SECRET` | 必须与 `backend_auth` 及其他业务服务一致 |
| `RAW_STORAGE_ROOT` | 工作区 `storage/raw` |
| `TMP_STORAGE_ROOT` | 工作区 `storage/tmp` |
| `PRODUCT_DATA_ROOT` | 工作区 `backend/data` |
| `CORS_ORIGINS` | 允许访问 8003 的前端来源 |
| `MAX_UPLOAD_CHUNK_SIZE` | 单分片上限，默认 8 MiB |
| `MAX_UPLOAD_FILE_SIZE` | 单文件上限，默认 20 GiB |
| `MAX_UPLOAD_CHUNKS` | 最大分片数，默认 10000 |

自动采集默认关闭：

| 变量 | 说明 |
| --- | --- |
| `SATELLITE_ACQUISITION_ENABLED=1` | 启动后台轮询 |
| `SATELLITE_ACQUISITION_INTERVAL_SECONDS` | 轮询间隔，最小 60 秒，默认 600 |
| `SATELLITE_ACQUISITION_LOOKBACK_HOURS` | Himawari 回看窗口，默认 1 小时 |
| `SATELLITE_DOWNLOAD_RETRIES` | 下载重试次数，默认 3 |
| `FY3_NSMC_ENABLED=1` | 启用 FY-3 来源 |
| `FY3_NSMC_MANIFEST` | NSMC 订单 URL 文本清单或 JSON `files` 数组 |
| `FY3_NSMC_TOKEN` | 可选 Bearer Token |
| `FY3_NSMC_USERNAME/PASSWORD` | 可选 Basic Auth |
| `HIMAWARI_ACQUISITION_ENABLED=1` | 启用 Himawari 来源 |
| `HIMAWARI_FTP_HOST` | 默认 `ftp.ptree.jaxa.jp` |
| `HIMAWARI_FTP_ROOT` | 默认 `/jma/hsd` |
| `HIMAWARI_FTP_USER/PASSWORD` | JAXA FTP 凭据 |
| `HIMAWARI_LATEST_DELAY_MINUTES` | 最新可用时次延迟，默认 60 分钟 |

凭据只写入部署环境或未提交的 `.env`，不得存入数据库 `config_json` 或仓库。

## 主要接口

上传和任务：

```text
GET  /api/upload/status
POST /api/upload/chunk
POST /api/upload/complete
GET  /api/upload/tasks
GET  /api/upload/tasks/{file_uuid}
POST /api/upload/tasks/{file_uuid}/retry
```

卫星集合：

```text
POST /api/upload/collections/prepare
GET  /api/upload/collections
GET  /api/upload/collections/{collection_uuid}
POST /api/upload/collections/{collection_uuid}/retry
```

目录与临近预报输入：

```text
GET  /api/catalog/resources
GET  /api/catalog/resources/{file_uuid}
POST /api/catalog/series
POST /api/catalog/reconcile
GET  /api/catalog/nowcast/latest
GET  /api/catalog/nowcast/resources/{file_uuid}/input
```

自动采集：

```text
GET  /api/acquisition/{FY3|Himawari}/status
GET  /api/acquisition/{FY3|Himawari}/scenes
POST /api/acquisition/{FY3|Himawari}/sync
```

所有业务接口要求 JWT。目录和状态读取最低 role 1；上传、重试、目录重建和手动采集最低 role 2。服务每次请求都会查询共享 `users`，禁用、降级或 `token_version` 变化立即生效。

## 上传规则

- 前端 `file_id` 只用于断点续传会话；数据库和磁盘业务主键使用服务端 `file_uuid`。
- `/complete` 对同一用户和上传会话幂等。
- 同一用户、同一数据类型、相同 SHA-256 会复用已有任务，不保存第二份 raw。
- 上传完成只表示 `ingest_status=success`；可展示状态以 `parse_status=success` 为准。
- 失败任务通过 retry 接口重新入队，不要求用户重新上传原文件。
- FY-3 集合需要同场景 science + geo；Himawari 需要配置波段的全部分段，集合成员不能单独重试。

## 验证

```powershell
.\.venv\Scripts\python.exe -m pip check
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
.\.venv\Scripts\python.exe -m py_compile main.py acquisition.py auth.py catalog_reconcile.py
```

测试启动时会使用临时 SQLite 验证旧卫星集合结构可自动迁移；不会修改正式数据目录。
