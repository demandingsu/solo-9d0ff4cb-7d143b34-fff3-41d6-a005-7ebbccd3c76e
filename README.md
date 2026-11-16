# 港航 AIS 靠港停留事件 API

岸基 AIS 点位 → 靠港停留事件，供排放核算使用。FastAPI + SQLite（WAL），
提供 Docker Compose 启动、CSV 上传、事件查询与游标变更流。

## 快速开始

```bash
docker compose up --build
```

* Swagger UI: <http://localhost:8000/docs>
* OpenAPI JSON: <http://localhost:8000/openapi.json>
* 健康检查: <http://localhost:8000/healthz>

演示迟到补点与撤回：在 `/docs` 中展开 **demo → POST /demo/run → Try it out →
Execute**。每次调用创建独立的 `DEMO-XXXXXXXX` 港区，不会污染业务数据。

演示变更领取 / 确认（带凭证与到期时间）：见下文「领取 / 确认工作流」，可直接
在 `/docs` 调 `POST /ports/{code}/changes/claims` 与
`POST /ports/{code}/changes/acknowledgements`，或运行
`python scripts/demo_claims.py http://localhost:8000 --demo-expiry`（含超时重领）。

双实例（共享同一 SQLite 卷，验证交错上传不丢更新）：

```bash
docker compose -f docker-compose.dual.yml up --build
# http://localhost:8001/docs 与 http://localhost:8002/docs
```

两个实例共享同一数据库文件，领取 / 确认同样在写锁上串行化：对同一消费者
并发领取只会得到同一个批次（见 `tests/test_claims.py` 与
`scripts/` 下的交错验证说明）。

## 业务规则

| 规则 | 实现 |
| --- | --- |
| 固定港区矩形范围 | `POST /ports`，同一 `code` 重复登记内容一致时幂等（200），不一致 409 |
| 多次上传 CSV | `POST /ports/{code}/uploads`，含船舶号、UTC 时间、经纬度 |
| 同船同刻同坐标去重 | 批次内与库内均去重，计入 `rows_duplicate` |
| 同船同刻异坐标整批拒收 | 返回 409 `conflicting_coordinates`，整批回滚，不留半批结果 |
| 按时间重排 | 每船按 `(event_time, id)` 排序后推导事件 |
| 相邻在港点 ≤ 10 分钟 | 间隔 > 600 秒断开 |
| 首末相距 ≥ 30 分钟 | 不足 1800 秒不形成事件 |
| 出港点断开 | 矩形外的点作为退出标记，关闭当前事件且不并入 |
| 迟到点改变事件 | 未变事件保留 `event_id`；被改变事件旧号 retract + 新号 upsert |
| 游标顺序读取变更 | `GET /ports/{code}/changes`，严格递增、可续读 |
| 按消费者领取 / 确认变更 | `POST /ports/{code}/changes/claims` 与 `.../acknowledgements`，批次带凭证 + 到期时间 |
| 上传原子生效 | 原始点、现役事件、变更记录同一 `BEGIN IMMEDIATE` 事务提交 |
| 两实例交错上传不丢更新 | SQLite WAL + `BEGIN IMMEDIATE` 写串行化 |
| 两实例交错领取不发不同批次 | 领取 / 确认均在 `BEGIN IMMEDIATE` 内完成，写锁保证同一消费者只有一个有效批次 |
| 失败不推进游标 | 事务回滚，`seq` 不前进，无半批结果；确认失败（凭证无效 / 过期 / 被取代）不改进度 |
| 重复上传不重发变更 | `Idempotency-Key`（缺省用内容 SHA-256）唯一约束，重放返回原批次 |

### 事件编号语义

事件以 `(port, vessel, start_time)` 为身份。迟到点只延长结束时间或增加点数时，
仍会 **retract 旧编号 + upsert 新编号**，保证消费方总能拿到完整快照而无需
patch 既有编号；起点不变、载荷不变的事件保留原编号。CSV 中落在港区矩形外的
点位也会入库（作为出港标记），`rows_outside_port` 会统计它们。

### CSV 格式

```csv
vessel,time,lat,lon
SHIP-001,2026-01-01T00:00:00Z,31.20,121.50
SHIP-001,2026-01-01T00:10:00Z,31.201,121.501
```

表头别名（大小写不敏感）：`vessel/船舶号/mmsi/ship_id`、`time/时间/timestamp`、
`lat/纬度`、`lon/lng/longitude/经度`。时间支持 ISO-8601（无时区按 UTC）或
epoch 秒/毫秒。坐标按 1e-6 度取整后比较。上传上限默认 10 MiB / 100,000 行，
可用 `AIS_MAX_UPLOAD_BYTES`、`AIS_MAX_ROWS` 调整。

## API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/ports` | 登记港区矩形范围（幂等） |
| GET | `/ports/{code}` | 读取港区定义 |
| POST | `/ports/{code}/uploads` | 上传 CSV 点位，整批原子生效 |
| GET | `/ports/{code}/events` | 现役事件，按 `event_id` 分页 |
| GET | `/ports/{code}/changes` | 变更流，`after` 游标 + `limit` |
| POST | `/ports/{code}/changes/claims` | 按消费者标识领取下一批变更（凭证 + 到期时间） |
| POST | `/ports/{code}/changes/acknowledgements` | 凭当前有效凭证确认批次，推进已确认游标 |
| GET | `/ports/{code}/changes/consumers/{id}` | 查看消费者进度与当前租约（重启接续用） |
| POST | `/demo/run` | 迟到补点 / retract 演示（`AIS_DEMO_ENABLED=1`） |
| GET | `/healthz` | 健康检查 |

上传响应示例：

```json
{
  "replayed": false,
  "batch_id": 7,
  "rows_received": 15,
  "rows_new": 15,
  "rows_duplicate": 0,
  "rows_outside_port": 0,
  "event_changes": {"upserted": 3, "retracted": 0},
  "change_cursor": {"from": 12, "to": 14}
}
```

变更响应示例（`GET /ports/CNSHA-PD/changes?after=14`）：

```json
{
  "port_id": 1,
  "next_cursor": 16,
  "has_more": false,
  "changes": [
    {"cursor": 15, "type": "retract", "event_id": 41, "vessel_id": "SHIP-001", "...": "..."},
    {"cursor": 16, "type": "upsert", "event_id": 42, "event": {"event_id": 42, "...": "..."}}
  ]
}
```

`seq` 在全库单调递增，同一港区的游标可能不连续；按 `seq > after` 读取即可。
`GET /events` 返回的 `cursor` 与变更流同源，可用于「先快照、后增量」。

## 领取 / 确认工作流（排放核算进程的可恢复消费）

排放核算端由多个采集进程读取变更，进程中断后要从上次确认处恢复。除裸游标
接口外，服务端提供按 **港区 + 消费者标识** 的领取 / 确认 API，每个消费者
（`consumer_id`，港区内唯一）独立维护已确认游标，新消费者从游标 0 开始。

### 语义保证

1. **限量按序领取**：`POST /changes/claims` 从已确认游标之后按 `seq` 升序
   返回最多 `limit` 条，批次带不透明 `claim_token` 与 `expires_at`。
2. **有效期内重复领取返回同一批**：租约未到期时再次领取，返回相同
   `lease_id` / `claim_token` / 原边界（`replayed=true`）；期间新上传的变更
   不会插入本批，留到下一批。
3. **只有当前未过期凭证才能确认**：
   `POST /changes/acknowledgements` 校验通过才把该消费者的已确认游标推进到
   批次右边界，确认与游标推进在同一事务提交。对同一有效凭证重复确认幂等。
4. **超时可重领，旧凭证不能越过新批次**：到期未确认的批次可重新领取，
   旧租约标记为 `superseded`；旧凭证晚到返回 409 `lease_superseded`，
   已过期返回 409 `lease_expired`，未知 / 跨港区凭证返回
   409 `invalid_claim_token`。这些失败都不推进游标。
5. **交错请求不跳过、不重发**：领取与确认都在 `BEGIN IMMEDIATE` 写事务内
   完成，两个服务实例并发为同一消费者领取时只会有一个事务创建租约，其余
   拿到同一批；部分唯一索引
   `uq_change_lease_one_active` 进一步保证一个消费者至多一个有效批次。
6. **重启可接续**：进度全部持久化在 SQLite。重启后用
   `GET .../changes/consumers/{id}` 查看已确认游标与当前租约；无有效租约时
   直接领取即可从已确认游标继续。

### 请求与响应示例

领取（`POST /ports/CNSHA-PD/changes/claims`）：

```json
{"consumer_id": "emission-accounting-01", "limit": 100}
```

```json
{
  "port_id": 1,
  "consumer_id": "emission-accounting-01",
  "lease": {
    "lease_id": 3,
    "claim_token": "lc_umYBWcn-PIuusQYlbktBVuhJOZdlhxjR",
    "cursor_from": 2,
    "cursor_to": 4,
    "status": "active",
    "expires_at": "2026-11-16T10:01:00.000+00:00",
    "replayed": false
  },
  "changes": [ {"cursor": 3, "type": "upsert", "...": "..."},
               {"cursor": 4, "type": "retract", "...": "..."} ],
  "change_count": 2,
  "next_cursor": 4,
  "has_more": false
}
```

确认（`POST /ports/CNSHA-PD/changes/acknowledgements`）：

```json
{"consumer_id": "emission-accounting-01",
 "claim_token": "lc_umYBWcn-PIuusQYlbktBVuhJOZdlhxjR"}
```

```json
{"port_id": 1, "consumer_id": "emission-accounting-01", "lease_id": 3,
 "confirmed_cursor": 4, "acknowledged_at": "2026-11-16T10:00:12+00:00"}
```

没有新变更时不创建租约，`lease` 为 `null`、`changes` 为空，稍后重新领取即可。
租约有效期由 `AIS_CHANGE_LEASE_TTL_SECONDS` 控制（默认 60 秒）。

## 本地开发与测试

```bash
python -m pip install -r requirements-dev.txt
python -m pytest -q
AIS_DEMO_ENABLED=1 uvicorn app.main:app --reload
```

测试覆盖：10 分钟/30 分钟边界、出港断开、批内与库内去重、异坐标整批回滚、
迟到点 retract/upsert 与编号保留、两事件合并、幂等重放、游标分页、港区隔离、
双实例（共享 DB 的并发线程与两个 FastAPI 实例）交错上传，以及领取 / 确认：
新消费者游标 0、限量按序、有效期重复领取同批且新变更留到下批、空领取不建租约、
无效 / 跨港区凭证拒绝、确认幂等、真实 TTL 超时重领、旧凭证晚到被
`lease_superseded` 拒绝且游标不动、两实例并发领取收敛为同一批次、
交错上传 / 领取 / 确认不丢不重、重启后接续。`docker build`
未能在本环境执行（无 Docker 守护进程），Compose 文件仅做了 YAML 解析校验；
双实例语义改用两个真实 uvicorn 进程共享同一 SQLite 文件实测。

## 配置

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `AIS_DB_PATH` | `./data/ais.sqlite3` | SQLite 路径（容器内 `/data/ais.sqlite3`） |
| `AIS_DEMO_ENABLED` | `0`（镜像内为 `1`） | 是否注册 `/demo/run` |
| `AIS_MAX_UPLOAD_BYTES` | `10485760` | 单次上传字节上限 |
| `AIS_MAX_ROWS` | `100000` | 单次上传唯一行数上限 |
| `AIS_SQLITE_BUSY_TIMEOUT_MS` | `30000` | 写锁等待时间，超时返回 503 |
| `AIS_CHANGE_LEASE_TTL_SECONDS` | `60` | 领取批次（租约）有效期，到期未确认可重领 |

## 已知限制

* 变更流只保留写入的记录、不提供保留期清理；长期运行需自行归档。
* 领取租约与消费者进度同样永久保留（`change_leases` / `change_consumers`），
  暂无清理 / 注销接口；消费者标识由调用方保证稳定与唯一。
* 事件推导按船全量重算该港区点位；单船超大数据量时应改为增量窗口。
* SQLite 共享卷适用于单机多实例；跨主机需换成服务端数据库。
* 不做鉴权；生产部署应置于反向代理/网关之后。
