"""SQLite 数据访问层。

表：records / baselines / previews，以及字段级来源链：
- provenance_chains：一条记录的一个世代（同 ID 删除后重建会新开一条链）；
- provenance_events：每次实际改值的一条事件，逐字段保存改前/改后值与前一来源。

所有写操作和合并确认共用 LOCK；每次写事务使用 BEGIN IMMEDIATE，先取得写锁再读
改前值。SQLite 的 WAL + busy_timeout 负责跨连接/跨进程串行化。查询使用显式只读
事务，保证“当前值”和“来源”来自同一快照。确认预览时，过期核对、记录变更、
来源事件和预览状态全部在同一个事务内完成。
"""

import json
import os
import sqlite3
import threading
from contextlib import contextmanager
from datetime import datetime

DB_PATH = os.environ.get("DB_PATH", os.path.join(os.path.dirname(__file__), "..", "survey.db"))

# 所有写操作与确认合并共用同一把锁，保证"检查-应用"之间不被插入
LOCK = threading.RLock()

FIELDS = ("quadrat", "species", "count")

SCHEMA = (
    """
    CREATE TABLE IF NOT EXISTS records (
        id      TEXT PRIMARY KEY,
        quadrat TEXT NOT NULL,
        species TEXT NOT NULL,
        count   INTEGER NOT NULL CHECK (count >= 0)
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS baselines (
        id         INTEGER PRIMARY KEY AUTOINCREMENT,
        name       TEXT NOT NULL,
        created_at TEXT NOT NULL,
        data       TEXT NOT NULL          -- JSON: {id: record}
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS previews (
        token       TEXT PRIMARY KEY,
        baseline_id INTEGER NOT NULL,
        created_at  TEXT NOT NULL,
        status      TEXT NOT NULL,        -- pending/applied/discarded
        message     TEXT NOT NULL DEFAULT '',
        revision    TEXT NOT NULL,        -- JSON: {id: record}
        snapshot    TEXT NOT NULL,        -- JSON: 预览时涉及记录的当前值 {id: record|null}
        plan        TEXT NOT NULL,        -- JSON: compute_plan 的结果
        resolutions TEXT NOT NULL DEFAULT '{}'  -- JSON: 确认时的人工冲突选择
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS provenance_chains (
        chain_id   INTEGER PRIMARY KEY AUTOINCREMENT,
        record_id  TEXT NOT NULL,
        started_at TEXT NOT NULL,
        start_kind TEXT NOT NULL,         -- trace_start/manual/merge
        deleted_at TEXT                    -- NULL 表示该世代当前仍在库
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS provenance_events (
        event_id       INTEGER PRIMARY KEY AUTOINCREMENT,
        chain_id       INTEGER NOT NULL REFERENCES provenance_chains(chain_id),
        record_id      TEXT NOT NULL,
        created_at     TEXT NOT NULL,
        kind           TEXT NOT NULL,     -- trace_start/manual/merge
        operation      TEXT NOT NULL,     -- create/update/delete
        preview_token  TEXT,              -- 离线修订事件关联的预览号
        baseline_id    INTEGER,           -- 离线修订事件关联的基线
        revision_id    TEXT,              -- 修订文件中的记录 ID
        changed_fields TEXT NOT NULL DEFAULT '[]',
        before_values  TEXT NOT NULL DEFAULT '{}',
        after_values   TEXT NOT NULL DEFAULT '{}',
        prev_sources   TEXT NOT NULL DEFAULT '{}',  -- {field: 改前值来源 event_id}
        field_origins  TEXT NOT NULL DEFAULT '{}',  -- {field: {origin, revision_value, resolution}}
        resolutions    TEXT NOT NULL DEFAULT '{}',  -- 冲突裁决 {field: rev|cur}
        detail         TEXT NOT NULL DEFAULT ''
    )
    """,
)

INDEXES = (
    "CREATE INDEX IF NOT EXISTS idx_chains_record ON provenance_chains(record_id, chain_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_chain ON provenance_events(chain_id, event_id)",
    "CREATE INDEX IF NOT EXISTS idx_events_record ON provenance_events(record_id, event_id)",
)


def connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False, timeout=30)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    conn.execute("PRAGMA busy_timeout = 30000")
    return conn


@contextmanager
def transaction():
    """写事务：BEGIN IMMEDIATE 立即取得写锁，异常整体回滚。"""
    with LOCK:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            try:
                yield conn
            except Exception:
                conn.rollback()
                raise
            else:
                conn.commit()
        finally:
            conn.close()


@contextmanager
def read_snapshot():
    """只读事务：短事务，保证当前值与来源事件来自同一快照。"""
    conn = connect()
    try:
        conn.execute("BEGIN")
        try:
            yield conn
        except Exception:
            conn.rollback()
            raise
        else:
            conn.commit()
    finally:
        conn.close()


@contextmanager
def read():
    """普通只读连接，用完即关。"""
    conn = connect()
    try:
        yield conn
    finally:
        conn.close()


def now() -> str:
    return datetime.now().isoformat(timespec="seconds")


def init_db(seed_csv: str | None = None):
    os.makedirs(os.path.dirname(os.path.abspath(DB_PATH)), exist_ok=True)
    with connect() as conn:
        conn.execute("PRAGMA journal_mode = WAL")
    with transaction() as conn:
        for stmt in SCHEMA:
            conn.execute(stmt)
        for stmt in INDEXES:
            conn.execute(stmt)
        # 兼容旧库：补齐后来新增的列（重建前已存在的库同样可用）
        _ensure_columns(
            conn,
            "previews",
            {"resolutions": "TEXT NOT NULL DEFAULT '{}'"},
        )
        fresh = (
            conn.execute("SELECT COUNT(*) AS n FROM records").fetchone()["n"] == 0
            and conn.execute("SELECT COUNT(*) AS n FROM provenance_chains").fetchone()["n"] == 0
        )
        if fresh and seed_csv and os.path.exists(seed_csv):
            from .csvio import parse_csv

            with open(seed_csv, encoding="utf-8") as f:
                seed = parse_csv(f.read())
            for rid, r in seed.items():
                conn.execute(
                    "INSERT OR IGNORE INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
                    (rid, r["quadrat"], r["species"], r["count"]),
                )
                _start_chain(
                    conn, rid, "trace_start", r,
                    origin="trace_start",
                    detail="启用来源追溯时已载入的初始数据",
                )
        # 启用来源追溯：此前已有的数据统一标为追溯起点，不推测更早的历史
        untracked = conn.execute(
            "SELECT * FROM records WHERE id NOT IN"
            " (SELECT record_id FROM provenance_chains WHERE deleted_at IS NULL) ORDER BY id"
        ).fetchall()
        for row in untracked:
            values = _row_values(row)
            _start_chain(
                conn, row["id"], "trace_start", values,
                origin="trace_start",
                detail="启用来源追溯时已有数据（历史不可考，不推测）",
            )


def _ensure_columns(conn, table: str, columns: dict):
    existing = {r["name"] for r in conn.execute(f"PRAGMA table_info({table})").fetchall()}
    for name, ddl in columns.items():
        if name not in existing:
            conn.execute(f"ALTER TABLE {table} ADD COLUMN {name} {ddl}")


# ---------- 来源链内部工具 ----------


def _row_values(row) -> dict:
    return {"quadrat": row["quadrat"], "species": row["species"], "count": row["count"]}


def _live_chain(conn, rid: str):
    return conn.execute(
        "SELECT * FROM provenance_chains WHERE record_id=? AND deleted_at IS NULL"
        " ORDER BY chain_id DESC LIMIT 1",
        (rid,),
    ).fetchone()


def _write_event(
    conn,
    *,
    chain_id: int,
    rid: str,
    kind: str,
    operation: str,
    before: dict | None = None,
    after: dict | None = None,
    prev_sources: dict | None = None,
    origins: dict | None = None,
    preview_token: str | None = None,
    baseline_id: int | None = None,
    revision_id: str | None = None,
    resolutions: dict | None = None,
    detail: str = "",
) -> int:
    """写一条来源事件；只记录实际发生变化的字段。"""
    before = before or {}
    after = after or {}
    prev_sources = prev_sources or {}
    origins = origins or {}

    if operation == "create":
        changed = [f for f in FIELDS if f in after]
    elif operation == "delete":
        changed = [f for f in FIELDS if f in before]
    else:
        changed = [f for f in FIELDS if before.get(f) != after.get(f)]

    before_values = {f: before[f] for f in changed if f in before}
    after_values = {f: after[f] for f in changed if f in after}
    field_origins = {}
    for f in changed:
        o = dict(origins.get(f) or {})
        o.setdefault("origin", kind)
        if "revision_value" not in o:
            o["revision_value"] = None
        if "resolution" not in o:
            o["resolution"] = None
        field_origins[f] = o
    # 只保存确实有前一来源的字段；新增/追溯起点没有前一来源
    field_prev = {f: prev_sources[f] for f in changed if prev_sources.get(f)}

    cur = conn.execute(
        "INSERT INTO provenance_events (chain_id, record_id, created_at, kind, operation,"
        " preview_token, baseline_id, revision_id, changed_fields, before_values, after_values,"
        " prev_sources, field_origins, resolutions, detail)"
        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
        (
            chain_id, rid, now(), kind, operation, preview_token, baseline_id, revision_id,
            json.dumps(changed, ensure_ascii=False),
            json.dumps(before_values, ensure_ascii=False),
            json.dumps(after_values, ensure_ascii=False),
            json.dumps(field_prev, ensure_ascii=False),
            json.dumps(field_origins, ensure_ascii=False),
            json.dumps(resolutions or {}, ensure_ascii=False),
            detail,
        ),
    )
    event_id = cur.lastrowid
    if operation == "delete":
        conn.execute(
            "UPDATE provenance_chains SET deleted_at=? WHERE chain_id=?", (now(), chain_id)
        )
    return event_id


def _start_chain(conn, rid: str, kind: str, values: dict, origin: str, detail: str) -> int:
    """开启新世代链，并写入 create 事件（含全部字段值与来源）。"""
    if _live_chain(conn, rid) is not None:
        raise ValueError(f"记录 {rid} 已有在库来源链，不能重复建链")
    cur = conn.execute(
        "INSERT INTO provenance_chains (record_id, started_at, start_kind) VALUES (?,?,?)",
        (rid, now(), kind),
    )
    chain_id = cur.lastrowid
    _write_event(
        conn, chain_id=chain_id, rid=rid, kind=kind, operation="create",
        before=None, after=values,
        origins={f: {"origin": origin} for f in FIELDS},
        detail=detail,
    )
    return chain_id


def _field_source_events(conn, chain_id: int) -> dict:
    """当前链内每个字段最近一次被写入的事件号（即字段当前来源）。"""
    events = conn.execute(
        "SELECT event_id, after_values FROM provenance_events"
        " WHERE chain_id=? AND operation != 'delete' ORDER BY event_id DESC",
        (chain_id,),
    ).fetchall()
    out = {}
    for ev in events:
        values = json.loads(ev["after_values"] or "{}")
        for f in FIELDS:
            if f in values and f not in out:
                out[f] = ev["event_id"]
    return out


# ---------- 当前记录 ----------


def get_records() -> dict:
    with read() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
    return {row["id"]: _row_values(row) for row in rows}


def get_records_by_ids(ids) -> dict:
    """返回 {id: record|None}，不存在的 ID 值为 None。"""
    out = {}
    with read_snapshot() as conn:
        for rid in ids:
            row = conn.execute("SELECT * FROM records WHERE id = ?", (rid,)).fetchone()
            out[rid] = _row_values(row) if row else None
    return out


def add_record(rid, quadrat, species, count):
    with transaction() as conn:
        if conn.execute("SELECT 1 FROM records WHERE id=?", (rid,)).fetchone():
            raise ValueError(f"记录 {rid} 已存在")
        conn.execute(
            "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
            (rid, quadrat, species, count),
        )
        _start_chain(
            conn, rid, "manual", {"quadrat": quadrat, "species": species, "count": count},
            origin="manual", detail="人工新增记录",
        )


def update_record(rid, quadrat, species, count) -> str:
    """人工修改。返回 updated / unchanged / missing。"""
    with transaction() as conn:
        row = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
        if not row:
            return "missing"
        before = _row_values(row)
        after = {"quadrat": quadrat, "species": species, "count": count}
        if before == after:
            return "unchanged"
        chain = _live_chain(conn, rid)
        if chain is None:
            # 防御：已有记录但缺链时补一个追溯起点，再衔接本次修改
            _start_chain(
                conn, rid, "trace_start", before,
                origin="trace_start", detail="发现记录缺少来源链，补记追溯起点",
            )
            chain = _live_chain(conn, rid)
        prev = _field_source_events(conn, chain["chain_id"])
        conn.execute(
            "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
            (quadrat, species, count, rid),
        )
        _write_event(
            conn, chain_id=chain["chain_id"], rid=rid, kind="manual", operation="update",
            before=before, after=after, prev_sources=prev,
            origins={f: {"origin": "manual"} for f in FIELDS},
            detail="人工修改记录",
        )
        return "updated"


def delete_record(rid) -> bool:
    with transaction() as conn:
        row = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
        if not row:
            return False
        before = _row_values(row)
        chain = _live_chain(conn, rid)
        if chain is None:
            _start_chain(
                conn, rid, "trace_start", before,
                origin="trace_start", detail="发现记录缺少来源链，补记追溯起点",
            )
            chain = _live_chain(conn, rid)
        prev = _field_source_events(conn, chain["chain_id"])
        conn.execute("DELETE FROM records WHERE id=?", (rid,))
        _write_event(
            conn, chain_id=chain["chain_id"], rid=rid, kind="manual", operation="delete",
            before=before, after=None, prev_sources=prev,
            origins={f: {"origin": "manual"} for f in FIELDS},
            detail="人工删除记录（旧链保留，同 ID 重建将新开链）",
        )
        return True


# ---------- 基线 ----------


def save_baseline(name: str, data: dict) -> int:
    with transaction() as conn:
        cur = conn.execute(
            "INSERT INTO baselines (name, created_at, data) VALUES (?,?,?)",
            (name, now(), json.dumps(data, ensure_ascii=False)),
        )
        return cur.lastrowid


def list_baselines() -> list:
    with read() as conn:
        rows = conn.execute("SELECT id, name, created_at, data FROM baselines ORDER BY id DESC").fetchall()
    return [
        {"id": r["id"], "name": r["name"], "created_at": r["created_at"], "size": len(json.loads(r["data"]))}
        for r in rows
    ]


def get_baseline(bid: int):
    with read() as conn:
        row = conn.execute("SELECT * FROM baselines WHERE id=?", (bid,)).fetchone()
    if not row:
        return None
    return {
        "id": row["id"],
        "name": row["name"],
        "created_at": row["created_at"],
        "data": json.loads(row["data"]),
    }


# ---------- 合并预览 ----------


def save_preview(token, baseline_id, revision, snapshot, plan):
    with transaction() as conn:
        conn.execute(
            "INSERT INTO previews (token, baseline_id, created_at, status, message, revision, snapshot, plan)"
            " VALUES (?,?,?,?,?,?,?,?)",
            (
                token,
                baseline_id,
                now(),
                "pending",
                "",
                json.dumps(revision, ensure_ascii=False),
                json.dumps(snapshot, ensure_ascii=False),
                json.dumps(plan, ensure_ascii=False),
            ),
        )


def get_preview(token):
    with read_snapshot() as conn:
        row = conn.execute("SELECT * FROM previews WHERE token=?", (token,)).fetchone()
    if not row:
        return None
    return {
        "token": row["token"],
        "baseline_id": row["baseline_id"],
        "created_at": row["created_at"],
        "status": row["status"],
        "message": row["message"],
        "revision": json.loads(row["revision"]),
        "snapshot": json.loads(row["snapshot"]),
        "plan": json.loads(row["plan"]),
    }


def list_previews() -> list:
    with read() as conn:
        rows = conn.execute(
            "SELECT token, baseline_id, created_at, status, message FROM previews ORDER BY created_at DESC, token DESC"
        ).fetchall()
    return [dict(r) for r in rows]


def update_preview_plan(token, snapshot, plan, message):
    """过期预览被重新比较后，用新计划与新快照替换旧内容（不写来源）。"""
    with transaction() as conn:
        conn.execute(
            "UPDATE previews SET snapshot=?, plan=?, message=?, status='pending', created_at=? WHERE token=?",
            (json.dumps(snapshot, ensure_ascii=False), json.dumps(plan, ensure_ascii=False), message, now(), token),
        )


def set_preview_status(token, status, message=""):
    with transaction() as conn:
        conn.execute("UPDATE previews SET status=?, message=? WHERE token=?", (status, message, token))


def apply_preview(token, baseline_id, snapshot, plan, actions, resolutions=None) -> str:
    """单事务：过期核对 + 记录变更 + 来源事件 + 预览状态。

    返回 applied / stale / processed。stale 表示拟触及记录在预览后已变化，事务
    未做任何改动；processed 表示预览已被确认或放弃。
    """
    with LOCK:
        conn = connect()
        try:
            conn.execute("BEGIN IMMEDIATE")
            pv = conn.execute("SELECT * FROM previews WHERE token=?", (token,)).fetchone()
            if not pv or pv["status"] != "pending":
                conn.rollback()
                return "processed"

            # 过期检测：用事务内实际读到的提交前值核对，而不是预览时的缓存
            for rid in plan.get("involved", []):
                row = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
                current = _row_values(row) if row else None
                if current != snapshot.get(rid):
                    conn.rollback()
                    return "stale"

            for a in actions:
                _apply_action(conn, a, token, baseline_id)
            conn.execute(
                "UPDATE previews SET status='applied', message=?, resolutions=? WHERE token=?",
                (
                    f"已应用 {len(actions)} 项离线改动",
                    json.dumps(resolutions or {}, ensure_ascii=False),
                    token,
                ),
            )
            conn.commit()
            return "applied"
        except Exception:
            conn.rollback()
            raise
        finally:
            conn.close()


def _apply_action(conn, a: dict, token: str, baseline_id: int):
    op = a["action"]
    rid = a["id"]
    row = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
    before = _row_values(row) if row else None

    if op == "insert":
        if row is not None:
            raise ValueError(f"记录 {rid} 已存在，不能重复新增")
        after = dict(a["record"])
        conn.execute(
            "INSERT INTO records (id, quadrat, species, count) VALUES (?,?,?,?)",
            (rid, after["quadrat"], after["species"], after["count"]),
        )
        _start_chain(
            conn, rid, "merge", after,
            origin="revision",
            detail="离线修订新增记录" if not a.get("restored") else "离线修订恢复已删除记录",
        )
        # 给 create 事件补上基线/预览号/修订原值/裁决
        conn.execute(
            "UPDATE provenance_events SET preview_token=?, baseline_id=?, revision_id=?,"
            " field_origins=?, resolutions=?"
            " WHERE chain_id=(SELECT chain_id FROM provenance_chains WHERE record_id=?"
            " AND deleted_at IS NULL) AND operation='create'",
            (
                token, baseline_id, a.get("revision_id"),
                json.dumps(_origins_for(a, before, after), ensure_ascii=False),
                json.dumps(a.get("resolutions") or {}, ensure_ascii=False),
                rid,
            ),
        )

    elif op == "update":
        if row is None:
            raise ValueError(f"记录 {rid} 已不存在，拒绝应用")
        after = dict(a["to"])
        chain = _live_chain(conn, rid)
        if chain is None:
            _start_chain(
                conn, rid, "trace_start", before,
                origin="trace_start", detail="发现记录缺少来源链，补记追溯起点",
            )
            chain = _live_chain(conn, rid)
        prev = _field_source_events(conn, chain["chain_id"])
        conn.execute(
            "UPDATE records SET quadrat=?, species=?, count=? WHERE id=?",
            (after["quadrat"], after["species"], after["count"], rid),
        )
        _write_event(
            conn, chain_id=chain["chain_id"], rid=rid, kind="merge", operation="update",
            before=before, after=after, prev_sources=prev,
            origins=_origins_for(a, before, after),
            preview_token=token, baseline_id=baseline_id,
            revision_id=a.get("revision_id"),
            resolutions=a.get("resolutions") or {},
            detail="离线修订合并（未变字段沿用原来源）",
        )

    elif op == "delete":
        if row is None:
            raise ValueError(f"记录 {rid} 已不存在，拒绝应用")
        chain = _live_chain(conn, rid)
        if chain is None:
            _start_chain(
                conn, rid, "trace_start", before,
                origin="trace_start", detail="发现记录缺少来源链，补记追溯起点",
            )
            chain = _live_chain(conn, rid)
        prev = _field_source_events(conn, chain["chain_id"])
        conn.execute("DELETE FROM records WHERE id=?", (rid,))
        _write_event(
            conn, chain_id=chain["chain_id"], rid=rid, kind="merge", operation="delete",
            before=before, after=None, prev_sources=prev,
            origins=_origins_for(a, before, None),
            preview_token=token, baseline_id=baseline_id,
            revision_id=a.get("revision_id"),
            resolutions=a.get("resolutions") or {},
            detail="离线修订删除（旧链保留，同 ID 重建将新开链）",
        )
    else:
        raise ValueError(f"未知动作：{op}")


def _origins_for(a: dict, before: dict | None, after: dict | None) -> dict:
    """把动作中给出的字段来源标记补齐；来源信息缺失时按修订处理。

    before 仅用于区分“新增/恢复”与“更新”，实际字段裁剪由 _write_event 完成。
    """
    origins = {f: dict(a.get("field_origins", {}).get(f) or {}) for f in FIELDS}
    for f, o in origins.items():
        if after is None:
            o.setdefault("origin", "revision_deletion")
        else:
            o.setdefault("origin", "revision")
        if "revision_value" not in o:
            o["revision_value"] = (after or {}).get(f)
        o.setdefault("resolution", None)
    return origins


# ---------- 来源查询 ----------


def _event_dict(row) -> dict:
    d = dict(row)
    for key, default in (
        ("changed_fields", []),
        ("before_values", {}),
        ("after_values", {}),
        ("prev_sources", {}),
        ("field_origins", {}),
        ("resolutions", {}),
    ):
        raw = d.get(key)
        d[key] = json.loads(raw) if raw else default
    return d


def field_provenance(rid: str) -> dict:
    """当前记录各字段来源；当前值与来源在同一次只读快照中取得。"""
    with read_snapshot() as conn:
        row = conn.execute("SELECT * FROM records WHERE id=?", (rid,)).fetchone()
        if not row:
            return {}
        chain = _live_chain(conn, rid)
        current = _row_values(row)
        if chain is None:
            return {f: {"value": current[f], "unknown": True} for f in FIELDS}
        events = conn.execute(
            "SELECT * FROM provenance_events WHERE chain_id=? ORDER BY event_id",
            (chain["chain_id"],),
        ).fetchall()
        events = [_event_dict(e) for e in events]
    out = {}
    for f in FIELDS:
        found = None
        for ev in reversed(events):
            if f in ev["after_values"]:
                found = ev
                break
        if found is None:
            out[f] = {"value": current[f], "unknown": True, "chain_id": chain["chain_id"]}
        else:
            out[f] = {
                "value": current[f],
                "unknown": False,
                "event_id": found["event_id"],
                "kind": found["kind"],
                "operation": found["operation"],
                "created_at": found["created_at"],
                "preview_token": found["preview_token"],
                "baseline_id": found["baseline_id"],
                "revision_id": found["revision_id"],
                "origin": found["field_origins"].get(f, {}).get("origin", found["kind"]),
                "revision_value": found["field_origins"].get(f, {}).get("revision_value"),
                "resolution": found["field_origins"].get(f, {}).get("resolution"),
                "prev_source": found["prev_sources"].get(f),
                "chain_id": chain["chain_id"],
                "detail": found["detail"],
            }
    return out


def provenance_overview() -> dict:
    """所有当前记录各字段来源（单只读快照）。"""
    with read_snapshot() as conn:
        rows = conn.execute("SELECT * FROM records ORDER BY id").fetchall()
        chains = conn.execute(
            "SELECT * FROM provenance_chains WHERE deleted_at IS NULL ORDER BY chain_id"
        ).fetchall()
        events = conn.execute(
            "SELECT * FROM provenance_events WHERE chain_id IN"
            " (SELECT chain_id FROM provenance_chains WHERE deleted_at IS NULL)"
            " ORDER BY event_id",
        ).fetchall()
    chain_map = {c["record_id"]: c for c in chains}
    by_chain = {}
    for ev in events:
        by_chain.setdefault(ev["chain_id"], []).append(_event_dict(ev))
    out = {}
    for row in rows:
        rid = row["id"]
        current = _row_values(row)
        chain = chain_map.get(rid)
        if chain is None:
            out[rid] = {f: {"value": current[f], "unknown": True} for f in FIELDS}
            continue
        evs = by_chain.get(chain["chain_id"], [])
        fields = {}
        for f in FIELDS:
            found = next((ev for ev in reversed(evs) if f in ev["after_values"]), None)
            if found is None:
                fields[f] = {"value": current[f], "unknown": True}
            else:
                fo = found["field_origins"].get(f, {})
                fields[f] = {
                    "value": current[f],
                    "unknown": False,
                    "event_id": found["event_id"],
                    "kind": found["kind"],
                    "created_at": found["created_at"],
                    "preview_token": found["preview_token"],
                    "baseline_id": found["baseline_id"],
                    "origin": fo.get("origin", found["kind"]),
                    "revision_value": fo.get("revision_value"),
                    "resolution": fo.get("resolution"),
                }
        out[rid] = fields
    return out


def record_history(rid: str) -> list:
    """一条记录的所有世代（含已删除世代），按链先后分组。"""
    with read_snapshot() as conn:
        chains = conn.execute(
            "SELECT * FROM provenance_chains WHERE record_id=? ORDER BY chain_id", (rid,)
        ).fetchall()
        events = conn.execute(
            "SELECT * FROM provenance_events WHERE record_id=? ORDER BY event_id", (rid,)
        ).fetchall()
    events = [_event_dict(e) for e in events]
    grouped = []
    for idx, c in enumerate(chains, start=1):
        grouped.append(
            {
                "generation": idx,
                "chain": dict(c),
                "live": c["deleted_at"] is None,
                "events": [e for e in events if e["chain_id"] == c["chain_id"]],
            }
        )
    return grouped


def deleted_records() -> list:
    """所有已删除且未再重建的记录，附删除时的值。"""
    with read_snapshot() as conn:
        rows = conn.execute(
            "SELECT c.record_id, MAX(c.deleted_at) AS deleted_at, MAX(c.chain_id) AS chain_id"
            " FROM provenance_chains c WHERE c.deleted_at IS NOT NULL AND NOT EXISTS"
            " (SELECT 1 FROM provenance_chains c2 WHERE c2.record_id=c.record_id AND c2.deleted_at IS NULL)"
            " GROUP BY c.record_id ORDER BY deleted_at DESC, c.record_id"
        ).fetchall()
        result = []
        for r in rows:
            ev = conn.execute(
                "SELECT * FROM provenance_events WHERE chain_id=? AND operation='delete'"
                " ORDER BY event_id DESC LIMIT 1",
                (r["chain_id"],),
            ).fetchone()
            generations = conn.execute(
                "SELECT COUNT(*) AS n FROM provenance_chains WHERE record_id=?",
                (r["record_id"],),
            ).fetchone()["n"]
            d = _event_dict(ev) if ev else None
            result.append(
                {
                    "id": r["record_id"],
                    "deleted_at": r["deleted_at"],
                    "values": d["before_values"] if d else {},
                    "kind": d["kind"] if d else "unknown",
                    "preview_token": d["preview_token"] if d else None,
                    "baseline_id": d["baseline_id"] if d else None,
                    "generations": generations,
                }
            )
    return result


def search_records(q: str) -> dict:
    """按 ID 包含匹配返回当前记录与历史（含已删）记录 ID。"""
    q = (q or "").strip()
    like = f"%{q}%"
    with read_snapshot() as conn:
        current = conn.execute("SELECT id FROM records WHERE id LIKE ? ORDER BY id", (like,)).fetchall()
        all_ids = conn.execute(
            "SELECT DISTINCT record_id FROM provenance_chains WHERE record_id LIKE ? ORDER BY record_id",
            (like,),
        ).fetchall()
    return {
        "current": [r["id"] for r in current],
        "history": [r["record_id"] for r in all_ids],
    }


def provenance_audit() -> dict:
    """来源链概览统计。"""
    with read_snapshot() as conn:
        chains = conn.execute("SELECT COUNT(*) AS n FROM provenance_chains").fetchone()["n"]
        live = conn.execute(
            "SELECT COUNT(*) AS n FROM provenance_chains WHERE deleted_at IS NULL"
        ).fetchone()["n"]
        deleted = conn.execute(
            "SELECT COUNT(DISTINCT record_id) AS n FROM provenance_chains"
            " WHERE deleted_at IS NOT NULL AND record_id NOT IN"
            " (SELECT record_id FROM provenance_chains WHERE deleted_at IS NULL)"
        ).fetchone()["n"]
        events = conn.execute("SELECT COUNT(*) AS n FROM provenance_events").fetchone()["n"]
        merges = conn.execute(
            "SELECT COUNT(*) AS n FROM provenance_events WHERE kind='merge'"
        ).fetchone()["n"]
        manuals = conn.execute(
            "SELECT COUNT(*) AS n FROM provenance_events WHERE kind='manual'"
        ).fetchone()["n"]
    return {
        "chains": chains, "live": live, "deleted": deleted,
        "events": events, "merges": merges, "manuals": manuals,
    }
