from __future__ import annotations

import hashlib
import json
import os
import re
import socket
import time
from dataclasses import dataclass
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

# Oracle JDBC on this host needs these flags to avoid ORA-01882. Set them
# before JayDeBeApi/JPype can start the JVM; an explicit service value wins.
os.environ.setdefault(
    "JAVA_TOOL_OPTIONS",
    "-Doracle.jdbc.timezoneAsRegion=false -Duser.timezone=UTC",
)

import jaydebeapi
import redis


ROOT = Path(__file__).resolve().parent
IDENTIFIER = re.compile(r"^[A-Z][A-Z0-9_$#]*(?:\.[A-Z][A-Z0-9_$#]*)?$")

DISPLAY_COLUMNS = (
    "DIG_ID",
    "DIG_TRAMITE",
    "DIG_ASEGURADORA",
    "DIG_FECHA_PLANILLA",
    "DIG_FECHA_HASTA",
    "DIG_PROCESADO",
    "DIG_FECHA_PROCESO",
    "DIG_HC",
    "DIG_EXPEDIENTE",
    "DIG_ANIO",
    "DIG_AREA_DEP",
    "REVISADA_CONTRL_DOC",
    "DIG_SERVICIO",
    "DIG_ESPECIALIDAD",
    "DIG_CEDULA",
    "DIG_FECHA_ALTA",
    "DIG_PATH_REPO",
)

SEARCH_TEXT_COLUMNS = (
    "DIG_ASEGURADORA",
    "DIG_EXPEDIENTE",
    "DIG_ANIO",
    "DIG_AREA_DEP",
    "DIG_SERVICIO",
    "DIG_ESPECIALIDAD",
    "DIG_CEDULA",
    "DIG_USUARIO",
    "DIG_CODIGO_USUARIO",
    "DIG_PATH_REPO",
)

SEARCH_NUMBER_COLUMNS = (
    "DIG_ID",
    "DIG_TRAMITE",
    "DIG_HC",
    "DIG_ID_GENERACION",
    "DIG_NUMERO_SOLICITUD",
    "DIG_ID_TRAMITE",
)

DATE_COLUMNS = {
    "DIG_FECHA_PLANILLA",
    "DIG_FECHA_HASTA",
    "DIG_FECHA_PROCESO",
    "DIG_FECHA_ALTA",
    "DIG_FECHA_COBERTURA",
    "DIG_FECHA_ACTUALIZACION_ALTA",
    "DIG_FECHA_ACTUALIZACION_PERM",
    "FECHA_CARGA_OBJECION",
    "FECHA_REVISION_OBJECION",
}

FULL_ROWS_KEY = "dig:v1:full:rows"
FULL_IDS_KEY = "dig:v1:full:ids"
FULL_TRAMITE_KEY = "dig:v1:full:tramite"
FULL_YEAR_PREFIX = "dig:v1:full:year:"
FULL_META_KEY = "dig:v1:full:meta"
CURRENT_MONTH_META_KEY = "dig:v1:current-month:meta"
FULL_LOCK_KEY = "dig:v1:full:lock"
CURRENT_YEAR = str(datetime.now(timezone.utc).year)

FS_DIRS_KEY = "dig:v1:fs:dirs"
FS_PDFS_KEY = "dig:v1:fs:pdfs"
FS_TRAMITE_DIRS_KEY = "dig:v1:fs:tramite-dirs"
FS_TRAMITE_PDFS_PREFIX = "dig:v1:fs:tramite-pdfs:"
FS_META_KEY = "dig:v1:fs:meta"
FS_LOCK_KEY = "dig:v1:fs:lock"
TRAMITE_JOB_STATUS_PREFIX = "dig:v1:tramite-refresh:"


def _control_doc_sync_table() -> str:
    """Return the validated Oracle owner/table for durable CD sync requests."""
    owner = (os.environ.get("ORACLE_CONTROL_DOC_OWNER") or "DIGITALIZACION").strip().upper()
    if not re.fullmatch(r"[A-Z][A-Z0-9_$#]*", owner):
        owner = "DIGITALIZACION"
    return f"{owner}.CD_REDIS_SYNC"


def _load_dotenv(path: Path) -> None:
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


_load_dotenv(ROOT / ".env")


def _identifier(value: str, default: str) -> str:
    result = (value or default).strip().upper()
    if not IDENTIFIER.fullmatch(result):
        raise RuntimeError(f"Identificador Oracle inválido: {result!r}")
    return result


def _resolve_jar(value: str) -> Path:
    configured = Path(value).expanduser()
    candidates = [configured]
    if not configured.is_absolute():
        candidates.extend((ROOT / configured, ROOT.parent / configured))
    for candidate in candidates:
        if candidate.exists():
            return candidate.resolve()
    raise RuntimeError(
        "No se encontró ORACLE_JDBC_JAR; rutas revisadas: "
        + ", ".join(str(c) for c in candidates)
    )


def _parse_targets(value: str) -> list[tuple[str, int, str]]:
    targets: list[tuple[str, int, str]] = []
    for item in value.split(","):
        item = item.strip()
        if not item:
            continue
        parts = item.split(":")
        if len(parts) != 3:
            raise RuntimeError(f"ORACLE_TARGETS inválido: {item!r}")
        host, port, sid = parts
        targets.append((host, int(port), sid))
    if not targets:
        raise RuntimeError("ORACLE_TARGETS no tiene destinos configurados")
    return targets


def _normalize_year_range(anio_desde: str = "", anio_hasta: str = "") -> tuple[str, str]:
    """Return a validated inclusive year range, or (all, all) for history."""
    desde = (anio_desde or "").strip().lower()
    hasta = (anio_hasta or "").strip().lower()
    if desde in {"", "all", "todos"} and hasta in {"", "all", "todos"}:
        return "all", "all"
    if desde in {"all", "todos"} or hasta in {"all", "todos"}:
        raise ValueError("anio_desde y anio_hasta deben ser ambos YYYY o ambos all")
    if not re.fullmatch(r"\d{4}", desde) or not re.fullmatch(r"\d{4}", hasta):
        raise ValueError("anio_desde y anio_hasta deben tener formato YYYY o all")
    if int(desde) > int(hasta):
        raise ValueError("anio_desde no puede ser mayor que anio_hasta")
    return desde, hasta


@dataclass(frozen=True)
class Settings:
    oracle_user: str
    oracle_password: str
    oracle_owner: str
    oracle_table: str
    oracle_jar: Path
    oracle_targets: list[tuple[str, int, str]]
    redis_url: str
    cache_ttl: int
    row_cache_ttl: int
    max_page_size: int
    full_batch_size: int
    full_year_from: str
    full_year_to: str
    filesystem_base_dir: Path
    filesystem_year_from: str
    filesystem_year_to: str
    filesystem_batch_size: int

    @classmethod
    def from_env(cls) -> "Settings":
        user = os.environ.get("ORACLE_USER", "DIGITALIZACION").strip()
        password = os.environ.get("ORACLE_PASSWORD", "")
        if not user or not password:
            raise RuntimeError("Faltan ORACLE_USER u ORACLE_PASSWORD")
        full_year_from, full_year_to = _normalize_year_range(
            os.environ.get("DIGI_CACHE_YEAR_FROM", str(int(CURRENT_YEAR) - 1)),
            os.environ.get("DIGI_CACHE_YEAR_TO", CURRENT_YEAR),
        )
        filesystem_year_from, filesystem_year_to = _normalize_year_range(
            os.environ.get("DIGI_FS_YEAR_FROM", CURRENT_YEAR),
            os.environ.get("DIGI_FS_YEAR_TO", CURRENT_YEAR),
        )
        return cls(
            oracle_user=user,
            oracle_password=password,
            oracle_owner=_identifier(
                os.environ.get("ORACLE_SOURCE_OWNER", user), user
            ),
            oracle_table=_identifier(
                os.environ.get("ORACLE_SOURCE_TABLE", "DIGITALIZACION"),
                "DIGITALIZACION",
            ),
            oracle_jar=_resolve_jar(
                os.environ.get("ORACLE_JDBC_JAR", "jdbc/ojdbc8 copy.jar")
            ),
            oracle_targets=_parse_targets(
                os.environ.get(
                    "ORACLE_TARGETS",
                    "172.16.60.20:1521:prdsgh1,172.16.60.21:1521:prdsgh2",
                )
            ),
            redis_url=os.environ.get(
                "DIGI_REDIS_URL", os.environ.get("REDIS_URL", "redis://127.0.0.1:6379/0")
            ),
            cache_ttl=max(5, int(os.environ.get("DIGI_CACHE_TTL", "120"))),
            row_cache_ttl=max(5, int(os.environ.get("DIGI_ROW_CACHE_TTL", "300"))),
            max_page_size=min(100, max(1, int(os.environ.get("DIGI_MAX_PAGE_SIZE", "100")))),
            full_batch_size=min(5000, max(100, int(os.environ.get("DIGI_FULL_BATCH_SIZE", "1000")))),
            full_year_from=full_year_from,
            full_year_to=full_year_to,
            filesystem_base_dir=Path(
                os.environ.get("DIGI_BASE_DIR", "datos")
            ).expanduser().resolve(),
            filesystem_year_from=filesystem_year_from,
            filesystem_year_to=filesystem_year_to,
            filesystem_batch_size=min(
                5000, max(100, int(os.environ.get("DIGI_FS_BATCH_SIZE", "1000")))
            ),
        )


settings = Settings.from_env()


def _jdbc_url(host: str, port: int, sid: str) -> str:
    return f"jdbc:oracle:thin:@{host}:{port}:{sid}"


def oracle_connect():
    last_error: BaseException | None = None
    for host, port, sid in settings.oracle_targets:
        try:
            return jaydebeapi.connect(
                "oracle.jdbc.OracleDriver",
                _jdbc_url(host, port, sid),
                [settings.oracle_user, settings.oracle_password],
                jars=[str(settings.oracle_jar)],
            )
        except BaseException as exc:
            last_error = exc
    raise RuntimeError("No se pudo conectar a ningún nodo Oracle RAC") from last_error


def _json_value(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, Decimal):
        return int(value) if value == value.to_integral_value() else float(value)
    if isinstance(value, (datetime, date)):
        return value.isoformat(sep=" ") if isinstance(value, datetime) else value.isoformat()
    return str(value)


def _rows_as_dicts(cursor) -> list[dict[str, Any]]:
    columns = [str(item[0]).upper() for item in (cursor.description or [])]
    return [
        {column: _json_value(value) for column, value in zip(columns, row)}
        for row in cursor.fetchall()
    ]


def _safe_page(page: int, size: int) -> tuple[int, int]:
    page = max(1, page)
    size = min(settings.max_page_size, max(1, size))
    return page, size


def build_filters(
    q: str = "",
    procesado: str = "",
    revisada: str = "",
    fecha_desde: str = "",
    fecha_hasta: str = "",
    anio_desde: str = "",
    anio_hasta: str = "",
    anio: str | None = None,
) -> tuple[str, list[Any]]:
    clauses: list[str] = []
    params: list[Any] = []
    q = q.strip()
    if q:
        if q.isdigit():
            # DIG_TRAMITE is the main business lookup and has a unique Oracle index.
            clauses.append("DIG_TRAMITE = ?")
            params.append(int(q))
        else:
            clauses.append("TO_CHAR(DIG_TRAMITE) LIKE ?")
            params.append(f"%{q}%")
    if procesado.strip().upper() in {"S", "N"}:
        clauses.append("TRIM(NVL(DIG_PROCESADO, 'N')) = ?")
        params.append(procesado.strip().upper())
    if revisada.strip().upper() in {"S", "N"}:
        clauses.append("TRIM(NVL(REVISADA_CONTRL_DOC, 'N')) = ?")
        params.append(revisada.strip().upper())
    if fecha_desde.strip():
        clauses.append("DIG_FECHA_ALTA >= TO_DATE(?, 'YYYY-MM-DD')")
        params.append(fecha_desde.strip())
    if fecha_hasta.strip():
        clauses.append("DIG_FECHA_ALTA < TO_DATE(?, 'YYYY-MM-DD') + 1")
        params.append(fecha_hasta.strip())
    if anio is not None and not anio_desde and not anio_hasta:
        anio_desde = anio_hasta = anio
    anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
    if anio_desde != "all":
        clauses.append("TRIM(NVL(DIG_ANIO, '')) BETWEEN ? AND ?")
        params.extend([anio_desde, anio_hasta])
    return (" WHERE " + " AND ".join(clauses)) if clauses else "", params


def _cache_key(params: dict[str, Any]) -> str:
    encoded = json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
    digest = hashlib.sha256(encoded.encode("utf-8")).hexdigest()
    return f"dig:v1:search:{digest}"


def _progress_payload(
    meta: dict[str, Any], loaded_key: str, target_key: str, rate_key: str
) -> dict[str, Any]:
    final_key = {
        "loaded_rows": "total",
        "loaded_directories": "directories",
        "loaded_pdfs": "pdfs",
    }.get(loaded_key)
    loaded_value = (
        meta.get(final_key)
        if meta.get("status") == "ready" and final_key
        else meta.get(loaded_key, 0)
    )
    loaded = int(loaded_value or 0)
    target_value = meta.get(target_key)
    if meta.get("status") == "ready":
        target_value = loaded
    target = int(target_value or 0)
    elapsed = float(meta.get("elapsed_seconds", 0) or 0)
    started_epoch = meta.get("started_epoch")
    if not started_epoch and meta.get("started_at"):
        try:
            started_epoch = datetime.fromisoformat(meta["started_at"]).timestamp()
        except (TypeError, ValueError):
            started_epoch = None
    if meta.get("status") in {"loading", "counting"} and started_epoch:
        elapsed = max(0.0, time.time() - float(started_epoch))
    elif elapsed <= 0 and meta.get("status") == "ready" and meta.get("loaded_at") and started_epoch:
        try:
            elapsed = max(
                0.0,
                datetime.fromisoformat(meta["loaded_at"]).timestamp() - float(started_epoch),
            )
        except (TypeError, ValueError):
            pass
    rate = float(meta.get(rate_key, 0) or 0)
    if elapsed > 0 and loaded > 0:
        rate = loaded / elapsed
    percent = 100.0 if meta.get("status") == "ready" and target else (
        round(min(100.0, loaded * 100.0 / target), 1) if target else 0.0
    )
    eta = max(0.0, (target - loaded) / rate) if target and rate > 0 else None
    return {
        "status": meta.get("status", "empty"),
        "loaded": loaded,
        "target": target,
        "percent": percent,
        "elapsed_seconds": round(elapsed, 1),
        "rate_per_second": round(rate, 1),
        "eta_seconds": round(eta, 1) if eta is not None else None,
        "started_at": meta.get("started_at"),
        "finished_at": meta.get("finished_at") or meta.get("loaded_at"),
        "error": meta.get("error"),
    }


class Cache:
    def __init__(self) -> None:
        self.client = redis.Redis.from_url(settings.redis_url, decode_responses=True)

    def ping(self) -> bool:
        return bool(self.client.ping())

    def get_json(self, key: str) -> Any | None:
        raw = self.client.get(key)
        return None if raw is None else json.loads(raw)

    def set_json(self, key: str, value: Any, ttl: int) -> None:
        self.client.setex(key, ttl, json.dumps(value, ensure_ascii=False))

    def clear_search(self) -> int:
        keys = list(self.client.scan_iter(match="dig:v1:search:*", count=500))
        keys.extend(self.client.scan_iter(match="dig:v1:row:*", count=500))
        return self.client.delete(*keys) if keys else 0

    def enqueue(self, payload: dict[str, Any]) -> None:
        self.client.lpush("dig:v1:jobs", json.dumps(payload, ensure_ascii=False))

    def enqueue_priority(self, payload: dict[str, Any]) -> None:
        self.client.lpush("dig:v1:jobs:priority", json.dumps(payload, ensure_ascii=False))

    def full_status(self) -> dict[str, Any]:
        return self.client.hgetall(FULL_META_KEY)

    def full_ready(self) -> bool:
        return self.full_status().get("status") == "ready"

    def full_covers_range(self, anio_desde: str, anio_hasta: str) -> bool:
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        if anio_desde == "all":
            return False
        meta = self.full_status()
        return (
            meta.get("status") == "ready"
            and meta.get("year_from") is not None
            and meta.get("year_to") is not None
            and int(meta["year_from"]) <= int(anio_desde)
            and int(meta["year_to"]) >= int(anio_hasta)
        )

    def full_page(
        self,
        page: int,
        size: int,
        anio_desde: str,
        anio_hasta: str,
    ) -> dict[str, Any] | None:
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        if not self.full_covers_range(anio_desde, anio_hasta):
            return None
        meta = self.full_status()
        page, size = _safe_page(page, size)
        start = (page - 1) * size
        if anio_desde == meta.get("year_from") and anio_hasta == meta.get("year_to"):
            ids_key = FULL_IDS_KEY
        elif anio_desde == anio_hasta:
            ids_key = f"{FULL_YEAR_PREFIX}{anio_desde}"
        else:
            # A multi-year subset would require a temporary sorted-set union;
            # Oracle remains the source for that uncommon case.
            return None
        ids = self.client.zrevrange(ids_key, start, start + size - 1)
        raw_rows = self.client.hmget(FULL_ROWS_KEY, ids)
        items = [json.loads(raw) for raw in raw_rows if raw is not None]
        total = int(meta.get("total", self.client.zcard(ids_key)) or 0) if ids_key == FULL_IDS_KEY else self.client.zcard(ids_key)
        return {
            "items": items,
            "total": total,
            "page": page,
            "size": size,
            "pages": (total + size - 1) // size if total else 0,
            "cache": "redis-full",
            "generated_at": meta.get("loaded_at"),
            "anio_desde": anio_desde,
            "anio_hasta": anio_hasta,
        }

    def full_row(self, dig_id: int) -> dict[str, Any] | None:
        if not self.full_ready():
            return None
        raw = self.client.hget(FULL_ROWS_KEY, str(dig_id))
        return None if raw is None else json.loads(raw)

    def full_tramite(self, tramite: int, anio_desde: str, anio_hasta: str) -> dict[str, Any] | None:
        if not self.full_ready():
            return None
        if not self.full_covers_range(anio_desde, anio_hasta):
            return None
        dig_id = self.client.hget(FULL_TRAMITE_KEY, str(tramite))
        if dig_id is None:
            return None
        row = self.full_row(int(dig_id))
        if row is None:
            return None
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        row_year = str(row.get("DIG_ANIO") or "").strip()
        if anio_desde != "all" and not (anio_desde <= row_year <= anio_hasta):
            return None
        return row

    def clear_full(self) -> None:
        keys = [FULL_ROWS_KEY, FULL_IDS_KEY, FULL_TRAMITE_KEY, FULL_META_KEY]
        keys.extend(self.client.scan_iter(match=f"{FULL_YEAR_PREFIX}*", count=500))
        self.client.delete(*keys)

    def filesystem_status(self) -> dict[str, Any]:
        return self.client.hgetall(FS_META_KEY)

    def filesystem_ready(self) -> bool:
        return self.filesystem_status().get("status") == "ready"

    def filesystem_covers_range(self, anio_desde: str, anio_hasta: str) -> bool:
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        if anio_desde == "all":
            return False
        meta = self.filesystem_status()
        return (
            meta.get("status") == "ready"
            and meta.get("year_from") is not None
            and meta.get("year_to") is not None
            and int(meta["year_from"]) <= int(anio_desde)
            and int(meta["year_to"]) >= int(anio_hasta)
        )

    def clear_filesystem(self) -> int:
        deleted = self.client.delete(
            FS_DIRS_KEY, FS_PDFS_KEY, FS_TRAMITE_DIRS_KEY, FS_META_KEY
        )
        batch: list[str] = []
        for key in self.client.scan_iter(match=f"{FS_TRAMITE_PDFS_PREFIX}*", count=500):
            batch.append(key)
            if len(batch) >= 500:
                deleted += self.client.delete(*batch)
                batch.clear()
        if batch:
            deleted += self.client.delete(*batch)
        return deleted

    def filesystem_for_tramite(
        self, tramite: int, anio_desde: str, anio_hasta: str
    ) -> dict[str, Any]:
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        meta = self.filesystem_status()
        if meta.get("status") != "ready":
            return {"status": "not_ready", "pdfs": [], "folders": []}
        if not self.filesystem_covers_range(anio_desde, anio_hasta):
            return {
                "status": "range_not_loaded",
                "pdfs": [],
                "folders": [],
                "loaded_year_from": meta.get("year_from"),
                "loaded_year_to": meta.get("year_to"),
            }

        years = (
            [str(year) for year in range(int(anio_desde), int(anio_hasta) + 1)]
            if anio_desde != "all"
            else []
        )
        folder_members: list[str] = []
        pdf_paths: list[str] = []
        for year in years:
            prefix = f"{year}|{int(tramite)}|"
            folder_members.extend(
                self.client.zrangebylex(
                    FS_TRAMITE_DIRS_KEY, f"[{prefix}", f"[{prefix}\xff"
                )
            )
            pdf_paths.extend(
                self.client.lrange(
                    f"{FS_TRAMITE_PDFS_PREFIX}{year}:{int(tramite)}", 0, -1
                )
            )

        raw_pdfs = self.client.hmget(FS_PDFS_KEY, pdf_paths)
        pdfs: list[dict[str, Any]] = []
        for relative_path, raw in zip(pdf_paths, raw_pdfs):
            item = json.loads(raw) if raw else {"relative_path": relative_path}
            item["path"] = str(settings.filesystem_base_dir / relative_path)
            pdfs.append(item)
        folders = [
            {
                "relative_path": member.split("|", 2)[2],
                "path": str(settings.filesystem_base_dir / member.split("|", 2)[2]),
            }
            for member in folder_members
        ]
        return {
            "status": "ready",
            "cache": "redis-filesystem",
            "year_from": anio_desde,
            "year_to": anio_hasta,
            "folders": folders,
            "pdfs": pdfs,
            "folder_count": len(folders),
            "pdf_count": len(pdfs),
        }

    def filesystem_counts_for_tramites(
        self, tramites: list[int], anio_desde: str, anio_hasta: str
    ) -> dict[str, Any]:
        """Return PDF counts for several trámites with one Redis pipeline."""
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        meta = self.filesystem_status()
        if meta.get("status") != "ready":
            return {"status": "not_ready", "counts": {}}
        if not self.filesystem_covers_range(anio_desde, anio_hasta):
            return {
                "status": "range_not_loaded",
                "counts": {},
                "loaded_year_from": meta.get("year_from"),
                "loaded_year_to": meta.get("year_to"),
            }

        unique = sorted({int(tramite) for tramite in tramites if int(tramite) > 0})
        counts = {str(tramite): 0 for tramite in unique}
        years = [
            str(year)
            for year in range(int(anio_desde), int(anio_hasta) + 1)
        ]
        keys: list[tuple[str, str]] = []
        pipe = self.client.pipeline(transaction=False)
        for year in years:
            for tramite in unique:
                key = f"{FS_TRAMITE_PDFS_PREFIX}{year}:{tramite}"
                keys.append((str(tramite), key))
                pipe.llen(key)
        values = pipe.execute()
        for (tramite, _key), value in zip(keys, values):
            counts[tramite] = counts.get(tramite, 0) + int(value or 0)
        return {
            "status": "ready",
            "cache": "redis-filesystem",
            "year_from": anio_desde,
            "year_to": anio_hasta,
            "counts": counts,
        }


cache = Cache()


def load_full_cache() -> dict[str, Any]:
    """Stream the configured recent-year window into Redis in bounded batches."""
    lock = cache.client.lock(FULL_LOCK_KEY, timeout=6 * 60 * 60, blocking=False)
    if not lock.acquire():
        status = cache.full_status()
        return {"status": "busy", "loaded_rows": int(status.get("loaded_rows", 0) or 0)}

    started = datetime.now(timezone.utc).isoformat()
    started_epoch = time.time()
    cache.client.hset(
        FULL_META_KEY,
        mapping={
            "status": "loading",
            "started_at": started,
            "started_epoch": str(started_epoch),
            "loaded_rows": "0",
            "target_rows": "0",
            "elapsed_seconds": "0",
            "rate_rows_sec": "0",
        },
    )
    old_year_keys = list(cache.client.scan_iter(match=f"{FULL_YEAR_PREFIX}*", count=500))
    cache.client.delete(FULL_ROWS_KEY, FULL_IDS_KEY, FULL_TRAMITE_KEY, *old_year_keys)
    total = 0
    conn = None
    try:
        conn = oracle_connect()
        cursor = conn.cursor()
        cursor.arraysize = settings.full_batch_size
        table_ref = f"{settings.oracle_owner}.{settings.oracle_table}"
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_ref} "
            "WHERE TRIM(NVL(DIG_ANIO, '')) BETWEEN ? AND ?",
            [settings.full_year_from, settings.full_year_to],
        )
        target_rows = int(cursor.fetchone()[0] or 0)
        cache.client.hset(FULL_META_KEY, mapping={"target_rows": str(target_rows)})
        cursor.execute(
            f"SELECT * FROM {table_ref} "
            "WHERE TRIM(NVL(DIG_ANIO, '')) BETWEEN ? AND ? ORDER BY DIG_ID",
            [settings.full_year_from, settings.full_year_to],
        )
        columns = [str(item[0]).upper() for item in (cursor.description or [])]
        while True:
            rows = cursor.fetchmany(settings.full_batch_size)
            if not rows:
                break
            row_map: dict[str, str] = {}
            tramite_map: dict[str, str] = {}
            score_map: dict[str, float] = {}
            year_score_maps: dict[str, dict[str, float]] = {}
            for raw_row in rows:
                record = {
                    column: _json_value(value)
                    for column, value in zip(columns, raw_row)
                }
                dig_id = record.get("DIG_ID")
                if dig_id is None:
                    continue
                dig_id_s = str(int(dig_id))
                row_map[dig_id_s] = json.dumps(
                    record, ensure_ascii=False, separators=(",", ":")
                )
                score_map[dig_id_s] = float(dig_id)
                anio = str(record.get("DIG_ANIO") or "").strip()
                if re.fullmatch(r"\d{4}", anio):
                    year_score_maps.setdefault(anio, {})[dig_id_s] = float(dig_id)
                tramite = record.get("DIG_TRAMITE")
                if tramite is not None:
                    tramite_map[str(int(tramite))] = dig_id_s
            if row_map:
                pipe = cache.client.pipeline(transaction=False)
                pipe.hset(FULL_ROWS_KEY, mapping=row_map)
                pipe.zadd(FULL_IDS_KEY, score_map)
                for anio, year_scores in year_score_maps.items():
                    pipe.zadd(f"{FULL_YEAR_PREFIX}{anio}", year_scores)
                if tramite_map:
                    pipe.hset(FULL_TRAMITE_KEY, mapping=tramite_map)
                pipe.execute()
                total += len(row_map)
                elapsed = max(0.001, time.time() - started_epoch)
                cache.client.hset(
                    FULL_META_KEY,
                    mapping={
                        "loaded_rows": str(total),
                        "elapsed_seconds": f"{elapsed:.1f}",
                        "rate_rows_sec": f"{total / elapsed:.1f}",
                    },
                )
        loaded_at = datetime.now(timezone.utc).isoformat()
        elapsed = max(0.001, time.time() - started_epoch)
        meta = {
            "status": "ready",
            "total": str(total),
            "loaded_rows": str(total),
            "started_at": started,
            "loaded_at": loaded_at,
            "source": f"{settings.oracle_owner}.{settings.oracle_table}",
            "year_from": settings.full_year_from,
            "year_to": settings.full_year_to,
            "target_rows": str(target_rows),
            "elapsed_seconds": f"{elapsed:.1f}",
            "rate_rows_sec": f"{total / elapsed:.1f}",
            "finished_at": loaded_at,
        }
        cache.client.hset(FULL_META_KEY, mapping=meta)
        return {
            "status": "ready",
            "total": total,
            "loaded_at": loaded_at,
            "elapsed_seconds": elapsed,
        }
    except Exception as exc:
        cache.client.hset(
            FULL_META_KEY,
            mapping={
                "status": "error",
                "loaded_rows": str(total),
                "error": str(exc)[:500],
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": f"{max(0.0, time.time() - started_epoch):.1f}",
            },
        )
        raise
    finally:
        if conn is not None:
            conn.close()
        try:
            lock.release()
        except Exception:
            pass


def refresh_current_month_cache() -> dict[str, Any]:
    """Replace only the current FE_PLA_ANIOMES slice in the full Redis cache."""
    month = datetime.now(timezone.utc).strftime("%Y%m")
    lock = cache.client.lock(FULL_LOCK_KEY, timeout=15 * 60, blocking=False)
    if not lock.acquire():
        return {"status": "busy", "month": month}
    conn = None
    started = datetime.now(timezone.utc).isoformat()
    started_epoch = time.time()
    cache.client.hset(
        CURRENT_MONTH_META_KEY,
        mapping={"status": "loading", "month": month, "started_at": started,
                 "started_epoch": str(started_epoch), "loaded_rows": "0",
                 "target_rows": "0", "removed_rows": "0"},
    )
    try:
        if not cache.full_ready():
            cache.client.hset(CURRENT_MONTH_META_KEY, mapping={"status": "not_ready", "finished_at": started})
            return {"status": "not_ready", "month": month}

        conn = oracle_connect()
        cursor = conn.cursor()
        cursor.arraysize = settings.full_batch_size
        table_ref = f"{settings.oracle_owner}.{settings.oracle_table}"
        cursor.execute(
            f"SELECT COUNT(*) FROM {table_ref} WHERE FE_PLA_ANIOMES = ?", [month]
        )
        target_rows = int(cursor.fetchone()[0] or 0)
        cache.client.hset(CURRENT_MONTH_META_KEY, mapping={"target_rows": str(target_rows)})
        cursor.execute(
            f"SELECT * FROM {table_ref} "
            "WHERE FE_PLA_ANIOMES = ? ORDER BY DIG_ID",
            [month],
        )
        columns = [str(item[0]).upper() for item in (cursor.description or [])]
        records: dict[str, dict[str, Any]] = {}
        while True:
            rows = cursor.fetchmany(settings.full_batch_size)
            if not rows:
                break
            for raw_row in rows:
                record = {
                    column: _json_value(value)
                    for column, value in zip(columns, raw_row)
                }
                dig_id = record.get("DIG_ID")
                if dig_id is not None:
                    records[str(int(dig_id))] = record
            cache.client.hset(CURRENT_MONTH_META_KEY, mapping={"loaded_rows": str(len(records))})

        month_key = f"{FULL_YEAR_PREFIX}{month[:4]}"
        year_ids = cache.client.zrange(month_key, 0, -1)
        old_raw_rows = cache.client.hmget(FULL_ROWS_KEY, year_ids)
        old_ids = {
            dig_id
            for dig_id, raw in zip(year_ids, old_raw_rows)
            if raw
            and str(json.loads(raw).get("FE_PLA_ANIOMES") or "").strip() == month
        }
        new_ids = set(records)
        removed_ids = old_ids - new_ids
        pipe = cache.client.pipeline(transaction=True)
        if removed_ids:
            pipe.hdel(FULL_ROWS_KEY, *removed_ids)
            pipe.zrem(FULL_IDS_KEY, *removed_ids)
            pipe.zrem(month_key, *removed_ids)
        if records:
            row_map = {
                dig_id: json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                for dig_id, record in records.items()
            }
            pipe.hset(FULL_ROWS_KEY, mapping=row_map)
            pipe.zadd(FULL_IDS_KEY, {dig_id: float(dig_id) for dig_id in records})
            pipe.zadd(month_key, {dig_id: float(dig_id) for dig_id in records})
        old_tramite_ids = {
            str(tramite): dig_id
            for tramite, dig_id in cache.client.hgetall(FULL_TRAMITE_KEY).items()
            if dig_id in old_ids
        }
        if old_tramite_ids:
            pipe.hdel(FULL_TRAMITE_KEY, *old_tramite_ids.keys())
        tramite_map = {
            str(int(record["DIG_TRAMITE"])): dig_id
            for dig_id, record in records.items()
            if record.get("DIG_TRAMITE") is not None
        }
        if tramite_map:
            pipe.hset(FULL_TRAMITE_KEY, mapping=tramite_map)
        pipe.execute()
        finished = datetime.now(timezone.utc).isoformat()
        cache.client.hset(CURRENT_MONTH_META_KEY, mapping={
            "status": "ready", "loaded_rows": str(len(records)),
            "removed_rows": str(len(removed_ids)), "loaded_at": finished,
            "finished_at": finished, "elapsed_seconds": f"{time.time() - started_epoch:.1f}",
        })
        return {
            "status": "ready",
            "month": month,
            "loaded": len(records),
            "removed": len(removed_ids),
        }
    except Exception as exc:
        cache.client.hset(CURRENT_MONTH_META_KEY, mapping={
            "status": "error", "error": str(exc)[:500],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        raise
    finally:
        if conn is not None:
            conn.close()
        try:
            lock.release()
        except Exception:
            pass


def _filesystem_entries(root: Path, years: list[str]):
    """Yield (directory|pdf, relative POSIX path, DirEntry) without following links."""
    for year in years:
        year_dir = root / year
        if not year_dir.is_dir():
            continue
        stack: list[tuple[Path, str]] = [(year_dir, year)]
        while stack:
            current, current_relative = stack.pop()
            yield "directory", current_relative, None
            try:
                with os.scandir(current) as iterator:
                    entries = list(iterator)
            except OSError:
                continue
            for entry in entries:
                relative = f"{current_relative}/{entry.name}"
                try:
                    if entry.is_dir(follow_symlinks=False):
                        stack.append((Path(entry.path), relative))
                    elif (
                        entry.is_file(follow_symlinks=False)
                        and entry.name.lower().endswith(".pdf")
                    ):
                        yield "pdf", relative, entry
                except OSError:
                    continue


def load_filesystem_cache() -> dict[str, Any]:
    """Index folders and PDF paths for the configured year window, read-only."""
    lock = cache.client.lock(FS_LOCK_KEY, timeout=6 * 60 * 60, blocking=False)
    if not lock.acquire():
        status = cache.filesystem_status()
        return {"status": "busy", "pdfs": int(status.get("pdfs", 0) or 0)}

    anio_desde, anio_hasta = settings.filesystem_year_from, settings.filesystem_year_to
    years = [str(year) for year in range(int(anio_desde), int(anio_hasta) + 1)]
    started = datetime.now(timezone.utc).isoformat()
    started_epoch = time.time()
    root = settings.filesystem_base_dir
    total_dirs = 0
    total_tramite_dirs = 0
    total_pdfs = 0
    try:
        if not root.is_dir():
            raise RuntimeError(f"DIGI_BASE_DIR no existe o no es carpeta: {root}")
        cache.clear_filesystem()
        cache.client.hset(
            FS_META_KEY,
            mapping={
                "status": "counting",
                "started_at": started,
                "started_epoch": str(started_epoch),
                "loaded_directories": "0",
                "loaded_tramite_dirs": "0",
                "loaded_pdfs": "0",
                "target_directories": "0",
                "target_tramite_dirs": "0",
                "target_pdfs": "0",
                "elapsed_seconds": "0",
                "rate_pdfs_sec": "0",
                "root": str(root),
                "year_from": anio_desde,
                "year_to": anio_hasta,
            },
        )
        target_dirs = 0
        target_tramite_dirs = 0
        target_pdfs = 0
        for entry_type, relative_path, _entry in _filesystem_entries(root, years):
            if entry_type == "directory":
                target_dirs += 1
                parts = relative_path.split("/")
                if len(parts) >= 3 and re.fullmatch(r"\d+", parts[2]):
                    target_tramite_dirs += 1
            else:
                target_pdfs += 1
        cache.client.hset(
            FS_META_KEY,
            mapping={
                "status": "loading",
                "target_directories": str(target_dirs),
                "target_tramite_dirs": str(target_tramite_dirs),
                "target_pdfs": str(target_pdfs),
            },
        )
        total_dirs = 0
        total_tramite_dirs = 0
        total_pdfs = 0
        pipe = cache.client.pipeline(transaction=False)
        batch_count = 0
        for entry_type, relative_path, entry in _filesystem_entries(root, years):
            if entry_type == "directory":
                total_dirs += 1
                pipe.sadd(FS_DIRS_KEY, relative_path)
                parts = relative_path.split("/")
                if len(parts) >= 3 and re.fullmatch(r"\d+", parts[2]):
                    tramite = str(int(parts[2]))
                    member = f"{parts[0]}|{tramite}|{relative_path}"
                    pipe.zadd(FS_TRAMITE_DIRS_KEY, {member: 0})
                    total_tramite_dirs += 1
            else:
                parts = relative_path.split("/")
                year = parts[0] if parts else ""
                group = parts[1] if len(parts) > 1 else ""
                tramite = str(int(parts[2])) if len(parts) >= 3 and re.fullmatch(r"\d+", parts[2]) else ""
                try:
                    stat = entry.stat(follow_symlinks=False)
                    size = stat.st_size
                    modified_at = datetime.fromtimestamp(stat.st_mtime, timezone.utc).isoformat()
                except OSError:
                    size = None
                    modified_at = None
                record = {
                    "name": entry.name,
                    "relative_path": relative_path,
                    "year": year,
                    "group": group,
                    "tramite": tramite or None,
                    "size": size,
                    "modified_at": modified_at,
                }
                encoded = json.dumps(record, ensure_ascii=False, separators=(",", ":"))
                pipe.hset(FS_PDFS_KEY, relative_path, encoded)
                if tramite:
                    pipe.rpush(f"{FS_TRAMITE_PDFS_PREFIX}{year}:{tramite}", relative_path)
                total_pdfs += 1
            batch_count += 1
            if batch_count >= settings.filesystem_batch_size:
                pipe.execute()
                pipe = cache.client.pipeline(transaction=False)
                batch_count = 0
                cache.client.hset(
                    FS_META_KEY,
                    mapping={
                        "loaded_directories": str(total_dirs),
                        "loaded_tramite_dirs": str(total_tramite_dirs),
                        "loaded_pdfs": str(total_pdfs),
                        "elapsed_seconds": f"{max(0.001, time.time() - started_epoch):.1f}",
                        "rate_pdfs_sec": f"{total_pdfs / max(0.001, time.time() - started_epoch):.1f}",
                    },
                )
        if batch_count:
            pipe.execute()
        loaded_at = datetime.now(timezone.utc).isoformat()
        elapsed = max(0.001, time.time() - started_epoch)
        cache.client.hset(
            FS_META_KEY,
            mapping={
                "status": "ready",
                "root": str(root),
                "year_from": anio_desde,
                "year_to": anio_hasta,
                "directories": str(total_dirs),
                "tramite_dirs": str(total_tramite_dirs),
                "pdfs": str(total_pdfs),
                "loaded_directories": str(total_dirs),
                "loaded_tramite_dirs": str(total_tramite_dirs),
                "loaded_pdfs": str(total_pdfs),
                "target_directories": str(target_dirs),
                "target_tramite_dirs": str(target_tramite_dirs),
                "target_pdfs": str(target_pdfs),
                "elapsed_seconds": f"{elapsed:.1f}",
                "rate_pdfs_sec": f"{total_pdfs / elapsed:.1f}",
                "finished_at": loaded_at,
                "loaded_at": loaded_at,
            },
        )
        return {
            "status": "ready",
            "directories": total_dirs,
            "tramite_dirs": total_tramite_dirs,
            "pdfs": total_pdfs,
            "loaded_at": loaded_at,
            "elapsed_seconds": elapsed,
        }
    except Exception as exc:
        cache.client.hset(
            FS_META_KEY,
            mapping={
                "status": "error",
                "error": str(exc)[:500],
                "finished_at": datetime.now(timezone.utc).isoformat(),
                "elapsed_seconds": f"{max(0.0, time.time() - started_epoch):.1f}",
            },
        )
        raise
    finally:
        try:
            lock.release()
        except Exception:
            pass


def _filesystem_tramite_entries(root: Path, years: list[str], tramite: str):
    """Yield entries only from matching year/group/tramite folders."""
    for year in years:
        year_dir = root / year
        if not year_dir.is_dir():
            continue
        try:
            with os.scandir(year_dir) as groups:
                group_entries = list(groups)
        except OSError:
            continue
        for group_entry in group_entries:
            try:
                if not group_entry.is_dir(follow_symlinks=False):
                    continue
                tramite_dir = Path(group_entry.path) / tramite
                if not tramite_dir.is_dir():
                    continue
                relative_root = f"{year}/{group_entry.name}/{tramite}"
                stack: list[tuple[Path, str]] = [(tramite_dir, relative_root)]
                while stack:
                    current, current_relative = stack.pop()
                    yield "directory", current_relative, None
                    with os.scandir(current) as entries:
                        for entry in entries:
                            relative = f"{current_relative}/{entry.name}"
                            if entry.is_dir(follow_symlinks=False):
                                stack.append((Path(entry.path), relative))
                            elif (
                                entry.is_file(follow_symlinks=False)
                                and entry.name.lower().endswith(".pdf")
                            ):
                                yield "pdf", relative, entry
            except OSError:
                continue


def _filesystem_cached_tramite_entries(
    root: Path, cached_roots: list[str]
):
    """Yield entries from trámite roots already known by the Redis index."""
    for relative_root in cached_roots:
        tramite_dir = root / relative_root
        if not tramite_dir.is_dir():
            continue
        stack: list[tuple[Path, str]] = [(tramite_dir, relative_root)]
        while stack:
            current, current_relative = stack.pop()
            try:
                yield "directory", current_relative, None
                with os.scandir(current) as entries:
                    for entry in entries:
                        relative = f"{current_relative}/{entry.name}"
                        if entry.is_dir(follow_symlinks=False):
                            stack.append((Path(entry.path), relative))
                        elif (
                            entry.is_file(follow_symlinks=False)
                            and entry.name.lower().endswith(".pdf")
                        ):
                            yield "pdf", relative, entry
            except OSError:
                continue


def refresh_tramite_cache(
    tramite: int, anio_desde: str = CURRENT_YEAR, anio_hasta: str = CURRENT_YEAR
) -> dict[str, Any]:
    """Refresh one tramite in the Oracle snapshot and filesystem index."""
    anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
    tramite_s = str(int(tramite))
    oracle_result: dict[str, Any] = {"status": "not_ready"}
    full_lock = cache.client.lock(FULL_LOCK_KEY, timeout=15 * 60, blocking=False)
    if full_lock.acquire():
        try:
            if cache.full_ready():
                conn = oracle_connect()
                try:
                    cursor = conn.cursor()
                    cursor.execute(
                        f"SELECT * FROM {settings.oracle_owner}.{settings.oracle_table} "
                        "WHERE DIG_TRAMITE = ?",
                        [int(tramite)],
                    )
                    rows = _rows_as_dicts(cursor)
                finally:
                    conn.close()

                old_id = cache.client.hget(FULL_TRAMITE_KEY, tramite_s)
                old_year = ""
                if old_id is not None:
                    old_raw = cache.client.hget(FULL_ROWS_KEY, old_id)
                    old_row = json.loads(old_raw) if old_raw else {}
                    old_year = str(old_row.get("DIG_ANIO") or "").strip()

                in_window = [
                    row for row in rows
                    if re.fullmatch(r"\d{4}", str(row.get("DIG_ANIO") or "").strip())
                    and settings.full_year_from
                    <= str(row.get("DIG_ANIO") or "").strip()
                    <= settings.full_year_to
                ]
                if in_window:
                    row_map: dict[str, str] = {}
                    score_map: dict[str, float] = {}
                    year_scores: dict[str, dict[str, float]] = {}
                    new_tramite_id: str | None = None
                    for row in in_window:
                        dig_id = row.get("DIG_ID")
                        if dig_id is None:
                            continue
                        dig_id_s = str(int(dig_id))
                        row_map[dig_id_s] = json.dumps(
                            row, ensure_ascii=False, separators=(",", ":")
                        )
                        score_map[dig_id_s] = float(dig_id)
                        year = str(row.get("DIG_ANIO") or "").strip()
                        if re.fullmatch(r"\d{4}", year):
                            year_scores.setdefault(year, {})[dig_id_s] = float(dig_id)
                        new_tramite_id = dig_id_s
                    pipe = cache.client.pipeline(transaction=True)
                    if old_id is not None:
                        pipe.hdel(FULL_ROWS_KEY, old_id)
                        pipe.zrem(FULL_IDS_KEY, old_id)
                        if re.fullmatch(r"\d{4}", old_year):
                            pipe.zrem(f"{FULL_YEAR_PREFIX}{old_year}", old_id)
                        pipe.hdel(FULL_TRAMITE_KEY, tramite_s)
                    if row_map:
                        pipe.hset(FULL_ROWS_KEY, mapping=row_map)
                        pipe.zadd(FULL_IDS_KEY, score_map)
                        for year, scores in year_scores.items():
                            pipe.zadd(f"{FULL_YEAR_PREFIX}{year}", scores)
                        pipe.hset(FULL_TRAMITE_KEY, tramite_s, new_tramite_id)
                        pipe.execute()
                        oracle_result = {"status": "updated", "rows": len(row_map)}
                    else:
                        pipe.execute()
                        oracle_result = {"status": "removed", "rows": 0}
                else:
                    pipe = cache.client.pipeline(transaction=True)
                    if old_id is not None:
                        pipe.hdel(FULL_ROWS_KEY, old_id)
                        pipe.zrem(FULL_IDS_KEY, old_id)
                        if re.fullmatch(r"\d{4}", old_year):
                            pipe.zrem(f"{FULL_YEAR_PREFIX}{old_year}", old_id)
                        pipe.hdel(FULL_TRAMITE_KEY, tramite_s)
                    pipe.execute()
                    oracle_result = {"status": "removed", "rows": 0}
                cache.client.hset(
                    FULL_META_KEY,
                    mapping={
                        "last_partial_refresh_at": datetime.now(timezone.utc).isoformat(),
                        "last_partial_refresh_tramite": tramite_s,
                    },
                )
        finally:
            try:
                full_lock.release()
            except Exception:
                pass
    else:
        oracle_result = {"status": "busy"}

    filesystem_result: dict[str, Any] = {"status": "not_ready"}
    fs_lock = cache.client.lock(FS_LOCK_KEY, timeout=15 * 60, blocking=False)
    if fs_lock.acquire():
        try:
            fs_meta = cache.filesystem_status()
            if fs_meta.get("status") == "ready":
                loaded_from = int(fs_meta.get("year_from", settings.filesystem_year_from))
                loaded_to = int(fs_meta.get("year_to", settings.filesystem_year_to))
                selected_years = [
                    str(year)
                    for year in range(loaded_from, loaded_to + 1)
                    if anio_desde == "all"
                    or int(anio_desde) <= year <= int(anio_hasta)
                ]
                if selected_years:
                    old_pdf_paths: list[str] = []
                    old_folder_members: list[str] = []
                    for year in selected_years:
                        list_key = f"{FS_TRAMITE_PDFS_PREFIX}{year}:{tramite_s}"
                        old_pdf_paths.extend(cache.client.lrange(list_key, 0, -1))
                        prefix = f"{year}|{tramite_s}|"
                        old_folder_members.extend(
                            cache.client.zrangebylex(
                                FS_TRAMITE_DIRS_KEY,
                                f"[{prefix}", f"[{prefix}\xff",
                            )
                            )
                    total_folders = 0
                    total_pdfs = 0
                    folder_paths: list[tuple[str, str]] = []
                    pdf_records: list[tuple[str, str, str]] = []
                    cached_roots = []
                    for member in old_folder_members:
                        relative_path = member.split("|", 2)[-1]
                        parts = relative_path.split("/")
                        if (
                            len(parts) == 3
                            and parts[0] in selected_years
                            and parts[2] == tramite_s
                            and (settings.filesystem_base_dir / relative_path).is_dir()
                        ):
                            cached_roots.append(relative_path)
                    cached_roots = sorted(set(cached_roots))
                    if cached_roots:
                        entries = _filesystem_cached_tramite_entries(
                            settings.filesystem_base_dir, cached_roots
                        )
                    else:
                        entries = _filesystem_tramite_entries(
                            settings.filesystem_base_dir, selected_years, tramite_s
                        )
                    for entry_type, relative_path, entry in entries:
                        if entry_type == "directory":
                            total_folders += 1
                            parts = relative_path.split("/")
                            if len(parts) >= 3 and parts[2] == tramite_s:
                                folder_paths.append(
                                    (relative_path, f"{parts[0]}|{tramite_s}|{relative_path}")
                                )
                        else:
                            try:
                                stat = entry.stat(follow_symlinks=False)
                                size = stat.st_size
                                modified_at = datetime.fromtimestamp(
                                    stat.st_mtime, timezone.utc
                                ).isoformat()
                            except OSError:
                                size = None
                                modified_at = None
                            parts = relative_path.split("/")
                            record = {
                                "name": entry.name,
                                "relative_path": relative_path,
                                "year": parts[0] if parts else "",
                                "group": parts[1] if len(parts) > 1 else "",
                                "tramite": tramite_s,
                                "size": size,
                                "modified_at": modified_at,
                            }
                            pdf_records.append(
                                (
                                    relative_path,
                                    parts[0],
                                    json.dumps(record, ensure_ascii=False, separators=(",", ":")),
                                )
                            )
                            total_pdfs += 1

                    # Prepare the complete trámite snapshot first, then make
                    # the replacement visible with one Redis transaction.
                    # This preserves either the previous snapshot or the new
                    # one if the scan or a Redis command fails.
                    pipe = cache.client.pipeline(transaction=True)
                    if old_pdf_paths:
                        pipe.hdel(FS_PDFS_KEY, *old_pdf_paths)
                    if old_folder_members:
                        pipe.zrem(FS_TRAMITE_DIRS_KEY, *old_folder_members)
                        pipe.srem(
                            FS_DIRS_KEY,
                            *[member.split("|", 2)[2] for member in old_folder_members],
                        )
                    for year in selected_years:
                        pipe.delete(f"{FS_TRAMITE_PDFS_PREFIX}{year}:{tramite_s}")
                    for relative_path, member in folder_paths:
                        pipe.sadd(FS_DIRS_KEY, relative_path)
                        pipe.zadd(FS_TRAMITE_DIRS_KEY, {member: 0})
                    for relative_path, year, record_json in pdf_records:
                        pipe.hset(FS_PDFS_KEY, relative_path, record_json)
                        pipe.rpush(
                            f"{FS_TRAMITE_PDFS_PREFIX}{year}:{tramite_s}",
                            relative_path,
                        )
                    pipe.execute()
                    filesystem_result = {
                        "status": "updated",
                        "folders": total_folders,
                        "pdfs": total_pdfs,
                        "years": selected_years,
                    }
                    cache.client.hset(
                        FS_META_KEY,
                        mapping={
                            "directories": str(cache.client.scard(FS_DIRS_KEY)),
                            "tramite_dirs": str(cache.client.zcard(FS_TRAMITE_DIRS_KEY)),
                            "pdfs": str(cache.client.hlen(FS_PDFS_KEY)),
                            "loaded_directories": str(cache.client.scard(FS_DIRS_KEY)),
                            "loaded_tramite_dirs": str(cache.client.zcard(FS_TRAMITE_DIRS_KEY)),
                            "loaded_pdfs": str(cache.client.hlen(FS_PDFS_KEY)),
                            "target_directories": str(cache.client.scard(FS_DIRS_KEY)),
                            "target_tramite_dirs": str(cache.client.zcard(FS_TRAMITE_DIRS_KEY)),
                            "target_pdfs": str(cache.client.hlen(FS_PDFS_KEY)),
                            "last_partial_refresh_at": datetime.now(timezone.utc).isoformat(),
                            "last_partial_refresh_tramite": tramite_s,
                        },
                    )
                else:
                    filesystem_result = {"status": "range_not_loaded"}
            else:
                filesystem_result = {"status": "not_ready"}
        finally:
            try:
                fs_lock.release()
            except Exception:
                pass
    else:
        filesystem_result = {"status": "busy"}

    return {
        "status": "updated" if oracle_result.get("status") == "updated" and filesystem_result.get("status") == "updated" else "partial",
        "tramite": tramite_s,
        "oracle": oracle_result,
        "filesystem": filesystem_result,
        "anio_desde": anio_desde,
        "anio_hasta": anio_hasta,
    }


def _oracle_begin(conn: Any) -> None:
    """Disable JDBC autocommit when claiming/finalizing durable jobs."""
    jconn = getattr(conn, "jconn", None)
    if jconn is not None:
        try:
            jconn.setAutoCommit(False)
            return
        except Exception:
            pass
    try:
        conn.autocommit = False
    except Exception:
        pass


def _control_doc_sync_success(result: dict[str, Any]) -> bool:
    """Treat an Oracle row removal as success, but not a partial refresh."""
    oracle_status = str(result.get("oracle", {}).get("status") or "")
    filesystem_status = str(result.get("filesystem", {}).get("status") or "")
    return oracle_status in {"updated", "removed"} and filesystem_status == "updated"


def _control_doc_sync_error(
    *, tramite: int, anio: str, claimed_version: int, worker_id: str, error: str
) -> None:
    conn = oracle_connect()
    _oracle_begin(conn)
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"UPDATE {_control_doc_sync_table()} "
                "SET STATUS = 'ERROR', "
                "    LAST_ERROR = ?, "
                "    NEXT_RETRY_AT = SYSTIMESTAMP + NUMTODSINTERVAL("
                "        LEAST(300, POWER(2, LEAST(ATTEMPTS, 8))), 'SECOND') "
                "WHERE DIG_TRAMITE = ? AND DIG_ANIO = ? "
                "  AND CLAIMED_VERSION = ? AND WORKER_ID = ?",
                (str(error)[:2000], int(tramite), str(anio), int(claimed_version), str(worker_id)[:100]),
            )
        finally:
            cursor.close()
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass


def process_control_documental_pending(
    *, worker_id: str | None = None, stale_seconds: int = 900
) -> bool:
    """Claim and process one durable Control documental Redis refresh request."""
    worker_id = (worker_id or f"{socket.gethostname()}:{os.getpid()}")[:100]
    stale_seconds = max(60, int(stale_seconds))
    table = _control_doc_sync_table()

    conn = oracle_connect()
    _oracle_begin(conn)
    item: tuple[int, str, int] | None = None
    try:
        cursor = conn.cursor()
        try:
            cursor.execute(
                f"UPDATE {table} SET STATUS = 'ERROR', "
                "LAST_ERROR = SUBSTR(NVL(LAST_ERROR, 'Worker lease expired'), 1, 2000), "
                "NEXT_RETRY_AT = SYSTIMESTAMP "
                "WHERE STATUS = 'PROCESSING' "
                "  AND STARTED_AT < SYSTIMESTAMP - NUMTODSINTERVAL(?, 'SECOND')",
                (stale_seconds,),
            )
            conn.commit()

            cursor.execute(
                f"SELECT DIG_TRAMITE, DIG_ANIO, REQUEST_VERSION FROM {table} "
                "WHERE STATUS IN ('PENDING', 'ERROR') "
                "  AND REQUEST_VERSION > PROCESSED_VERSION "
                "  AND (NEXT_RETRY_AT IS NULL OR NEXT_RETRY_AT <= SYSTIMESTAMP) "
                "  AND ROWNUM <= 1 FOR UPDATE SKIP LOCKED"
            )
            row = cursor.fetchone()
            if not row:
                conn.rollback()
                return False

            item = (int(row[0]), str(row[1]), int(row[2]))
            cursor.execute(
                f"UPDATE {table} SET STATUS = 'PROCESSING', "
                "CLAIMED_VERSION = ?, STARTED_AT = SYSTIMESTAMP, "
                "WORKER_ID = ?, ATTEMPTS = ATTEMPTS + 1, LAST_ERROR = NULL "
                "WHERE DIG_TRAMITE = ? AND DIG_ANIO = ?",
                (item[2], worker_id, item[0], item[1]),
            )
        finally:
            cursor.close()
        conn.commit()
    except Exception:
        try:
            conn.rollback()
        except Exception:
            pass
        raise
    finally:
        try:
            conn.close()
        except Exception:
            pass

    assert item is not None
    tramite, anio, claimed_version = item
    try:
        result = refresh_tramite_cache(tramite, anio, anio)
        if not _control_doc_sync_success(result):
            raise RuntimeError(
                "Refresco incompleto: "
                + json.dumps(result, ensure_ascii=False, default=str)[:1800]
            )

        conn = oracle_connect()
        _oracle_begin(conn)
        try:
            cursor = conn.cursor()
            try:
                cursor.execute(
                    f"UPDATE {table} SET PROCESSED_VERSION = ?, "
                    "STATUS = CASE WHEN REQUEST_VERSION > ? THEN 'PENDING' ELSE 'DONE' END, "
                    "PROCESSED_AT = SYSTIMESTAMP, NEXT_RETRY_AT = NULL, LAST_ERROR = NULL "
                    "WHERE DIG_TRAMITE = ? AND DIG_ANIO = ? "
                    "  AND CLAIMED_VERSION = ? AND WORKER_ID = ?",
                    (claimed_version, claimed_version, tramite, anio, claimed_version, worker_id),
                )
            finally:
                cursor.close()
            conn.commit()
        except Exception:
            try:
                conn.rollback()
            except Exception:
                pass
            raise
        finally:
            try:
                conn.close()
            except Exception:
                pass
        return True
    except Exception as exc:
        try:
            _control_doc_sync_error(
                tramite=tramite,
                anio=anio,
                claimed_version=claimed_version,
                worker_id=worker_id,
                error=str(exc),
            )
        except Exception:
            # Keep the original refresh failure visible to the worker log.
            pass
        raise


def search_digitalizacion(
    *,
    q: str = "",
    procesado: str = "",
    revisada: str = "",
    fecha_desde: str = "",
    fecha_hasta: str = "",
    anio_desde: str = CURRENT_YEAR,
    anio_hasta: str = CURRENT_YEAR,
    page: int = 1,
    size: int = 25,
    force: bool = False,
) -> dict[str, Any]:
    page, size = _safe_page(page, size)
    anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
    params = {
        "q": q.strip(),
        "procesado": procesado.strip().upper(),
        "revisada": revisada.strip().upper(),
        "fecha_desde": fecha_desde.strip(),
        "fecha_hasta": fecha_hasta.strip(),
        "anio_desde": anio_desde,
        "anio_hasta": anio_hasta,
        "page": page,
        "size": size,
    }
    has_other_filters = any(params[key] for key in (
        "q", "procesado", "revisada", "fecha_desde", "fecha_hasta"
    ))
    if not has_other_filters:
        full_result = cache.full_page(page, size, anio_desde, anio_hasta)
        if full_result is not None:
            return full_result
    if params["q"].isdigit() and not any(params[key] for key in (
        "procesado", "revisada", "fecha_desde", "fecha_hasta"
    )):
        full_row = cache.full_tramite(int(params["q"]), anio_desde, anio_hasta)
        if full_row is not None:
            return {
                "items": [full_row],
                "total": 1,
                "page": 1,
                "size": size,
                "pages": 1,
                "cache": "redis-full",
                "generated_at": cache.full_status().get("loaded_at"),
                "anio_desde": anio_desde,
                "anio_hasta": anio_hasta,
            }
        if cache.full_covers_range(anio_desde, anio_hasta):
            return {
                "items": [],
                "total": 0,
                "page": 1,
                "size": size,
                "pages": 0,
                "cache": "redis-full",
                "generated_at": cache.full_status().get("loaded_at"),
            }
    key = _cache_key(params)
    if not force:
        cached = cache.get_json(key)
        if cached is not None:
            cached["cache"] = "redis"
            return cached

    where_sql, filter_params = build_filters(
        q=params["q"],
        procesado=params["procesado"],
        revisada=params["revisada"],
        fecha_desde=params["fecha_desde"],
        fecha_hasta=params["fecha_hasta"],
        anio_desde=anio_desde,
        anio_hasta=anio_hasta,
    )
    table_ref = f"{settings.oracle_owner}.{settings.oracle_table}"
    select_sql = ", ".join(DISPLAY_COLUMNS)
    offset = (page - 1) * size
    conn = oracle_connect()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT COUNT(*) FROM {table_ref}{where_sql}", filter_params)
        total = int(cursor.fetchone()[0] or 0)
        cursor.execute(
            f"SELECT {select_sql} FROM ("
            f"SELECT inner_q.*, ROW_NUMBER() OVER (ORDER BY DIG_ID DESC) AS rn "
            f"FROM (SELECT {select_sql} FROM {table_ref}{where_sql}) inner_q"
            f") WHERE rn > ? AND rn <= ?",
            filter_params + [offset, offset + size],
        )
        items = _rows_as_dicts(cursor)
    finally:
        conn.close()
    result = {
        "items": items,
        "total": total,
        "page": page,
        "size": size,
        "pages": (total + size - 1) // size if total else 0,
        "cache": "oracle",
        "generated_at": datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "anio_desde": anio_desde,
        "anio_hasta": anio_hasta,
    }
    cache.set_json(key, result, settings.cache_ttl)
    return result


def get_digitalizacion(dig_id: int, force: bool = False) -> dict[str, Any] | None:
    if not force:
        full_row = cache.full_row(dig_id)
        if full_row is not None:
            return {"item": full_row, "cache": "redis-full"}
    key = f"dig:v1:row:{dig_id}"
    if not force:
        cached = cache.get_json(key)
        if cached is not None:
            cached["cache"] = "redis"
            return cached
    table_ref = f"{settings.oracle_owner}.{settings.oracle_table}"
    conn = oracle_connect()
    try:
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {table_ref} WHERE DIG_ID = ?", [dig_id])
        rows = _rows_as_dicts(cursor)
    finally:
        conn.close()
    if not rows:
        return None
    result = {"item": rows[0], "cache": "oracle"}
    cache.set_json(key, result, settings.row_cache_ttl)
    return result


def refresh_search(payload: dict[str, Any]) -> dict[str, Any]:
    return search_digitalizacion(force=True, **payload)


def cache_info() -> dict[str, Any]:
    info = cache.client.info("memory")
    full = cache.full_status()
    filesystem = cache.filesystem_status()
    current_month = cache.client.hgetall(CURRENT_MONTH_META_KEY)
    return {
        "redis": "ok",
        "used_memory_human": info.get("used_memory_human"),
        "used_memory": info.get("used_memory"),
        "cache_ttl": settings.cache_ttl,
        "row_cache_ttl": settings.row_cache_ttl,
        "full_cache_status": full.get("status", "empty"),
        "full_cache_rows": int(full.get("total", full.get("loaded_rows", 0)) or 0),
        "full_cache_loaded_at": full.get("loaded_at"),
        "full_cache_year_from": full.get("year_from", settings.full_year_from),
        "full_cache_year_to": full.get("year_to", settings.full_year_to),
        "full_cache_progress": _progress_payload(
            full, "loaded_rows", "target_rows", "rate_rows_sec"
        ),
        "current_month": current_month.get("month"),
        "current_month_progress": _progress_payload(
            current_month, "loaded_rows", "target_rows", "rate_rows_sec"
        ),
        "filesystem_cache_status": filesystem.get("status", "empty"),
        "filesystem_cache_root": filesystem.get("root", str(settings.filesystem_base_dir)),
        "filesystem_cache_year_from": filesystem.get("year_from", settings.filesystem_year_from),
        "filesystem_cache_year_to": filesystem.get("year_to", settings.filesystem_year_to),
        "filesystem_cache_directories": int(filesystem.get("directories", filesystem.get("loaded_directories", 0)) or 0),
        "filesystem_cache_tramite_dirs": int(filesystem.get("tramite_dirs", filesystem.get("loaded_tramite_dirs", 0)) or 0),
        "filesystem_cache_pdfs": int(filesystem.get("pdfs", filesystem.get("loaded_pdfs", 0)) or 0),
        "filesystem_cache_loaded_at": filesystem.get("loaded_at"),
        "filesystem_cache_progress": {
            "status": filesystem.get("status", "empty"),
            "directories": _progress_payload(
                filesystem,
                "loaded_directories",
                "target_directories",
                "rate_pdfs_sec",
            ),
            "pdfs": _progress_payload(
                filesystem,
                "loaded_pdfs",
                "target_pdfs",
                "rate_pdfs_sec",
            ),
            "tramite_dirs": int(
                filesystem.get("tramite_dirs", filesystem.get("loaded_tramite_dirs", 0)) or 0
            ),
            "target_tramite_dirs": int(filesystem.get("target_tramite_dirs", 0) or 0),
        },
    }
