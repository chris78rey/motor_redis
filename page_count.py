from __future__ import annotations

import json
import itertools
import logging
import os
import re
import subprocess
import time
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

try:
    import pymupdf
except ImportError:  # pragma: no cover - fallback for older deployments
    pymupdf = None

from core import (
    FULL_ROWS_KEY,
    FS_TRAMITE_PDFS_PREFIX,
    PAGE_COUNT_DEFAULT_FROM,
    PAGE_COUNT_LOCK_KEY,
    PAGE_COUNT_META_KEY,
    PAGE_COUNT_MONTHS_KEY,
    cache,
    settings,
)


logger = logging.getLogger("motor_redis.page_count")
MONTH_RE = re.compile(r"^\d{6}$")
PAGE_COUNT_HISTORY_FILE = Path(
    os.environ.get("DIGI_PAGE_COUNT_HISTORY_FILE", str(Path(__file__).resolve().parent / "page_count_history.json"))
)


def _save_page_count_history(records: dict[str, str]) -> None:
    """Persist monthly totals as a recoverable, atomic JSON snapshot."""
    data = {month: json.loads(value) for month, value in sorted(records.items())}
    temporary = PAGE_COUNT_HISTORY_FILE.with_suffix(PAGE_COUNT_HISTORY_FILE.suffix + ".tmp")
    PAGE_COUNT_HISTORY_FILE.parent.mkdir(parents=True, exist_ok=True)
    temporary.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    temporary.replace(PAGE_COUNT_HISTORY_FILE)


def _month(value: str | None, default: str | None = None) -> str:
    result = str(value or default or "").strip()
    if not MONTH_RE.fullmatch(result):
        raise ValueError("El período debe tener formato YYYYMM")
    year, month = int(result[:4]), int(result[4:])
    if not 1 <= month <= 12:
        raise ValueError("El período debe tener un mes válido")
    return result


def _month_range(start: str, end: str) -> list[str]:
    start = _month(start)
    end = _month(end)
    if start > end:
        raise ValueError("El período inicial no puede ser mayor que el final")
    year, month = int(start[:4]), int(start[4:])
    end_year, end_month = int(end[:4]), int(end[4:])
    result: list[str] = []
    while (year, month) <= (end_year, end_month):
        result.append(f"{year:04d}{month:02d}")
        month += 1
        if month == 13:
            year += 1
            month = 1
    return result


def _default_end() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m")


def page_count_status() -> dict[str, Any]:
    meta = cache.client.hgetall(PAGE_COUNT_META_KEY)
    if not meta:
        return {"status": "empty"}
    result = dict(meta)
    for key in ("loaded_pdfs", "target_pdfs", "total_pages", "failed_pdfs"):
        if key in result:
            result[key] = int(result[key] or 0)
    for key in ("elapsed_seconds", "rate_pdfs_sec"):
        if key in result:
            result[key] = float(result[key] or 0)
    if result.get("status") in {"loading", "counting"}:
        started_epoch = float(result.get("started_epoch") or 0)
        elapsed = max(0.0, time.time() - started_epoch) if started_epoch else 0.0
        loaded = int(result.get("loaded_pdfs") or 0)
        target = int(result.get("target_pdfs") or 0)
        rate = loaded / elapsed if elapsed and loaded else 0.0
        result.update({
            "elapsed_seconds": round(elapsed, 1),
            "rate_pdfs_sec": round(rate, 1),
            "percent": round(loaded * 100.0 / target, 1) if target else 0.0,
            "eta_seconds": round((target - loaded) / rate, 1) if rate and target else None,
        })
    return result


def page_count_report(desde: str = PAGE_COUNT_DEFAULT_FROM, hasta: str | None = None) -> dict[str, Any]:
    hasta = _month(hasta, _default_end())
    months = _month_range(desde, hasta)
    raw = cache.client.hmget(PAGE_COUNT_MONTHS_KEY, months)
    items = []
    for month, encoded in zip(months, raw):
        if encoded:
            item = json.loads(encoded)
        else:
            item = {
                "fe_pla_aniomes": month,
                "hojas": 0,
                "pdfs": 0,
                "tramites": 0,
                "failed_pdfs": 0,
                "status": "pending",
            }
        items.append(item)
    total_pages = sum(int(item.get("hojas", 0) or 0) for item in items)
    return {
        "status": page_count_status().get("status", "empty"),
        "desde": months[0],
        "hasta": months[-1],
        "total_hojas": total_pages,
        "meses": items,
    }


def _count_pdf_pages(relative_path: str) -> tuple[str, int | None, str | None]:
    path = settings.filesystem_base_dir / relative_path
    try:
        resolved = path.resolve()
        root = settings.filesystem_base_dir.resolve()
        if resolved != root and root not in resolved.parents:
            return relative_path, None, "ruta fuera del repositorio configurado"
        if not resolved.is_file():
            return relative_path, None, "archivo no encontrado"
        if pymupdf is not None:
            document = None
            try:
                document = pymupdf.open(str(resolved))
                return relative_path, int(document.page_count), None
            finally:
                if document is not None:
                    document.close()
        completed = subprocess.run(
            ["pdfinfo", str(resolved)],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            timeout=120,
            check=False,
        )
        match = re.search(r"^Pages:\s*(\d+)\s*$", completed.stdout, re.MULTILINE)
        if completed.returncode != 0 or not match:
            detail = (completed.stderr or "no se pudo obtener Pages").strip()
            return relative_path, None, detail[:300]
        return relative_path, int(match.group(1)), None
    except Exception as exc:
        return relative_path, None, str(exc)[:300]


def calculate_page_counts(desde: str = PAGE_COUNT_DEFAULT_FROM, hasta: str | None = None) -> dict[str, Any]:
    """Read PDFs only and persist monthly page totals in Redis."""
    hasta = _month(hasta, _default_end())
    months = _month_range(desde, hasta)
    lock = cache.client.lock(PAGE_COUNT_LOCK_KEY, timeout=24 * 60 * 60, blocking=False)
    if not lock.acquire():
        return {"status": "busy", **page_count_status()}

    started = datetime.now(timezone.utc).isoformat()
    started_epoch = time.time()
    # PyMuPDF is fast but its document handles are not safe to share across
    # threads on this host; keep the default conservative. It can be tuned
    # explicitly after a controlled benchmark with DIGI_PAGE_COUNT_WORKERS.
    workers = min(32, max(1, int(os.environ.get("DIGI_PAGE_COUNT_WORKERS", "1"))))
    cache.client.hset(PAGE_COUNT_META_KEY, mapping={
        "status": "loading",
        "desde": months[0],
        "hasta": months[-1],
        "started_at": started,
        "started_epoch": str(started_epoch),
        "loaded_pdfs": "0",
        "target_pdfs": "0",
        "total_pages": "0",
        "failed_pdfs": "0",
        "workers": str(workers),
    })
    cache.client.hdel(PAGE_COUNT_META_KEY, "error")
    try:
        if cache.full_status().get("status") != "ready":
            raise RuntimeError("La caché completa de DIGITALIZACION aún no está lista")
        if cache.filesystem_status().get("status") != "ready":
            raise RuntimeError("La caché filesystem de PDFs aún no está lista")

        # Build the month -> tramite relation from the rows already held in Redis.
        month_tramites: dict[str, set[str]] = defaultdict(set)
        for _dig_id, raw in cache.client.hscan_iter(FULL_ROWS_KEY, count=1000):
            try:
                record = json.loads(raw)
            except json.JSONDecodeError:
                continue
            period = str(record.get("FE_PLA_ANIOMES") or "").strip()
            tramite = record.get("DIG_TRAMITE")
            if period not in months or tramite is None:
                continue
            month_tramites[period].add(str(int(tramite)))

        # Fetch only list lengths first. The actual paths are loaded one month at
        # a time below, so the worker never keeps all million paths in memory.
        requests: list[tuple[str, str]] = [
            (month, f"{FS_TRAMITE_PDFS_PREFIX}{month[:4]}:{tramite}")
            for month in months
            for tramite in sorted(month_tramites.get(month, set()))
        ]
        target = 0
        for offset in range(0, len(requests), 1000):
            batch = requests[offset:offset + 1000]
            pipe = cache.client.pipeline(transaction=False)
            for _month_name, key in batch:
                pipe.llen(key)
            target += sum(int(value or 0) for value in pipe.execute())

        cache.client.hset(PAGE_COUNT_META_KEY, mapping={"target_pdfs": str(target), "status": "counting"})
        month_pages = {month: 0 for month in months}
        month_pdfs = {month: 0 for month in months}
        month_errors = {month: 0 for month in months}
        errors: list[dict[str, str]] = []
        loaded = 0

        for month in months:
            month_requests = [
                (month, f"{FS_TRAMITE_PDFS_PREFIX}{month[:4]}:{tramite}")
                for tramite in sorted(month_tramites.get(month, set()))
            ]
            paths: list[str] = []
            for offset in range(0, len(month_requests), 1000):
                batch = month_requests[offset:offset + 1000]
                pipe = cache.client.pipeline(transaction=False)
                for _month_name, key in batch:
                    pipe.lrange(key, 0, -1)
                for values in pipe.execute():
                    paths.extend(dict.fromkeys(values))

            # Keep only a small batch of futures in memory. executor.map() over
            # nearly one million paths would otherwise enqueue all futures at once.
            path_iterator = iter(paths)
            with ThreadPoolExecutor(max_workers=workers) as executor:
                while True:
                    batch_paths = list(itertools.islice(path_iterator, 500))
                    if not batch_paths:
                        break
                    for relative_path, pages, error in executor.map(_count_pdf_pages, batch_paths):
                        loaded += 1
                        if pages is None:
                            month_errors[month] += 1
                            if len(errors) < 100:
                                errors.append({"path": relative_path, "error": error or "error desconocido"})
                        else:
                            month_pages[month] += pages
                            month_pdfs[month] += 1
                        if loaded % 250 == 0 or loaded == target:
                            elapsed = max(0.001, time.time() - started_epoch)
                            cache.client.hset(PAGE_COUNT_META_KEY, mapping={
                                "loaded_pdfs": str(loaded),
                                "elapsed_seconds": f"{elapsed:.1f}",
                                "rate_pdfs_sec": f"{loaded / elapsed:.1f}",
                                "total_pages": str(sum(month_pages.values())),
                                "failed_pdfs": str(sum(month_errors.values())),
                            })
            del paths

        month_records = {
            month: json.dumps({
                "fe_pla_aniomes": month,
                "hojas": month_pages[month],
                "pdfs": month_pdfs[month],
                "tramites": len(month_tramites.get(month, set())),
                "failed_pdfs": month_errors[month],
                "status": "ready" if not month_errors[month] else "ready_with_errors",
                "calculated_at": datetime.now(timezone.utc).isoformat(),
            }, ensure_ascii=False, separators=(",", ":"))
            for month in months
        }
        cache.client.hset(PAGE_COUNT_MONTHS_KEY, mapping=month_records)
        # Keep a durable snapshot in addition to Redis. This is updated only
        # after all requested months have been counted successfully enough to
        # produce records, and is safe to replace atomically.
        existing = {
            month: encoded
            for month, encoded in zip(
                cache.client.hkeys(PAGE_COUNT_MONTHS_KEY),
                cache.client.hmget(PAGE_COUNT_MONTHS_KEY, cache.client.hkeys(PAGE_COUNT_MONTHS_KEY)),
            )
            if encoded
        }
        _save_page_count_history(existing)
        total_pages = sum(month_pages.values())
        failed = sum(month_errors.values())
        finished = datetime.now(timezone.utc).isoformat()
        cache.client.hset(PAGE_COUNT_META_KEY, mapping={
            "status": "ready" if not failed else "ready_with_errors",
            "loaded_pdfs": str(loaded),
            "target_pdfs": str(target),
            "total_pages": str(total_pages),
            "failed_pdfs": str(failed),
            "errors": json.dumps(errors, ensure_ascii=False, separators=(",", ":")),
            "finished_at": finished,
            "elapsed_seconds": f"{time.time() - started_epoch:.1f}",
        })
        return {"status": "ready" if not failed else "ready_with_errors", "total_pages": total_pages, "failed_pdfs": failed}
    except Exception as exc:
        cache.client.hset(PAGE_COUNT_META_KEY, mapping={
            "status": "error",
            "error": str(exc)[:500],
            "finished_at": datetime.now(timezone.utc).isoformat(),
        })
        raise
    finally:
        try:
            lock.release()
        except Exception:
            pass
