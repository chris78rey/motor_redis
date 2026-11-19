from __future__ import annotations

import json
import logging
import os
import signal
import time

from core import (
    CURRENT_YEAR,
    cache,
    load_filesystem_cache,
    load_full_cache,
    refresh_current_month_cache,
    refresh_recent_days_cache,
    process_control_documental_pending,
    refresh_search,
    refresh_tramite_cache,
    TRAMITE_JOB_STATUS_PREFIX,
)
from page_count import calculate_page_counts


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("motor_redis.worker")
running = True


def stop(*_args) -> None:
    global running
    running = False


signal.signal(signal.SIGTERM, stop)
signal.signal(signal.SIGINT, stop)


def handle(payload: dict) -> None:
    if payload.get("job") == "calculate_page_counts":
        result = calculate_page_counts(payload.get("desde", "202602"), payload.get("hasta"))
        logger.info("Monthly PDF page count finished: %s", result)
        return
    if payload.get("job") == "warm_current_month":
        result = refresh_current_month_cache()
        logger.info("Current month Redis refresh finished: %s", result)
        return
    if payload.get("job") == "warm_recent_days":
        result = refresh_recent_days_cache(int(payload.get("days", 7)))
        logger.info("Recent days Redis refresh finished: %s", result)
        return
    if payload.get("job") == "warm_tramite":
        job_id = payload.get("job_id")
        status_key = f"{TRAMITE_JOB_STATUS_PREFIX}{job_id}" if job_id else None
        if status_key:
            cache.client.hset(status_key, mapping={"status": "loading"})
            cache.client.expire(status_key, 3600)
        try:
            result = refresh_tramite_cache(
                int(payload["tramite"]),
                payload.get("anio_desde", CURRENT_YEAR),
                payload.get("anio_hasta", CURRENT_YEAR),
            )
            if status_key:
                cache.client.hset(
                    status_key,
                    mapping={"status": "ready", "result": json.dumps(result, ensure_ascii=False)},
                )
            logger.info("Tramite cache refresh finished: %s", result)
        except Exception as exc:
            if status_key:
                cache.client.hset(status_key, mapping={"status": "error", "error": str(exc)[:500]})
            raise
        return
    if payload.get("job") == "warm_all_caches":
        try:
            result = load_full_cache()
            logger.info(
                "Combined Oracle cache load finished: %s rows in %.1fs",
                result.get("total", result.get("loaded_rows")),
                result.get("elapsed_seconds", 0),
            )
        except Exception:
            logger.exception("Combined load: Oracle cache failed")
        try:
            result = load_filesystem_cache()
            logger.info(
                "Combined filesystem cache load finished: %s directories / %s PDFs in %.1fs",
                result.get("directories"),
                result.get("pdfs"),
                result.get("elapsed_seconds", 0),
            )
        except Exception:
            logger.exception("Combined load: filesystem cache failed")
        return
    if payload.get("job") == "warm_default":
        result = refresh_search({"q": "", "procesado": "", "revisada": "", "fecha_desde": "", "fecha_hasta": "", "anio_desde": CURRENT_YEAR, "anio_hasta": CURRENT_YEAR, "page": 1, "size": 25})
        logger.info("Default cache warmed: %s rows / %s total", len(result["items"]), result["total"])
        return
    if payload.get("job") == "warm_all":
        result = load_full_cache()
        logger.info(
            "Full cache load finished: %s rows in %.1fs",
            result.get("total", result.get("loaded_rows")),
            result.get("elapsed_seconds", 0),
        )
        return
    if payload.get("job") == "warm_filesystem":
        result = load_filesystem_cache()
        logger.info(
            "Filesystem cache load finished: %s directories / %s PDFs in %.1fs",
            result.get("directories"),
            result.get("pdfs"),
            result.get("elapsed_seconds", 0),
        )
        return
    logger.warning("Unknown job: %s", payload)


def main() -> None:
    logger.info("DIGITALIZACION cache worker started")
    priority_only = os.environ.get("DIGI_PRIORITY_ONLY", "0").lower() in {"1", "true", "yes"}
    # True cuando la ultima vuelta proceso una solicitud durable de Control
    # documental: probablemente hay mas en cola y no conviene esperar 5 s.
    backlog = False
    while running:
        if priority_only:
            item = cache.client.brpop("dig:v1:jobs:priority", timeout=5)
        else:
            # BRPOP revisa las claves en orden: primero la cola prioritaria.
            # Antes se usaba RPOP, que devuelve solo el valor (no el par
            # clave/valor); item[1] tomaba un caracter y la tarea se perdia.
            item = cache.client.brpop(
                ["dig:v1:jobs:priority", "dig:v1:jobs"],
                timeout=1 if backlog else 5,
            )
        if item:
            try:
                handle(json.loads(item[1]))
            except Exception:
                logger.exception("Cache job failed; waiting for the next job")
                time.sleep(1)
        try:
            backlog = bool(process_control_documental_pending())
        except Exception:
            backlog = False
            logger.exception("Control documental Redis sync failed; waiting for the next job")
            time.sleep(1)
    logger.info("DIGITALIZACION cache worker stopped")


if __name__ == "__main__":
    main()
