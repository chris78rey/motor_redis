from __future__ import annotations

import logging
import json
import os
import uuid
from pathlib import Path

from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from core import (
    CURRENT_YEAR,
    DISPLAY_COLUMNS,
    cache,
    cache_info,
    get_digitalizacion,
    search_digitalizacion,
    TRAMITE_JOB_STATUS_PREFIX,
    _normalize_year_range,
)


logging.basicConfig(level=os.environ.get("LOG_LEVEL", "INFO"))
logger = logging.getLogger("motor_redis")

app = FastAPI(title="Consulta DIGITALIZACION", version="1.0.0")
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))


@app.get("/", response_class=HTMLResponse)
def index(request: Request):
    return templates.TemplateResponse(
        request=request,
        name="index.html",
        context={"columns": DISPLAY_COLUMNS, "current_year": CURRENT_YEAR},
    )


@app.get("/health")
def health():
    try:
        cache.ping()
        return {"status": "ok", "redis": "ok"}
    except Exception as exc:
        logger.exception("Redis health check failed")
        return {"status": "degraded", "redis": "error", "detail": str(exc)}


@app.get("/api/cache/info")
def api_cache_info():
    try:
        return cache_info()
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Redis no disponible: {exc}") from exc


@app.get("/api/digitalizacion")
def api_search(
    q: str = Query(default="", max_length=120),
    procesado: str = Query(default="", pattern="^(|S|N|s|n)$"),
    revisada: str = Query(default="", pattern="^(|S|N|s|n)$"),
    fecha_desde: str = Query(default="", max_length=10),
    fecha_hasta: str = Query(default="", max_length=10),
    anio_desde: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    anio_hasta: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    page: int = Query(default=1, ge=1),
    size: int = Query(default=25, ge=1, le=100),
):
    try:
        return search_digitalizacion(
            q=q,
            procesado=procesado,
            revisada=revisada,
            fecha_desde=fecha_desde,
            fecha_hasta=fecha_hasta,
            anio_desde=anio_desde,
            anio_hasta=anio_hasta,
            page=page,
            size=size,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Parámetro inválido: {exc}") from exc
    except Exception as exc:
        logger.exception("DIGITALIZACION search failed")
        raise HTTPException(status_code=503, detail=f"No se pudo consultar DIGITALIZACION: {exc}") from exc


@app.get("/api/digitalizacion/{dig_id}")
def api_detail(
    dig_id: int,
    anio_desde: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    anio_hasta: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
):
    try:
        result = get_digitalizacion(dig_id)
        if result is not None:
            item = result.get("item", {})
            tramite = item.get("DIG_TRAMITE")
            if tramite is not None and str(tramite).isdigit():
                result["filesystem"] = cache.filesystem_for_tramite(
                    int(tramite), anio_desde, anio_hasta
                )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Parámetro inválido: {exc}") from exc
    except Exception as exc:
        logger.exception("DIGITALIZACION detail failed")
        raise HTTPException(status_code=503, detail=f"No se pudo consultar DIGITALIZACION: {exc}") from exc
    if result is None:
        raise HTTPException(status_code=404, detail="Registro no encontrado")
    return result


@app.get("/api/filesystem/tramite/{tramite}")
def api_filesystem_tramite(
    tramite: int,
    anio_desde: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    anio_hasta: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
):
    try:
        return cache.filesystem_for_tramite(tramite, anio_desde, anio_hasta)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Parámetro inválido: {exc}") from exc
    except Exception as exc:
        logger.exception("Filesystem lookup failed")
        raise HTTPException(status_code=503, detail=f"No se pudo consultar el filesystem: {exc}") from exc


@app.get("/api/filesystem/tramites/counts")
def api_filesystem_tramites_counts(
    tramites: str = Query(default="", max_length=10000),
    anio_desde: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    anio_hasta: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
):
    """Return PDF counts for several trámites in one Redis round trip."""
    try:
        values = [part.strip() for part in str(tramites or "").split(",")]
        if not values or any(not part.isdigit() or int(part) <= 0 for part in values):
            raise ValueError("La lista de trámites es inválida")
        if len(values) > 500:
            raise ValueError("Se permiten como máximo 500 trámites")
        return cache.filesystem_counts_for_tramites(
            [int(value) for value in values], anio_desde, anio_hasta
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Parámetro inválido: {exc}") from exc
    except Exception as exc:
        logger.exception("Filesystem counts lookup failed")
        raise HTTPException(status_code=503, detail=f"No se pudo consultar los conteos: {exc}") from exc


@app.post("/api/cache/tramite/{tramite}")
def api_refresh_tramite(
    tramite: int,
    anio_desde: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
    anio_hasta: str = Query(default=CURRENT_YEAR, pattern="^(all|todos|[0-9]{4})$"),
):
    """Queue a refresh for one tramite in both main indexes."""
    try:
        anio_desde, anio_hasta = _normalize_year_range(anio_desde, anio_hasta)
        job_id = uuid.uuid4().hex
        status_key = f"{TRAMITE_JOB_STATUS_PREFIX}{job_id}"
        cache.client.hset(
            status_key,
            mapping={"status": "queued", "tramite": str(tramite)},
        )
        cache.client.expire(status_key, 3600)
        cache.enqueue_priority(
            {
                "job": "warm_tramite",
                "tramite": tramite,
                "anio_desde": anio_desde,
                "anio_hasta": anio_hasta,
                "job_id": job_id,
            }
        )
        return {
            "status": "queued",
            "tramite": tramite,
            "job_id": job_id,
            "message": f"Actualización del trámite {tramite} encolada",
        }
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"Parámetro inválido: {exc}") from exc
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar la actualización: {exc}") from exc


@app.get("/api/cache/tramite/status/{job_id}")
def api_refresh_tramite_status(job_id: str):
    if not job_id or len(job_id) > 64 or not all(char in "0123456789abcdef" for char in job_id.lower()):
        raise HTTPException(status_code=400, detail="Identificador de tarea inválido")
    status = cache.client.hgetall(f"{TRAMITE_JOB_STATUS_PREFIX}{job_id}")
    if not status:
        raise HTTPException(status_code=404, detail="Tarea no encontrada o expirada")
    if status.get("result"):
        status["result"] = json.loads(status["result"])
    return status


@app.post("/api/cache/refresh")
def api_refresh():
    try:
        removed = cache.clear_search()
        cache.enqueue({"job": "warm_default"})
        return {"status": "queued", "removed_search_keys": removed}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar el refresco: {exc}") from exc


@app.post("/api/cache/current-month")
def api_current_month_refresh():
    """Queue a refresh limited to the current FE_PLA_ANIOMES value."""
    try:
        cache.enqueue({"job": "warm_current_month"})
        return {"status": "queued", "message": "Refresco del mes actual encolado"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar el refresco mensual: {exc}") from exc


@app.post("/api/cache/recent-days")
def api_recent_days_refresh(days: int = Query(7, ge=3, le=8)):
    """Queue a refresh for the rolling DIG_FECHA_PLANILLA window."""
    if os.environ.get("DIGI_RECENT_DAYS_ENABLED", "1").lower() in {"0", "false", "no"}:
        raise HTTPException(status_code=404, detail="Refresco por período deshabilitado")
    if days not in {3, 5, 7, 8}:
        raise HTTPException(status_code=400, detail="days debe ser 3, 5, 7 u 8")
    try:
        cache.enqueue({"job": "warm_recent_days", "days": days})
        return {"status": "queued", "days": days, "message": f"Refresco de los últimos {days} días encolado"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar el refresco: {exc}") from exc


@app.post("/api/cache/full-load")
def api_full_load():
    """Queue a complete snapshot load; the worker performs it in batches."""
    try:
        status = cache.client.hget("dig:v1:full:meta", "status") or "empty"
        if status == "loading":
            return {"status": "already_loading"}
        cache.enqueue({"job": "warm_all"})
        return {"status": "queued", "message": "Carga completa encolada"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar la carga completa: {exc}") from exc


@app.post("/api/cache/load-all")
def api_load_all_caches():
    """Queue one worker job that refreshes both main Redis collections."""
    try:
        full_status = cache.full_status().get("status")
        filesystem_status = cache.filesystem_status().get("status")
        if full_status == "loading" or filesystem_status in {"loading", "counting"}:
            return {"status": "already_loading"}
        cache.enqueue({"job": "warm_all_caches"})
        return {
            "status": "queued",
            "message": "Carga de ambas cachés encolada",
        }
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar la carga de ambas cachés: {exc}") from exc


@app.post("/api/cache/filesystem-load")
def api_filesystem_load():
    """Queue a read-only filesystem index load; the worker performs the scan."""
    try:
        status = cache.client.hget("dig:v1:fs:meta", "status") or "empty"
        if status == "loading":
            return {"status": "already_loading"}
        cache.enqueue({"job": "warm_filesystem"})
        return {"status": "queued", "message": "Índice de carpetas y PDFs encolado"}
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"No se pudo encolar el índice filesystem: {exc}") from exc
