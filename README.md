# motor_redis

Interfaz de consulta de solo lectura para `DIGITALIZACION`, con Oracle como fuente oficial y Redis como caché.

## Ejecutar en desarrollo

El `.env` ya debe estar en este directorio. El jar se resuelve también desde `../jdbc/ojdbc8 copy.jar` cuando `ORACLE_JDBC_JAR` usa la ruta relativa original.

```bash
/opt/digitalizacion_he1/vevdigi/bin/python -m pip install -r requirements.txt
cd /opt/digitalizacion_he1/motor_redis
export JAVA_TOOL_OPTIONS='-Doracle.jdbc.timezoneAsRegion=false -Duser.timezone=UTC'
/opt/digitalizacion_he1/vevdigi/bin/python -m uvicorn app:app --host 127.0.0.1 --port 8090
```

Para acceder desde otra computadora de la red, usar `--host 0.0.0.0` y abrir la IP LAN del servidor, por ejemplo `http://172.16.60.127:8090/`. No se debe exponer Redis; Redis permanece en `127.0.0.1:6379`.

```bash
/opt/digitalizacion_he1/vevdigi/bin/python -m uvicorn app:app --host 0.0.0.0 --port 8090
```

## Worker de refresco

En otra terminal/proceso:

```bash
cd /opt/digitalizacion_he1/motor_redis
export JAVA_TOOL_OPTIONS='-Doracle.jdbc.timezoneAsRegion=false -Duser.timezone=UTC'
/opt/digitalizacion_he1/vevdigi/bin/python worker.py
```

## Servicios persistentes

En este servidor quedaron instalados como servicios del usuario `red_gestion`:

```bash
systemctl --user status motor-redis-api.service motor-redis-worker.service
systemctl --user restart motor-redis-api.service motor-redis-worker.service
journalctl --user -u motor-redis-api.service -f
```

La API escucha en `0.0.0.0:8090` y el worker tiene sus propios límites de `512M` de RAM y `100%` de CPU. `loginctl` tiene `Linger=yes`, por lo que los servicios no dependen de mantener una terminal abierta.

El botón `Refrescar` elimina el caché de búsquedas y encola un calentamiento de la primera página del año actual. `Cargar ambas cachés` encola una tarea única que refresca la instantánea Oracle de los dos años más recientes y luego el índice filesystem en modo solo lectura. Este último recorre el árbol configurado en `DIGI_BASE_DIR` (por defecto solo el año actual), y guarda en Redis las rutas y metadatos de carpetas/PDFs, nunca sus binarios. La interfaz parte filtrando solo el año actual. Se puede indicar un rango mayor o marcar `Todos los años (Oracle)`; esos rangos se consultan en Oracle y quedan únicamente en caché temporal. Las consultas normales usan caché por 120 segundos y el detalle por 300 segundos. Se pueden cambiar con `DIGI_CACHE_TTL`, `DIGI_ROW_CACHE_TTL`, `DIGI_FULL_BATCH_SIZE` y `DIGI_FS_BATCH_SIZE`. Los años del índice filesystem se pueden ajustar con `DIGI_FS_YEAR_FROM` y `DIGI_FS_YEAR_TO`.

## API

```text
GET  /api/digitalizacion?q=...&anio_desde=2026&anio_hasta=2026&procesado=S&page=1&size=25
GET  /api/digitalizacion/{DIG_ID}
GET  /api/filesystem/tramite/{DIG_TRAMITE}?anio_desde=2026&anio_hasta=2026
GET  /api/cache/info
POST /api/cache/refresh
POST /api/cache/full-load
POST /api/cache/filesystem-load
POST /api/cache/load-all
POST /api/cache/tramite/{DIG_TRAMITE}?anio_desde=2026&anio_hasta=2026
GET  /api/cache/tramite/status/{job_id}
GET  /health
```

El estado de ambas colecciones se consulta en `/api/cache/info`; la pantalla actualiza barras de progreso cada dos segundos con porcentaje, velocidad, tiempo transcurrido y ETA. Oracle continúa siendo la fuente oficial y las instantáneas deben refrescarse cuando se necesite. El índice filesystem solo lee el repositorio: no modifica, mueve, renombra ni elimina archivos.

Desde el detalle de un registro, `Actualizar este trámite` refresca únicamente ese `DIG_TRAMITE` en la caché Oracle y en el índice filesystem. El worker elimina las rutas antiguas del trámite y vuelve a leer solo sus carpetas y PDFs.
