# Hoja de ruta

Hoja de ruta viva para `media-catalog-movies`.

El objetivo es consolidar una aplicación local fiable para catalogar películas, enriquecer fichas con modelos locales y fuentes externas, revisar manualmente los casos dudosos y exportar el catálogo sin poner en riesgo la base DuckDB local.

## Estado actual

Versión preparada: `0.2.0`.

Arquitectura actual:

```text
Usuario
  -> Streamlit
      -> FastAPI
          -> DuckDB
          -> Ollama
          -> IMDb / OMDb
```

Fortalezas actuales:

- Separación práctica entre frontend Streamlit y backend FastAPI.
- Tabla canónica `items` para revisión comercial, desacoplada de las tablas fuente del pipeline.
- Catálogo jerárquico de secciones de cine cargado desde `data/secciones.csv`.
- Pipeline LangGraph con etapas acotables y reintentos.
- DuckDB local con esquema normalizado por áreas: core, extraction, imdb, omdb y workflow.
- Vista `movies` como contrato compatible para API, UI y exportación.
- Estado de workflow persistido con intentos, errores, revisión e historial.

Limitaciones actuales:

- Ya existe gestión de snapshots externos con publicación, importación verificada, backup local y limpieza por retención.
- La migración `0001_baseline` aplica el esquema inicial; quedan pendientes migraciones evolutivas y pruebas legacy.
- Ya existe CI básico de lint/tests; queda pendiente elegir lockfile de dependencias.
- La API no tiene autenticación y está pensada para uso local.
- `src/backend/services/movies.py` aún concentra consultas, migración suave y reglas de workflow, aunque el arranque ya expone `ensure_schema`.
- La exportación Importamatic ya vive sobre la vista DuckDB `export` y escribe artefactos bajo `data/exports`.

## Principios de trabajo

- Datos primero: ningún cambio debe poner en riesgo `data/movies.duckdb`.
- Uso local antes que multiusuario.
- Cambios pequeños, probados y reversibles.
- Versión única en `pyproject.toml`.
- Documentación operativa para instalación, datos, backups, snapshots y releases.

## Hitos

### M0. Alineamiento base con `media-catalog-vinyls`

Objetivo: poner suelo de proyecto mantenible antes de cambios más grandes.

Tareas:

- [x] `P0` Centralizar versión en `pyproject.toml` y leerla desde backend/frontend.
- [x] `P0` Añadir `CHANGELOG.md`.
- [x] `P0` Añadir `ROADMAP.md`.
- [x] `P0` Añadir tests mínimos de metadatos y arranque API.
- [x] `P0` Añadir targets `make lint` y `make test`.
- [x] `P0` Ajustar `.gitignore` para datos locales, backups DuckDB y artefactos Python.
- [x] `P1` Revisar si se deben desversionar artefactos ya trackeados como `.egg-info`.

### M1. Datos y snapshots

Objetivo: replicar el patrón seguro de `media-catalog-vinyls`.

Tareas:

- [x] `P1` Crear `BBDD_DIR=../bbdd` y derivar `media-catalog-movies/snapshots` desde esa carpeta base.
- [x] `P1` Crear servicio `src/backend/services/snapshots.py` adaptado a `movies.duckdb`.
- [x] `P1` Crear endpoints `/snapshots/status`, `/snapshots`, `/snapshots/publish`, `/snapshots/import` y `/snapshots/cleanup`.
- [x] `P1` Crear `scripts/snapshots.py`.
- [x] `P1` Añadir pantalla Streamlit `Datos`.
- [x] `P1` Crear backup local obligatorio antes de importar snapshots.
- [x] `P2` Añadir tests de snapshot válido, incompleto y corrupto.

### M2. Formulario, mantenimiento y exportación

Tareas:

- [x] `P0` Crear la tabla canónica `items` sin campo `formato`.
- [x] `P0` Preparar nuevas fichas sin sobrescribir ediciones manuales.
- [x] `P0` Cargar estados permitidos y secciones desde `data/secciones.csv`.
- [x] `P0` Exponer API para preparar, consultar y editar fichas.
- [x] `P0` Crear el formulario único de revisión comercial al estilo de vinyls.
- [x] `P0` Crear la vista `export` con la plantilla Importamatic de Todocolección.
- [x] `P0` Generar HTML en la descripción para datos sin columna propia.
- [x] `P1` Añadir `scripts/db_maintenance.py`.
- [x] `P1` Añadir `make db-maint`, `make db-repack` y `make db-repack-replace`.
- [x] `P1` Centralizar `EXPORTS_DIR=data/exports`.
- [x] `P1` Validar fichas antes de exportar y bloquear CSV con errores de datos.
- [x] `P1` Homogeneizar el selector de secciones TC con helper local independiente por repo.
- [x] `P2` Descartar la exportación TSV como flujo principal y mantener la salida Importamatic CSV bajo `data/exports`.

### M3. Migraciones y seguridad de esquema

Tareas:

- [x] `P1` Crear tabla `schema_migrations`.
- [x] `P1` Hacer que `0001_baseline` aplique la creación y sincronización inicial del esquema.
- [ ] `P1` Extraer futuras migraciones evolutivas a pasos numerados específicos.
- [x] `P1` Crear `make migrate-db`.
- [ ] `P1` Probar migración desde una DB legacy simulada.

### M4. Modularización backend

Tareas:

- [~] `P2` Extraer schema management desde `movies.py`; `ensure_schema` ya permite reutilizar el camino desde migraciones y arranque.
- [x] `P2` Extraer construcción de secciones TC a un helper local independiente.
- [x] `P2` Añadir tests unitarios del helper de secciones TC.
- [~] `P2` Extraer repositorios por tabla; `items_repo` ya cubre la tabla comercial.
- [x] `P2` Separar rutas FastAPI por dominio para fichas, exportación y snapshots.
- [ ] `P2` Añadir tests unitarios de normalizadores, multi-value y derivación de etapa.

### M5. Calidad y releases

Tareas:

- [x] `P1` Crear CI para lint y tests.
- [x] `P1` Añadir lanzadores multiplataforma en `tools/`.
- [ ] `P1` Elegir lockfile (`uv`, `pip-tools` u otra herramienta).
- [ ] `P2` Crear docs de proceso de release.
- [ ] `P2` Documentar rollback y recuperación de datos.

## Riesgos

| Riesgo | Impacto | Mitigación |
| --- | --- | --- |
| Importar o migrar DB rompe `data/movies.duckdb` | Alto | Backup previo, instantáneas verificadas y tests de migración |
| Dependencias externas cambian o fallan | Medio | Tests con mocks y lockfile |
| Modelos Ollama no disponibles | Medio | Diagnóstico visible y errores claros |
| API expuesta fuera de localhost | Alto | Documentación, bind local y token interno futuro |
| `movies.py` crece demasiado | Medio | Separar schema, repositorios y casos de uso |

## Orden inmediato

1. Extraer repositorios por tabla y reducir `movies.py`.
2. Documentar rollback, recuperación de datos y proceso de release.
3. Revisar lockfile de dependencias.
4. Preparar tests de migración desde una base legacy simulada.
5. Diseñar migraciones evolutivas para próximos cambios de esquema.
