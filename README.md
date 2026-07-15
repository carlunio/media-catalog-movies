# media-catalog-movies

Aplicación local para catalogar películas con `FastAPI`, `Streamlit`, `DuckDB`, `Ollama`, OMDb, IMDb y un workflow orquestado con LangGraph.

Puedes consultar:

- Historial de cambios: [`CHANGELOG.md`](./CHANGELOG.md)
- Hoja de ruta: [`ROADMAP.md`](./ROADMAP.md)

## Requisitos

- Python 3.12+
- Ollama en local para las fases de visión y traducción
- Una clave de OMDb para descargar metadatos desde OMDb

## Configuración

1. Crea tu entorno a partir del ejemplo:

```bash
cp .env.example .env
```

2. Ajusta al menos `OMDB_API_KEY` y, si hace falta, los modelos de Ollama.

## Puesta en marcha

```bash
make setup
make dev
```

Servicios por defecto:

- Backend: `http://127.0.0.1:8000`
- Frontend: `http://127.0.0.1:8501`

## Lanzadores

La carpeta [`tools/`](./tools/README.md) incluye lanzadores de doble clic para preparar, arrancar, detener y actualizar la aplicación en Windows y Ubuntu/Linux.

## Comandos útiles

```bash
make dev
make stop
make restart
make update-repo
make update
make db-maint
make db-repack
make db-repack-replace
make publish-snapshot
make list-snapshots
make import-snapshot SNAPSHOT_ID=<id>
make cleanup-snapshots
make migrate-db
make lint
make format
make test
```

## Pipeline

1. Lectura de carátulas en DuckDB.
2. Ejecución del workflow LangGraph: `extraction -> imdb -> title_es -> omdb -> translation`.
3. Marcado automático de registros que necesitan revisión manual.
4. Título: revisión y corrección con portada visible.
5. Revisión o ajuste manual de IMDb, OMDb y sinopsis traducida.
6. Preparación de fichas comerciales en `items`, con `ALTA` y `En stock` como valores iniciales.
7. Formulario comercial y exportación del catálogo.
8. Publicación, importación y limpieza de snapshots de la base local.

## Modelo de datos

La base activa de la aplicación es `data/movies.duckdb`.
Las rutas internas de carátulas se guardan relativas al proyecto, por ejemplo `data/input/P0001.jpeg`; los snapshots y backups externos son independientes y usan `BBDD_DIR`.

El esquema interno está normalizado por áreas:

- `movies_core`: identidad, rutas de carátulas y fechas.
- `movie_extraction`: título/equipo extraídos y ajustes manuales.
- `movie_imdb`: búsqueda IMDb y títulos en español.
- `movie_omdb`: payload y campos de OMDb, incluida la traducción de la sinopsis.
- `movie_workflow`: estado, intentos, errores e historial del workflow.
- Vista `movies`: contrato de lectura del pipeline y de los datos enriquecidos.
- `items`: tabla canónica de fichas comerciales editables; la preparación solo añade películas nuevas y no sobrescribe cambios manuales.
- `inventory_field_allowed_values`: opciones controladas para estado de carga, stock y estado de Todocolección.
- `tc_sections`: árbol de secciones construido desde `data/secciones.csv`, con una raíz común de cine y televisión.

## API de fichas comerciales

- `POST /items/prepare`: crea fichas para las películas que todavía no existen en `items`.
- `GET /items`: lista resumida de fichas.
- `GET /items/options`: valores permitidos y árbol de secciones de cine.
- `GET /items/{item_id}`: devuelve una ficha completa.
- `PUT /items/{item_id}`: guarda cambios manuales sin modificar las tablas fuente.

El catálogo comercial no incluye un campo `formato`: la sección de Todocolección seleccionada define esa clasificación.

## Exportación a Todocolección

La vista DuckDB `export` reproduce la plantilla `data/plantilla_importamatic_otros.csv`. Solo incluye fichas cuya operación sea `ALTA`, `CAMBIO` o `BAJA`.

- `GET /export/movies/preview`: devuelve las filas preparadas para Importamatic.
- `POST /export/movies/csv`: genera un CSV UTF-8 separado por `#` para los identificadores seleccionados.
- `GET /export/movies/file`: descarga un CSV generado.
- `POST /export/movies/covers`: copia las carátulas seleccionadas a `data/exports/covers` con el nombre usado en el CSV.
- `POST /omdb/covers/download`: descarga los pósteres de OMDb como segunda imagen en `data/output/covers/<id>_2.jpg`.
- `POST /export/movies/clear-operation`: vacía la operación después de una exportación confirmada.

La columna `IMAGEN 2` usa la convención `<id>_2.jpg`, alineada con la descarga de pósteres de OMDb. La descripción se genera como HTML con los datos cinematográficos que no tienen columna propia en Importamatic: título original, año, tipo, dirección, guion, reparto, duración, géneros, país, idiomas, sinopsis, premios, productora e información de IMDb.

## Copias, snapshots y mantenimiento

La aplicación trabaja contra una DuckDB local en `DB_PATH`. Para compartir o recuperar datos no se debe usar una DuckDB viva en una carpeta sincronizada; se publican snapshots verificados en `BBDD_DIR/media-catalog-movies/snapshots`.

- `make db-maint`: ejecuta `CHECKPOINT` y `VACUUM` sobre la base local.
- `make db-repack`: crea una copia compactada `<db>.repacked.duckdb`.
- `make db-repack-replace`: sustituye la base local por la copia compactada y conserva un backup previo.
- `make publish-snapshot`: publica un snapshot reempaquetado con manifiesto JSON y hash `sha256`.
- `make list-snapshots`: lista snapshots disponibles.
- `make import-snapshot SNAPSHOT_ID=<id>`: importa un snapshot verificado y crea antes un backup local en `data/backups/local`.
- `make cleanup-snapshots`: limpia snapshots antiguos respetando retención y mínimos configurados.
- `make migrate-db`: prepara el esquema actual y registra migraciones pendientes en `schema_migrations`.

La pantalla `Datos` permite publicar, importar, listar y limpiar snapshots desde Streamlit.

## API del workflow

- `POST /workflow/run`: ejecuta el pipeline para una película o lote.
- `GET /workflow/graph`: metadatos del grafo usados por Streamlit.
- `GET /workflow/snapshot`: conteos por etapa/estado y cola de revisión.
- `POST /workflow/review/{movie_id}`: aprueba o reintenta desde una fase.
- `POST /workflow/review/{movie_id}/mark`: marca una película para revisión.
- Endpoints heredados como `/extract/run`, `/imdb/search`, `/omdb/fetch` y `/plot/translate` se mantienen mapeados a ejecuciones acotadas de LangGraph.

## Variables de entorno

- `APP_CHANNEL`: canal opcional mostrado en UI, por ejemplo `dev`.
- `DB_PATH`: ruta del fichero DuckDB.
- `COVERS_DIR`: carpeta relativa del repo para leer carátulas; por defecto `data/input`. Las rutas de carátulas se guardan relativas al proyecto.
- `EXPORTS_DIR`: carpeta reservada para los archivos de exportación.
- `TC_SECTIONS_CSV_PATH`: CSV que define las secciones disponibles de cine.
- `IMPORTAMATIC_OTHERS_FIXED_COST`: gastos fijos usados por la plantilla Importamatic de Otros.
- `BBDD_DIR`: carpeta base externa donde se crea `media-catalog-movies/snapshots` para publicar snapshots.
- `SYNC_STATE_PATH`: archivo local con el último snapshot publicado o importado.
- `SYNC_ACTOR`: persona que publica o importa snapshots.
- `SYNC_DEVICE`: equipo desde el que se publica o importa.
- `SYNC_RETENTION_DAYS`: días de retención para snapshots antiguos.
- `SYNC_KEEP_MIN`: mínimo de snapshots que se conservan siempre.
- `OMDB_API_KEY`: clave necesaria para OMDb.
- `OMDB_PLOT_MODE`: `full` o `short`.
- `VISION_TITLE_MODEL`: modelo Ollama para extraer título desde portada.
- `VISION_TEAM_MODEL`: modelo Ollama para extraer equipo desde portada.
- `TRANSLATION_MODEL`: modelo Ollama para traducir la sinopsis.
- `IMDB_MAX_RESULTS`: máximo de resultados revisados por intento IMDb.
- `IMDB_SLEEP_SECONDS`: espera entre películas durante búsqueda batch.
- `REQUEST_TIMEOUT_SECONDS`: timeout HTTP del backend.
- `WORKFLOW_MAX_ATTEMPTS`: reintentos automáticos antes de revisión.
- `API_URL`: backend objetivo del frontend.
- `API_TIMEOUT_SECONDS`: timeout normal frontend -> backend.
- `API_LONG_TIMEOUT_SECONDS`: timeout para trabajos largos.

## Versionado

- La versión vive en `pyproject.toml`.
- Backend y frontend leen esa versión mediante `src/project_meta.py`.
- El historial se documenta en `CHANGELOG.md`.
