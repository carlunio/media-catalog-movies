## [Sin publicar]

- Se corrige una regresión donde la normalización de textos había cambiado identificadores técnicos `poster`/`omdb_poster` a formas con tilde, impidiendo arrancar el backend contra la base real.
- Se revisan textos de documentación, UI, prompts y mensajes para usar español correcto con tildes y eñes, manteniendo intactos los identificadores técnicos.
### Añadido
- Se usa `assets/dicts.json` para traducir campos estructurados de OMDb y se añade el campo comercial `Tipo`.
- Se añade la descarga de pósteres OMDb como segunda imagen (`data/output/covers/<id>_2.jpg`) y la vista `export` rellena `IMAGEN 2` con esa convención.
- Se añade CI básico en GitHub Actions para ejecutar `make lint` y `make test`.
- Se añade una migración baseline `0001_baseline`, tabla `schema_migrations` y target `make migrate-db`.
- Se añaden lanzadores multiplataforma en `tools/` para Windows y Ubuntu/Linux.
- Se añade validación backend/frontend antes de exportar a Importamatic, bloqueando CSV con fichas incompletas.
- Se añade un helper local de selector jerárquico de secciones TC, independiente del repo de vinilos.
- Se separan routers FastAPI para fichas comerciales, exportación y snapshots, dejando `main.py` centrado en arranque y pipeline.
- La migración `0001_baseline` pasa a aplicar el esquema inicial mediante `ensure_schema`, compartiendo el mismo camino que usa el arranque del backend.
- Se añaden tests unitarios para el helper de secciones TC y para la ejecución idempotente de la migración baseline sin importar `main.py`.
- Se añade `src/backend/repositories/items_repo.py` para separar el acceso a datos de la tabla `items` del servicio de catálogo.
- Se deja de versionar el artefacto generado `src/media_catalog_movies.egg-info` y se homogeneiza el quoting interno del `Makefile`.
- Se añade gestión de snapshots externos para `movies.duckdb`: publicación, listado, importación con backup local, limpieza por retención y pantalla `Datos`.
- La carpeta externa de snapshots pasa a derivarse de `BBDD_DIR`, creando debajo `media-catalog-movies/snapshots`.
- Se añaden scripts y targets `make` para mantenimiento DuckDB, repack y snapshots.
- Se crea `items` como tabla canónica de fichas comerciales, separada de las tablas del pipeline.
- Se añade una preparación idempotente que asigna `ALTA` y `En stock` a las fichas nuevas sin sobrescribir ediciones manuales.
- Se incorporan `inventory_field_allowed_values` y la tabla `tc_sections`, construida desde `data/secciones.csv`.
- Se añaden endpoints para preparar, listar, consultar y editar fichas comerciales.
- Se añade la vista `export` y la plantilla Importamatic de Otros para Todocolección.
- Se genera una descripción HTML con los metadatos cinematográficos sin columna propia.
- Se incorpora la selección, previsualización, descarga CSV, preparación de carátulas y limpieza posterior de operación.
- Se añade `src/project_meta.py` para compartir la versión declarada en `pyproject.toml` entre backend, frontend y tests.
- Se añaden `ROADMAP.md`, `CHANGELOG.md` y tests mínimos de metadatos/API.
- Se añaden comandos `make lint`, `make format`, `make test`, `make update-repo`, `make update` y `make ensure-env`.

### Cambiado
- Se corrige la prioridad del título inicial en `items`: primero título español de IMDb, después título revisado/extraído y por último título original.
- La pantalla de catálogo comercial pasa a llamarse `Formulario`.
- La construcción del árbol de secciones TC se extrae a `src/backend/services/tc_sections.py` para reducir responsabilidades en `catalog.py`.
- `pyproject.toml` pasa a declarar `0.2.0`, alineado con la versión que ya exponía la API.
- El `Makefile` carga `.env` de forma opcional y usa instalación editable con extras de desarrollo.
- `.gitignore` pasa a cubrir cachés, `.egg-info`, bases DuckDB, WAL, backups DuckDB y datos locales bajo `data/`.


## [0.2.0] - 2026-06-16

### Añadido
- Pipeline LangGraph para orquestar extracción, IMDb, título en español, OMDb y traducción.
- Esquema DuckDB normalizado con tablas por área y vista `movies` de compatibilidad.
- Pantalla de orquestación con grafo, estado global, ejecución acotada y cola de revisión.
- Controles de timeout HTTP en Streamlit para trabajos largos.


## [0.1.0]

Primera versión funcional del catálogo de películas con backend FastAPI, frontend Streamlit y persistencia DuckDB.
