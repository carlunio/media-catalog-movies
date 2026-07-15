import requests
import streamlit as st
from urllib.parse import urlparse

try:
    from src.frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        render_movie_prev_next,
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
    )
except ModuleNotFoundError:  # pragma: no cover
    from frontend.utils import (
        LONG_TIMEOUT_SECONDS,
        api_get,
        api_post,
        api_put,
        build_review_rerun_options,
        configure_page,
        render_icon_heading,
        render_movie_prev_next,
        infer_review_stage,
        render_timeout_controls,
        select_movie_id,
    )

configure_page()
render_icon_heading("Fase 4 - OMDb", icon="database", level=1)
render_timeout_controls()


def _switch_page(target: str) -> None:
    switch_fn = getattr(st, "switch_page", None)
    if callable(switch_fn):
        switch_fn(target)
    else:
        st.info("Tu versión de Streamlit no soporta `switch_page`.")


def _filter_rows(
    rows: list[dict],
    *,
    mode: str,
) -> list[dict]:
    if mode == "review":
        return [row for row in rows if row.get("workflow_needs_review")]
    if mode == "review_stage":
        return [
            row for row in rows if row.get("workflow_needs_review") and infer_review_stage(row) == "omdb"
        ]
    return rows


def _split_semicolon_keep_empty(raw_value: str | None) -> list[str]:
    parts = [part.strip() for part in str(raw_value or "").split(";")]
    if len(parts) == 1 and not parts[0]:
        return []
    return parts


def _split_poster_urls(raw_value: str | None) -> list[tuple[int, str]]:
    urls: list[tuple[int, str]] = []
    for index, part in enumerate(_split_semicolon_keep_empty(raw_value), start=1):
        candidate = part.strip()
        if not candidate or candidate.upper() == "N/A":
            continue

        parsed = urlparse(candidate)
        if parsed.scheme.lower() not in {"http", "https"} or not parsed.netloc:
            continue

        urls.append((index, candidate))
    return urls


render_icon_heading("Acciones por lote (opcional)", icon="list", level=2)
with st.expander("Descargar OMDb por lote", expanded=False):
    batch_c1, batch_c2, batch_c3 = st.columns([2, 1, 1])
    with batch_c1:
        movie_id = st.text_input("ID concreto (opcional)", value="")
    with batch_c2:
        limit = st.number_input("Límite batch", min_value=1, max_value=5000, value=20)
    with batch_c3:
        overwrite = st.checkbox("Volver a descargar", value=False)

    if st.button("Descargar OMDb (batch)"):
        try:
            result = api_post(
                "/omdb/fetch",
                json={
                    "movie_id": movie_id or None,
                    "limit": int(limit),
                    "overwrite": overwrite,
                },
                timeout=LONG_TIMEOUT_SECONDS,
            )
            st.success("Descarga de OMDb completada")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error(
                "Timeout esperando al backend. "
                "Reduce el límite del lote o cambia el modo en Sidebar > HTTP timeout."
            )
        except Exception as exc:
            st.error(str(exc))

st.divider()

rows = api_get("/movies", params={"limit": 5000})
rows_with_imdb = [row for row in rows if row.get("imdb_id")]
if not rows_with_imdb:
    st.info("No hay películas con IMDb ID")
    st.stop()

mode_labels = {
    "all": "Mostrar todas",
    "review": "Solo en revisión",
    "review_stage": "Solo revisión de OMDb",
}
if "omdb_filter_mode" not in st.session_state:
    st.session_state["omdb_filter_mode"] = "all"

filter_mode = st.segmented_control(
    "Filtro",
    options=list(mode_labels.keys()),
    default=st.session_state["omdb_filter_mode"],
    format_func=lambda value: mode_labels.get(str(value), str(value)),
    key="omdb_filter_mode",
    width="stretch",
)
if filter_mode is None:
    filter_mode = "all"

filtered_rows = _filter_rows(
    rows_with_imdb,
    mode=str(filter_mode),
)
st.caption(
    f"Filtro actual: {mode_labels.get(str(filter_mode), 'Mostrar todas')} | "
    f"{len(filtered_rows)} de {len(rows_with_imdb)} películas"
)

if not filtered_rows:
    st.info("No hay películas con los filtros actuales.")
    st.stop()

selected_id = select_movie_id(filtered_rows, label="Película", key="omdb_movie_selector")
render_movie_prev_next(filtered_rows, selected_id, key_prefix="omdb_movie")


nav_f2, nav_f3, nav_f5 = st.columns(3)
with nav_f2:
    if st.button("Ir a Fase 2 - Título", width="stretch", key="omdb_to_f2"):
        _switch_page("pages/02_Título.py")
with nav_f3:
    if st.button("Ir a Fase 3 - IMDb", width="stretch", key="omdb_to_f3"):
        _switch_page("pages/03_IMDb.py")
with nav_f5:
    if st.button("Ir a Fase 5 - Sinopsis ES", width="stretch", key="omdb_to_f5"):
        _switch_page("pages/05_Sinopsis_ES.py")

movie = api_get(f"/movies/{selected_id}")
review_stage = infer_review_stage(movie)

render_icon_heading("Acciones sobre película seleccionada", icon="film", level=2)
st.write("IMDb ID:", movie.get("imdb_id") or "")
st.write("Estado OMDb:", movie.get("omdb_status") or "")
if review_stage:
    st.caption(f"Origen de revisión detectado: `{review_stage}`")
if movie.get("omdb_last_error"):
    st.warning(movie.get("omdb_last_error"))
if movie.get("workflow_needs_review"):
    st.warning(movie.get("workflow_review_reason") or "Pendiente de revisión")

if st.button("Descargar OMDb solo este ID", key="omdb_fetch_single_btn"):
    try:
        result = api_post(
            "/omdb/fetch",
            json={
                "movie_id": selected_id,
                "limit": 1,
                "overwrite": True,
            },
            timeout=LONG_TIMEOUT_SECONDS,
        )
        st.success("Descarga de OMDb completada para la película seleccionada")
        st.json(result)
    except requests.exceptions.ReadTimeout:
        st.error("Timeout esperando al backend para este ID. Prueba Sidebar > HTTP timeout.")
    except Exception as exc:
        st.error(str(exc))

form_col, poster_col = st.columns([2.25, 1.0], gap="large")
with form_col:
    with st.form("omdb_review"):
        b1_c1, b1_c2, b1_c3 = st.columns([2.2, 0.8, 1.0])
        with b1_c1:
            omdb_title = st.text_input("Título", value=movie.get("omdb_title") or "")
        with b1_c2:
            omdb_year = st.text_input("Year", value=movie.get("omdb_year") or "")
        with b1_c3:
            omdb_runtime = st.text_input("Runtime", value=movie.get("omdb_runtime") or "")

        b2_c1, b2_c2 = st.columns(2)
        with b2_c1:
            omdb_type = st.text_input("Tipo", value=movie.get("omdb_type") or "")
        with b2_c2:
            omdb_genre = st.text_input("Genero", value=movie.get("omdb_genre") or "")

        b3_c1, b3_c2 = st.columns(2)
        with b3_c1:
            omdb_language = st.text_input("Idioma", value=movie.get("omdb_language") or "")
        with b3_c2:
            omdb_country = st.text_input("Pais", value=movie.get("omdb_country") or "")

        b4_c1, b4_c2, b4_c3 = st.columns([1.1, 1.1, 1.8])
        with b4_c1:
            omdb_director = st.text_input("Director", value=movie.get("omdb_director") or "")
        with b4_c2:
            omdb_writer = st.text_input("Guionista", value=movie.get("omdb_writer") or "")
        with b4_c3:
            omdb_actors = st.text_input("Actores", value=movie.get("omdb_actors") or "")

        omdb_plot_en = st.text_area("Sinopsis (EN)", value=movie.get("omdb_plot_en") or "", height=190)

        save = st.form_submit_button("Guardar cambios")

with poster_col:
    st.markdown("**Portada OMDb**")
    title_slots = _split_semicolon_keep_empty(movie.get("omdb_title"))
    poster_raw_slots = _split_semicolon_keep_empty(movie.get("omdb_poster"))
    poster_urls = _split_poster_urls(movie.get("omdb_poster"))
    total_slots = max(len(title_slots), len(poster_raw_slots), len(poster_urls))

    if not poster_urls:
        st.info("Sin URL válida en `omdb_poster`.")
    else:
        poster_index_key = f"omdb_poster_index::{selected_id}"
        current_index = int(st.session_state.get(poster_index_key, 0))
        if current_index < 0 or current_index >= len(poster_urls):
            current_index = 0
            st.session_state[poster_index_key] = 0

        if len(poster_urls) > 1:
            nav_prev, nav_mid, nav_next = st.columns([1, 1.3, 1])
            with nav_prev:
                if st.button(
                    "←",
                    key=f"omdb_poster_prev::{selected_id}",
                    width="stretch",
                ):
                    current_index = (current_index - 1) % len(poster_urls)
                    st.session_state[poster_index_key] = current_index
            with nav_mid:
                slot_number = int(poster_urls[current_index][0])
                if total_slots > 1:
                    st.caption(f"{slot_number:02d}/{total_slots:02d}")
                else:
                    st.caption("01/01")
            with nav_next:
                if st.button(
                    "→",
                    key=f"omdb_poster_next::{selected_id}",
                    width="stretch",
                ):
                    current_index = (current_index + 1) % len(poster_urls)
                    st.session_state[poster_index_key] = current_index

        current_slot, current_url = poster_urls[int(st.session_state.get(poster_index_key, current_index))]
        if 0 < current_slot <= len(title_slots) and title_slots[current_slot - 1]:
            st.caption(f"Título {current_slot:02d}: {title_slots[current_slot - 1]}")
        st.image(current_url)
        st.caption(current_url)
        if st.button(
            "Descargar como imagen 2",
            key=f"omdb_download_second_image::{selected_id}",
            width="stretch",
        ):
            try:
                result = api_post(
                    "/omdb/covers/download",
                    json={"movie_id": selected_id, "poster_slot": int(current_slot)},
                    timeout=LONG_TIMEOUT_SECONDS,
                )
            except Exception as exc:
                st.error(f"No se pudo descargar la imagen 2: {exc}")
            else:
                downloaded_count = int(result.get("downloaded_count") or 0)
                failed_count = int(result.get("failed_count") or 0)
                if downloaded_count and not failed_count:
                    st.success("Imagen 2 descargada desde OMDb.")
                elif downloaded_count:
                    st.warning("Imagen 2 descargada, con avisos.")
                else:
                    st.warning("No se ha descargado ninguna imagen 2.")
                output_dir = str(result.get("output_dir") or "")
                if output_dir:
                    st.caption(f"Carpeta: `{output_dir}`.")
                errors = list(result.get("errors") or [])
                if errors:
                    with st.expander("Ver errores de descarga"):
                        st.dataframe(errors, width="stretch", hide_index=True)

if save:
    try:
        api_put(
            f"/movies/{selected_id}/omdb",
            json={
                "fields": {
                    "omdb_title": omdb_title,
                    "omdb_year": omdb_year,
                    "omdb_type": omdb_type,
                    "omdb_runtime": omdb_runtime,
                    "omdb_genre": omdb_genre,
                    "omdb_director": omdb_director,
                    "omdb_writer": omdb_writer,
                    "omdb_actors": omdb_actors,
                    "omdb_language": omdb_language,
                    "omdb_country": omdb_country,
                    "omdb_plot_en": omdb_plot_en,
                }
            },
        )
        st.success("OMDb actualizado")
    except Exception as exc:
        st.error(str(exc))

if movie.get("omdb_raw"):
    with st.expander("Ver OMDb raw"):
        st.json(movie["omdb_raw"])

st.divider()
render_icon_heading("Reejecución acotada hasta revisión", icon="rotate-right", level=2)
if not movie.get("workflow_needs_review"):
    st.info("Esta película no está en revisión.")
else:
    stage_target = review_stage or "omdb"
    options = build_review_rerun_options(stage_target)
    option_labels = [label for label, _ in options]
    option_map = {label: start for label, start in options}
    selected_option = st.selectbox("Reejecución disponible", option_labels, index=0, key="omdb_rerun_option")
    selected_start_stage = option_map[selected_option]

    if st.button("Reejecutar workflow hasta revisión", key="omdb_rerun_btn"):
        payload = {
            "movie_id": selected_id,
            "limit": 1,
            "start_stage": selected_start_stage,
            "stop_after": stage_target,
            "overwrite": True,
        }
        try:
            result = api_post("/workflow/run", json=payload, timeout=LONG_TIMEOUT_SECONDS)
            st.success(f"Workflow relanzado desde {selected_start_stage} hasta {stage_target}.")
            st.json(result)
        except requests.exceptions.ReadTimeout:
            st.error("Timeout relanzando workflow")
        except Exception as exc:
            st.error(str(exc))
