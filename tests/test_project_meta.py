from src.project_meta import get_app_meta


def test_project_meta_matches_current_version():
    meta = get_app_meta()

    assert meta.project_name == "media-catalog-movies"
    assert meta.version == "0.2.0"
    assert meta.changelog_path.name == "CHANGELOG.md"
