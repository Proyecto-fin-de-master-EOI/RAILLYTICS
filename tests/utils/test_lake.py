import pytest

from raillytics.utils.lake import LakeLayout, S3Settings, is_s3


def test_s3_settings_from_env_strips_scheme_and_detects_ssl():
    env = {"MINIO_ENDPOINT": "http://localhost:19000", "MINIO_ROOT_USER": "user", "MINIO_ROOT_PASSWORD": "pass"}

    assert S3Settings.from_env(env) == S3Settings(
        endpoint="localhost:19000", access_key="user", secret_key="pass", use_ssl=False
    )
    assert S3Settings.from_env({**env, "MINIO_ENDPOINT": "https://minio.example:443"}).use_ssl is True
    # Sin esquema (como lo escribiría alguien a mano) también vale.
    assert S3Settings.from_env({**env, "MINIO_ENDPOINT": "minio:9000"}).endpoint == "minio:9000"


def test_s3_settings_from_env_requires_credentials():
    with pytest.raises(ValueError, match="MINIO_ROOT_USER"):
        S3Settings.from_env({"MINIO_ENDPOINT": "http://minio:9000"})


def test_lake_layout_defaults_to_minio_buckets():
    layout = LakeLayout.from_env({})

    assert layout == LakeLayout(silver_root="s3://raillytics-silver", gold_root="s3://raillytics-gold")
    assert layout.uses_s3
    assert layout.silver_glob("viajeros_enriquecidos") == "s3://raillytics-silver/viajeros_enriquecidos/*.parquet"
    assert layout.silver_file("viajeros_enriquecidos") == "s3://raillytics-silver/viajeros_enriquecidos/viajeros_enriquecidos.parquet"
    assert layout.gold_glob("dim_fecha") == "s3://raillytics-gold/dim_fecha/*.parquet"
    assert layout.cargas_glob() == "s3://raillytics-gold/_trazabilidad/cargas/*.parquet"


def test_lake_layout_honours_bucket_names_and_local_overrides():
    assert LakeLayout.from_env({"MINIO_BUCKET_GOLD": "otro-gold"}).gold_root == "s3://otro-gold"

    local = LakeLayout.from_env({"SILVER_ROOT": "C:/lake/silver", "GOLD_ROOT": "C:/lake/gold"})

    assert local == LakeLayout(silver_root="C:/lake/silver", gold_root="C:/lake/gold")
    assert not local.uses_s3
    assert not is_s3("C:/lake/gold")
    assert is_s3("s3://raillytics-gold")
