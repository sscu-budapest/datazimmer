"""Versioned publishing + consumption across the remote backends.

* ``local``  — the full producer↔consumer roundtrip (publish, load, codegen)
* ``s3``     — moto-mocked: publish + manifest fetch + codegen (boto3 paths)
* ``http``   — read-only consumer: manifest fetch + codegen + lazy load over HTTP
* ``ssh``    — full rsync roundtrip; only when ``DOGSHOW_SSH_REMOTE`` is set
               (wired in the dockerized run against the sshd service)
"""

import os
import threading

import pytest
from datazimmer import Config, fetch_manifest, load, publish, schema_codegen
from datazimmer.remote import LocalBackend, S3Backend, SshBackend


def _assert_dog_codegen(manifest):
    code = schema_codegen(manifest)
    ns: dict = {}
    exec(compile(code, "<codegen>", "exec"), ns)  # noqa: S102
    ann = ns["Dog"].__annotations__
    assert "breed" in ann and "kennel" in ann  # both FKs, incl. the nullable one


def test_local_roundtrip(built):
    config = Config.load()  # remote = local "_remote"
    manifest = publish(config, built.pipeline.output_tables)

    dogs_meta = manifest.tables["dogs"].schema
    assert dogs_meta["index"] == ["dog_id"]
    assert dogs_meta["sorted_by"] == ["dog_id"]
    fks = {fk["target_entity"] for fk in dogs_meta["foreign_keys"]}
    assert {"Breed", "Kennel"} <= fks

    breeds = load("_remote/dogshow", "breeds").collect()
    assert sorted(breeds["name"].to_list()) == [
        "Border Collie",
        "Great Dane",
        "Labrador",
        "Pug",
    ]
    _assert_dog_codegen(fetch_manifest("_remote/dogshow"))


def test_s3_moto(built, monkeypatch):
    moto = pytest.importorskip("moto")
    import boto3

    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "test")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "test")
    with moto.mock_aws():
        boto3.client("s3").create_bucket(Bucket="dz-bucket")
        config = Config.load()
        publish(
            config,
            built.pipeline.output_tables,
            backend=S3Backend("s3://dz-bucket/datasets"),
        )
        manifest = fetch_manifest("s3://dz-bucket/datasets/dogshow")
        assert manifest.version == "0.1.0"
        assert {"dogs", "breeds", "results"} <= set(manifest.tables)
        _assert_dog_codegen(manifest)


def test_http_consumer(built, tmp_path):
    from flask import Flask
    from werkzeug.serving import make_server

    srv = tmp_path / "srv"
    publish(Config.load(), built.pipeline.output_tables, backend=LocalBackend(str(srv)))

    # a static host with Range support (as any real one has): polars scans
    # parquet over HTTP with range requests
    app = Flask(__name__, static_folder=str(srv), static_url_path="")
    server = make_server("127.0.0.1", 0, app, threaded=True)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        source = f"http://127.0.0.1:{server.server_port}/dogshow"
        manifest = fetch_manifest(source)
        assert manifest.version == "0.1.0" and "breeds" in manifest.tables
        _assert_dog_codegen(manifest)
        for table in (built.breeds, built.results):  # results is partitioned
            assert load(source, table.name).collect().height == table.n_rows()
    finally:
        server.shutdown()


@pytest.mark.skipif(
    not os.environ.get("DOGSHOW_SSH_REMOTE"),
    reason="set DOGSHOW_SSH_REMOTE (user@host:/path) to test the ssh backend",
)
def test_ssh_roundtrip(built):
    base = os.environ["DOGSHOW_SSH_REMOTE"]
    config = Config.load()
    publish(config, built.pipeline.output_tables, backend=SshBackend(base))
    breeds = load(f"{base}/dogshow", "breeds").collect()  # pull then local scan
    assert breeds.height == 4
