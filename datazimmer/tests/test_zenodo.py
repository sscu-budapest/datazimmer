"""Behaviour of the Zenodo REST wrapper, with ``requests`` fully faked so the
core suite needs neither the network nor the ``[zenodo]`` extra installed."""

import sys
import types

import pytest


class _FakeResponse:
    def __init__(self, payload: dict) -> None:
        self._payload = payload

    def raise_for_status(self) -> None:
        pass

    def json(self) -> dict:
        return self._payload


class _FakeSession:
    def __init__(self) -> None:
        self.params: dict = {}
        self.calls: list[tuple] = []
        self.timeouts: list = []

    def post(self, url, json=None, timeout=None):
        self.calls.append(("POST", url, json))
        self.timeouts.append(timeout)
        payload = {"id": 42}
        if url.endswith("/deposit/depositions"):
            payload["links"] = {"bucket": "https://zenodo.example/bucket/abc"}
        return _FakeResponse(payload)

    def put(self, url, data=None, timeout=None):
        self.calls.append(("PUT", url, None))
        self.timeouts.append(timeout)
        return _FakeResponse({"key": url.rsplit("/", 1)[-1]})


def _fake_requests(monkeypatch) -> None:
    fake_requests = types.ModuleType("requests")
    fake_requests.Session = _FakeSession
    monkeypatch.setitem(sys.modules, "requests", fake_requests)


@pytest.fixture
def deposit(monkeypatch):
    _fake_requests(monkeypatch)
    monkeypatch.setenv("ZENODO_TOKEN", "tok")
    from datazimmer.zenodo import ZenodoDeposit

    return ZenodoDeposit()


def test_token_from_env(deposit):
    assert deposit._session.params == {"access_token": "tok"}
    assert deposit.base.endswith("zenodo.org/api")


def test_create_posts_metadata(deposit):
    dep_id = deposit.create("Dogshow", "1.2.0", description="dogs")
    assert dep_id == 42
    method, url, body = deposit._session.calls[-1]
    assert (method, url) == ("POST", f"{deposit.base}/deposit/depositions")
    meta = body["metadata"]
    assert (meta["title"], meta["version"], meta["upload_type"]) == (
        "Dogshow",
        "1.2.0",
        "dataset",
    )


def test_upload_puts_to_bucket(deposit, tmp_path):
    deposit.create("Dogshow", "1.0.0")
    f = tmp_path / "data.parquet"
    f.write_bytes(b"x")
    deposit.upload(f)
    method, url, _ = deposit._session.calls[-1]
    assert method == "PUT"
    assert url == "https://zenodo.example/bucket/abc/data.parquet"


def test_publish_hits_publish_action(deposit):
    deposit.create("Dogshow", "1.0.0")
    deposit.publish()
    method, url, _ = deposit._session.calls[-1]
    assert method == "POST"
    assert url == f"{deposit.base}/deposit/depositions/42/actions/publish"


def test_every_request_is_bounded(deposit, tmp_path):
    from datazimmer.remote import TIMEOUT

    deposit.create("Dogshow", "1.0.0")
    (f := tmp_path / "data.parquet").write_bytes(b"x")
    deposit.upload(f)
    deposit.publish()
    assert deposit._session.timeouts == [TIMEOUT] * 3


def test_requires_create_before_upload(deposit, tmp_path):
    with pytest.raises(RuntimeError, match="create"):
        deposit.upload(tmp_path / "x")


def test_sandbox_uses_sandbox_token(monkeypatch):
    _fake_requests(monkeypatch)
    monkeypatch.setenv("ZENODO_SANDBOX_TOKEN", "sbox")
    from datazimmer.zenodo import ZenodoDeposit

    dep = ZenodoDeposit(sandbox=True)
    assert dep._session.params == {"access_token": "sbox"}
    assert "sandbox" in dep.base
