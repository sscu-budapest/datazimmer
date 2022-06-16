from datazimmer.full_auth import ZimmerAuth

AU_DIC = {
    "keys": {
        "main": {"key": "a", "secret": "b"},
    },
    "rsa_keys": {"xkey": "y"},
    "ssh": {"host1": {"key": "xkey"}},
    "ssh_remote": {"url": "ssh://foo/742"},
    "s3_remote": {"key": "main"},
}


def test_auth(tmp_path):
    auth = ZimmerAuth(AU_DIC)
    tconf = tmp_path / ".ssh" / "config"
    tconf.parent.mkdir()
    tconf.write_text("")
    auth._dump_ssh_conf(tmp_path)
    assert tconf.read_text().strip().startswith("Host host1")
