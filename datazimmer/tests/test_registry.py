import os

from datazimmer.naming import GIT_TOKEN_ENV_VAR
from datazimmer.registry import _de_auth


def test_de_auth():
    os.environ[GIT_TOKEN_ENV_VAR] = "GTOKEN"
    url_auth = "git@github.com:sscu-budapest/datazimmer.git"
    url_tokened = "https://GTOKEN@github.com/sscu-budapest/datazimmer"
    assert _de_auth(url_auth) == "https://github.com/sscu-budapest/datazimmer"
    assert _de_auth(url_auth, re_auth=True) == url_tokened
