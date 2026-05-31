"""The self-contained kennel-federation website aswan scrapes in the tests.

No external data: ``federation.py`` serves the registry/breed/shows/results
pages from in-memory fixtures, and ``runner.py`` spins it up (plus the relay
worker) the way aswan's own ``godel_src`` does.
"""

from .federation import DATA, federation_app_creator, federation_port

__all__ = ["DATA", "federation_app_creator", "federation_port"]
