"""Explicit pipeline: declared steps, topological execution, hash-based skipping::

    pipeline = Pipeline()

    @pipeline.step(inputs=["raw/"], outputs=[election_table])
    def load():
        election_table.write(parse(Path("raw/")))

    @pipeline.step(inputs=[election_table], outputs=[summary_table])
    def summarize():
        summary_table.write(aggregate(election_table.collect()))

    pipeline.run()   # topo-sorts, skips steps whose inputs are unchanged

A step is skipped when its fingerprint — its source plus the fingerprints of
its inputs — matches its last successful run and its outputs are still present.
Step fingerprints propagate downstream, so editing an upstream step re-runs every
dependent. A step's fingerprint is cleared before it starts and recorded as soon
as it succeeds, in ``HASH_FILE`` under the state dir (``config.STATE_DIR``), so a
failed or interrupted run never leaves a stale fingerprint behind.

An input may be a ``Table``, a path (``str``/``Path``), or another step (its
decorated function) — the last expresses a pure ordering dependency on a step
that produces no table (e.g. a collection step that only populates a depot)::

    @pipeline.step(outputs=[])
    def collect(): ...

    @pipeline.step(inputs=[collect], outputs=[raw_table])
    def parse(): ...
"""

import hashlib
import inspect
import json
from collections.abc import Iterable
from dataclasses import dataclass
from pathlib import Path

from ._hashing import digest_path
from .config import STATE_DIR
from .table import Table

HASH_FILE = "step_hashes.json"


@dataclass(frozen=True)
class Step:
    fn: "callable"
    inputs: tuple
    outputs: tuple[Table, ...]

    @property
    def name(self) -> str:
        return self.fn.__name__


class PipelineError(Exception):
    pass


class Pipeline:
    def __init__(self) -> None:
        self.steps: list[Step] = []

    def step(self, *, inputs=(), outputs=()):
        def decorate(fn):
            if any(s.name == fn.__name__ for s in self.steps):
                raise PipelineError(f"duplicate step name {fn.__name__!r}")
            self.steps.append(Step(fn, tuple(inputs), tuple(outputs)))
            return fn

        return decorate

    @property
    def output_tables(self) -> list[Table]:
        return _unique(t for s in self.steps for t in s.outputs)

    @property
    def tables(self) -> list[Table]:
        """All tables the pipeline touches (step inputs + outputs)."""
        return _unique(
            t
            for s in self.steps
            for t in (*s.outputs, *s.inputs)
            if isinstance(t, Table)
        )

    def run(
        self, *, force: bool = False, state_dir: Path | str = STATE_DIR
    ) -> list[str]:
        """Execute steps in dependency order, skipping unchanged ones.

        Returns the names of the steps that actually ran.
        """
        order = self._topo_sort()
        producers = self._producers()
        hash_path = Path(state_dir) / HASH_FILE
        hash_path.parent.mkdir(parents=True, exist_ok=True)
        stored: dict = json.loads(hash_path.read_text()) if hash_path.exists() else {}

        fingerprints: dict[str, str] = {}
        ran: list[str] = []
        for step in order:
            fp = fingerprints[step.name] = self._fingerprint(
                step, producers, fingerprints
            )
            unchanged = stored.get(step.name) == fp
            if not force and unchanged and all(t.is_populated() for t in step.outputs):
                continue
            stored.pop(step.name, None)
            _save(hash_path, stored)
            step.fn()
            stored[step.name] = fp
            _save(hash_path, stored)
            ran.append(step.name)
        return ran

    def _producers(self) -> dict[str, Step]:
        producers: dict[str, Step] = {}
        for step in self.steps:
            for table in step.outputs:
                key = _table_key(table)
                if key in producers:
                    raise PipelineError(
                        f"table {table.path} produced by both "
                        f"{producers[key].name} and {step.name}"
                    )
                producers[key] = step
        return producers

    def _upstream(self, inp, producers: dict[str, Step]) -> Step | None:
        """The step an input depends on: a table's producer or a referenced step."""
        if isinstance(inp, Table):
            return producers.get(_table_key(inp))
        if _is_step_ref(inp):
            step = next((s for s in self.steps if s.fn is inp), None)
            if step is None:
                raise PipelineError(
                    f"input step {inp.__name__!r} is not in the pipeline"
                )
            return step
        return None

    def _deps(self, step: Step, producers: dict[str, Step]) -> list[Step]:
        deps = (self._upstream(inp, producers) for inp in step.inputs)
        return [d for d in deps if d is not None and d is not step]

    def _topo_sort(self) -> list[Step]:
        producers = self._producers()
        order: list[Step] = []
        seen: set[str] = set()
        visiting: set[str] = set()

        def visit(step: Step):
            if step.name in seen:
                return
            if step.name in visiting:
                raise PipelineError(f"cycle in pipeline at step {step.name!r}")
            visiting.add(step.name)
            for dep in self._deps(step, producers):
                visit(dep)
            visiting.discard(step.name)
            seen.add(step.name)
            order.append(step)

        for step in self.steps:
            visit(step)
        return order

    def _fingerprint(
        self, step: Step, producers: dict[str, Step], computed: dict[str, str]
    ) -> str:
        h = hashlib.blake2b()
        h.update(_source(step.fn).encode("utf-8"))
        for inp in sorted(step.inputs, key=_input_key):
            upstream = self._upstream(inp, producers)
            if upstream is not None and upstream is not step:
                h.update(computed[upstream.name].encode("utf-8"))
            elif isinstance(inp, Table):
                h.update(digest_path(inp.path).encode("utf-8"))
            elif not _is_step_ref(inp):
                h.update(digest_path(inp).encode("utf-8"))
        return h.hexdigest()


def _unique(tables: Iterable[Table]) -> list[Table]:
    return list({_table_key(t): t for t in tables}.values())


def _save(hash_path: Path, stored: dict) -> None:
    hash_path.write_text(json.dumps(stored, indent=2, sort_keys=True))


def _table_key(table: Table) -> str:
    return str(Path(table.path).resolve())


def _is_step_ref(inp) -> bool:
    """A bare step (its decorated function) used to declare an ordering dep."""
    return callable(inp) and not isinstance(inp, (str, Path))


def _input_key(inp) -> str:
    if isinstance(inp, Table):
        return _table_key(inp)
    if _is_step_ref(inp):
        return getattr(inp, "__name__", repr(inp))
    return str(inp)


def _source(fn) -> str:
    try:
        return inspect.getsource(fn)
    except (OSError, TypeError):  # pragma: no cover
        return fn.__name__
