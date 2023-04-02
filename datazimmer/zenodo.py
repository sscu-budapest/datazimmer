import calendar
import datetime as dt
import json
import os
import re
from dataclasses import asdict, dataclass, field
from functools import cached_property, partial, reduce
from pathlib import Path

import yaml
from cryptography.fernet import Fernet

from .config_loading import Config, UserConfig
from .naming import README_PATH

ZENODO_TOKEN_ENV_VAR = "ZENODO_TOKEN"
ZENODO_TEST_TOKEN_ENV_VAR = "ZENODO_SANDBOX_TOKEN"
CITATION_FILE = Path("CITATION.cff")
CITE_HEADER = "## Citation"

cite_frame = """
To reference this resource, please cite the dataset {short_data}, \
and the software {short_dz} used to create, parse and publish it.

{ama_data}

{ama_dz}

```
{dz_bib}
```

```
{bib}
```"""

rm_frame = (
    "[![DOI](https://{url_base}/badge/doi/{doi_num}/zenodo.{zid}.svg)]"
    "(https://doi.org/{doi_num}/zenodo.{zid})"
)

z_rex = reduce(lambda le, ri: le.replace(ri, f"\\{ri}"), "()[]!", rm_frame).format(
    url_base="(.*)", doi_num="(.*)", zid=r"(\d+)"
)

dz_concept_zid = 7499121


def get_cites():
    return [
        {"relation": "cites", "identifier": f"10.5281/zenodo.{dz_concept_zid}"},
        # TODO: {"relation": "isDerivedFrom", "identifier": "imported raw or project"},
    ]


def key_hyphener(kv_pairs):
    return {k.replace("_", "-"): v for k, v in kv_pairs}


as_yaml_dict = partial(asdict, dict_factory=key_hyphener)


@dataclass
class Author:
    family_names: str
    given_names: str
    orcid: str

    @classmethod
    def from_zdic(cls, dic):
        fnames, gnames = dic["name"].split(", ")
        return cls(orcid=dic["orcid"], family_names=fnames, given_names=gnames)

    def comma_name(self):
        return f"{self.family_names}, {self.given_names}"


@dataclass
class ShortCitation:
    title: str
    doi: str
    version: str
    type: str  # software or dataset
    date_released: str

    @classmethod
    def from_zen_dic(cls, dic):
        meta = dic["metadata"]
        return cls(
            doi=dic["doi"],
            title=meta["title"],
            version=meta["version"],
            type=meta["resource_type"]["type"],
            date_released=meta["publication_date"],
        )


@dataclass
class Citation(ShortCitation):
    authors: list[Author]
    keywords: list[str]
    license: str
    message: str
    url: str
    references: list[ShortCitation] = field(default_factory=list)
    cff_version: str = "1.2.0"

    @classmethod
    def from_zen_dic(cls, dic):
        short = ShortCitation.from_zen_dic(dic)
        meta = dic["metadata"]
        return cls(
            **asdict(short),
            authors=list(map(Author.from_zdic, meta["creators"])),
            keywords=meta.get("keywords", []),
            license=meta.get("license", {"id": "closed"})["id"],
            message=meta.get(
                "notes", "If you use this software, please cite it as below."
            ),
            url=f"https://doi.org/{short.doi}",
        )

    def to_bib(self):
        pairs = [
            ("author", " and ".join(map(Author.comma_name, self.authors))),
            ("title", self.title),
            ("month", self.month),
            ("year", str(self.year)),
            ("publisher", self.publisher),
            ("version", self.version),
            ("doi", self.doi),
            ("url", self.url),
        ]
        bib_lines = [
            "@%s{%s," % (self.type, self.id_),
            ",\n".join(map(_to_bibline, pairs)),
            "}",
        ]
        return "\n".join(bib_lines)

    def to_ama(self):
        longnames = " & ".join(f"{person.comma_name()}" for person in self.authors)
        return (
            f"{longnames}. ({self.year}). {self.title} ({self.version})."
            f" {self.publisher}. {self.url}"
        )

    def to_short(self):
        shortnames = ", ".join(person.family_names for person in self.authors)
        return f"({shortnames}, {self.year})"

    @property
    def year(self):
        return self._date.year

    @property
    def month(self):
        return calendar.month_abbr[self._date.month].lower()

    @property
    def publisher(self):
        return "Zenodo"

    @property
    def id_(self):
        authid = self.authors[0].family_names.lower().replace(" ", "_")
        return f"{authid}_{self.year}_{self.doi.split('.')[-1]}"

    @property
    def _date(self):
        return dt.date.fromisoformat(self.date_released)


class ZenodoMeta:
    def __init__(self, title, version, lines, private: bool) -> None:
        import markdown2

        self.title: str = title  # from git remote last 2 elems
        self.version: str = version  # from git tag
        self.publication_date = dt.date.today().isoformat()
        self.upload_type: str = "dataset"  # /software/other
        self.description: str = markdown2.markdown("\n".join(lines))
        self.access_right = "closed" if private else "open"
        # embargoed/restricted/closed
        self.license: str = "" if private else "MIT"
        # self.keywords: list[str] = []
        self.related_identifiers: list[dict] = get_cites()
        uconf = UserConfig.load()
        # TODO: optionally add more
        self.creators: list[dict] = [
            {
                "name": ", ".join([uconf.last_name, uconf.first_name]),
                "orcid": uconf.orcid,
            }
        ]
        # contributors: list[dict] = field(default_factory=list)  # type
        # embargo_date if embargoed - automatically made public
        # access_conditions: str (html) if restricted

    def data(self):
        return {"metadata": self.__dict__}


class ZenApi:
    def __init__(self, conf: Config, private: bool, tag: str, test: bool = True):
        import requests

        self.live = not test
        self.name = conf.name
        self.tag = tag
        self.private = private
        self.url_base = _get_z_url(test)
        self.url = self.url_base + "/api"
        self.env_var = ZENODO_TEST_TOKEN_ENV_VAR if test else ZENODO_TOKEN_ENV_VAR

        self.s = requests.Session()
        self.s.params = {"access_token": os.environ[self.env_var]}
        self.readme_lines = []

    def publish(self):
        resp = self.s.post(f"{self._dep_path()}/{self.depo_id}/actions/publish")
        assert resp.ok, resp.content.decode("utf-8")

    def get(self, **params):
        return self.s.get(self._dep_path(), params=params)

    def new_deposition(self):
        return self.s.post(self._dep_path(), **self._meta_kwargs())

    def upload(self, fpath: Path, key_path=""):
        assert fpath.exists(), f"{fpath} must exist to upload to zenodo"
        if fpath.is_dir():
            self.upload_directory(fpath, key_path)
        else:
            self.upload_file(fpath, key_path)

    def upload_file(self, fpath: Path, key_path=""):
        if key_path:
            fern = Fernet(bytes.fromhex(Path(key_path).read_text()))
            enc_fpath = Path(fpath.parent, fpath.name + ".encrypted")
            enc_fpath.write_bytes(fern.encrypt(fpath.read_bytes()))
            fpath = enc_fpath
        return self.s.post(
            f"{self._dep_path()}/{self.depo_id}/files",
            data={"name": fpath.as_posix()},
            files={"file": fpath.open(mode="rb")},
        )

    def upload_directory(self, dirpath, key_path=""):
        for root, _, files in os.walk(dirpath):
            for f in files:
                self.upload_file(Path(root, f), key_path)

    def cite(self, did: str, bib=True, live=False):
        headers = {}
        if bib:
            headers["accept"] = "application/x-bibtex"
        return self.s.get(f"{_get_z_url(not live)}/api/records/{did}", headers=headers)

    def zid_from_readme(self):
        zid = None
        for readme_line in README_PATH.read_text().split("\n"):
            if readme_line == CITE_HEADER:
                break
            found = re.findall(z_rex, readme_line)
            if not found:
                self.readme_lines.append(readme_line)
            else:
                zid = int(found[0][-1])
        return zid

    def update_readme(self):
        new_zen_line = rm_frame.format(
            url_base="zenodo.org", doi_num="10.5281", zid=self.depo_id
        )
        self.readme_lines.insert(2, new_zen_line)
        # todo: this is the latest dz bib, but cant search with version
        dz_dic = self.cite(dz_concept_zid, live=True, bib=False).json()
        dz = Citation.from_zen_dic(dz_dic)
        datac = Citation.from_zen_dic(
            self.cite(self.depo_id, live=self.live, bib=False).json()
        )
        cite_inst = cite_frame.format(
            dz_bib=dz.to_bib(),
            bib=datac.to_bib(),
            short_data=datac.to_short(),
            short_dz=dz.to_short(),
            ama_data=datac.to_ama(),
            ama_dz=dz.to_ama(),
        )
        self.readme_lines.extend([CITE_HEADER, cite_inst])
        README_PATH.write_text("\n".join(self.readme_lines))
        datac.references.append(ShortCitation.from_zen_dic(dz_dic))
        CITATION_FILE.write_bytes(
            yaml.safe_dump(as_yaml_dict(datac), encoding="utf-8", allow_unicode=True)
        )

    @property
    def meta(self):
        return ZenodoMeta(self.name, self.tag, self.readme_lines, self.private)

    @cached_property
    def depo_id(self):
        zid = self.zid_from_readme()
        if not zid:
            return self.new_deposition().json()["id"]

        ver_resp = self.s.post(f"{self._dep_path()}/{zid}/actions/newversion")
        if ver_resp.ok:
            id_ = ver_resp.json()["links"]["latest_draft"].split("/")[-1]
            for file in ver_resp.json()["files"]:
                self.s.delete(f"{self._dep_path()}/{id_}/files/{file['id']}")
            resp = self.s.put(f"{self._dep_path()}/{id_}", **self._meta_kwargs())
            assert resp.ok, resp.content.decode("utf-8")
            return id_
        return self.new_deposition().json()["id"]

    def _dep_path(self):
        return f"{self.url}/deposit/depositions"

    def _meta_kwargs(self):
        return dict(
            headers={"Content-Type": "application/json"},
            data=json.dumps(self.meta.data()),
        )


def _get_z_url(sandbox: bool):
    return f"https://{'sandbox.' if sandbox else ''}zenodo.org"


def _to_bibline(kv):
    k = kv[0]
    v = "{%s}" % kv[1]
    return f"  {k}{' ' * (12 -len(k))} = {v}"
