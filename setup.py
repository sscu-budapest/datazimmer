import os
import re
from glob import glob

import toml
from setuptools import find_packages, setup

pytom = toml.load("pyproject.toml")
package_name = pytom["project"]["name"]
author_name = " - ".join(pytom["project"]["authors"])

__version__ = ...
with open(os.path.join(package_name, "_version.py")) as fp:
    exec(fp.read())  # get __version__

with open("README.md") as fp:
    long_description = fp.read()

with open("requirements.txt") as fp:
    requirements = fp.read().strip().split("\n")

extras_dict = {}
for extra_req_path in glob("requirements-*.txt"):
    extra_id = re.findall("requirements-(.*)\.txt", extra_req_path)[0]
    with open(extra_req_path) as fp:
        extras_dict[extra_id] = fp.read().strip().split("\n")

extras_dict["complete"] = sum(extras_dict.values(), [])

if __name__ == "__main__":
    setup(
        name=package_name,
        version=__version__,
        description=pytom["project"]["description"],
        long_description=long_description,
        long_description_content_type="text/markdown",
        license="MIT",
        classifiers=["License :: OSI Approved :: MIT License"],
        url=pytom["project"]["url"],
        keywords=pytom["project"].get("keywords", []),
        author=author_name,
        packages=find_packages(),
        include_package_data=True,
        python_requires=pytom["project"]["python"],
        platforms="any",
        install_requires=requirements,
        extras_require=extras_dict,
    )
