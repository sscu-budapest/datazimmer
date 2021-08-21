from invoke import Collection

from branthebuilder import docs, clean, sonar, test, release, misc, django


ns = Collection()
for module in [docs, clean, sonar, test, release, misc, django]:
    ns.add_collection(Collection.from_module(module))
