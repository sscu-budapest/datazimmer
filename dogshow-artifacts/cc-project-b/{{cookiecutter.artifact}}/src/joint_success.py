# p1ns1.Success to both dog kinds (from ds1 and ds2)
# based on dotm, race, and comp

from .imported_namespaces import dogsuccess, dogbase

# here dogbase means ds2, where in ds2 dogbase means ds1
# this conflict needs to be resolved


class DogSuccess:

    success = dogsuccess.Success
    # based on dotm  and competitions in ds2
