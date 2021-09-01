from sscutils.core.metadata_files import RawColsMetadata, TreposMetadata

test_prefix = "fing"

trepo_from = """from sscutils import create_trepo_with_subsets

objects_table = create_trepo_with_subsets("objects")
"""


trepo_to = """from sscutils import create_trepo_with_subsets

objects_table = create_trepo_with_subsets("objects", prefix="fing")
"""

rawcols_from = """from colassigner import ColAccessor

class CommonCols(ColAccessor):
    obj_id = "object_id"
"""


rawcols_to = """from colassigner import ColAccessor

class FingCommonCols(ColAccessor):
    obj_id = "object_id"
"""


def test_trepo():
    trmeta = TreposMetadata("", "", test_prefix)
    assert trepo_to == trmeta.adjust_content(trepo_from)


def test_raw_cols():
    rcmeta = RawColsMetadata("", "", test_prefix)
    assert rawcols_to == rcmeta.adjust_content(rawcols_from)
