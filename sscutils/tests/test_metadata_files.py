from sscutils.core.metadata_files import RawColsMetadata, TreposMetadata

test_prefix = "fing"

trepo_from = (
    """from sscutils import create_trepo_with_subsets

from .raw_cols import CommonCols

objects_table = create_trepo_with_subsets("objects","""
    """ group_cols=[CommonCols.some_id])
"""
)


trepo_to = (
    """from sscutils import create_trepo_with_subsets

from .fing_raw_cols import CommonCols

objects_table = create_trepo_with_subsets("objects","""
    """ group_cols=[CommonCols.some_id], prefix="fing")
"""
)

rawcols_from = """from colassigner import ColAccessor

class CommonCols(ColAccessor):
    obj_id = "object_id"
"""


rawcols_to = """from colassigner import ColAccessor

class CommonCols(ColAccessor):
    obj_id = "object_id"
"""


def test_trepo():
    trmeta = TreposMetadata("", "", test_prefix)
    assert trepo_to == trmeta.adjust_content(trepo_from)


def test_raw_cols():
    rcmeta = RawColsMetadata("", "", test_prefix)
    assert rawcols_to == rcmeta.adjust_content(rawcols_from)
