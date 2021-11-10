from ...artifact_context import ArtifactContext
from ...helpers import get_all_child_modules
from ...utils import reset_src_module
from .from_bedrock import ScriptWriter
from .to_bedrock import DatascriptToBedrockConverter


def all_datascript_to_bedrock():
    _cms_to_bedrock(get_all_child_modules())


def child_module_datascript_to_bedrock(child_module):
    _cms_to_bedrock([child_module])


def imported_bedrock_to_datascript():
    ctx = ArtifactContext()
    for nsi in ctx.imported_namespace_meta_list:
        ScriptWriter(nsi, ctx.has_data_env(nsi))
    reset_src_module()


def _cms_to_bedrock(cm_list):
    ctx = ArtifactContext()
    for child_module in cm_list:
        ns_meta = DatascriptToBedrockConverter(child_module).to_ns_metadata()
        ctx.metadata.add_ns(ns_meta)
    ctx.serialize()
