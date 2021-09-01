from .pipereg import pipereg
from .transform_data import transform_step


@pipereg.register(dependencies=[transform_step])
def report_step():
    pass
