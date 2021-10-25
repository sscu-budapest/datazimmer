from .success import calculate_success, status_table
from .pipereg import pipereg


@pipereg.register(dependencies=[status_table, calculate_success])
def report_step():
    pass
