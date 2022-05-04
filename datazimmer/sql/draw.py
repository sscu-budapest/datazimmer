from sqlmermaid import to_file


def dump_graph(constr):
    to_file(constr, "erd.md")
