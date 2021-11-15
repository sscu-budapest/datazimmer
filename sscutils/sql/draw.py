from dataclasses import dataclass, field

import pydot
import sqlalchemy as sa

from ..utils import is_postgres


def dump_graph(sql_meta, sql_engine, schema_path="dbschema.png"):
    """WIP"""
    Drawer(
        meta=sql_meta,
        engine=sql_engine,
        rankdir="LR",
        concentrate=False,
    ).dump_graph(schema_path)

    # TODO: UML and prettier


@dataclass
class Drawer:

    meta: sa.MetaData
    engine: sa.engine.Engine
    font: str = "Bitstream-Vera Sans"
    concentrate: bool = True
    relation_options: dict = field(default_factory=dict)
    rankdir: str = "TB"

    def get_graph(self):

        graph = self._init_graph()

        for table_id, table in self.tables.items():
            graph.add_node(
                pydot.Node(
                    table_id,
                    shape="plaintext",
                    label=self._render_table_html(table),
                    fontname=self.font,
                    fontsize="7.0",
                )
            )

        for table_id, table in self.tables.items():
            for fk in table.foreign_keys:
                edge = [table_id, fk.column.table.fullname]
                graph_edge = pydot.Edge(
                    *edge,
                    headlabel="+ %s" % fk.column.name,
                    taillabel="+ %s" % fk.parent.name,
                    arrowhead="odot",
                    arrowtail=(fk.parent.primary_key or fk.parent.unique)
                    and "empty"
                    or "crow",
                    fontname=self.font,
                    **self.relation_kwargs,
                )
                graph.add_edge(graph_edge)

        return graph

    def dump_graph(self, path):
        self.get_graph().write_png(path)

    def _init_graph(self):
        return pydot.Dot(
            prog="dot",
            mode="ipsep",
            overlap="ipsep",
            sep="0.01",
            concentrate=str(self.concentrate),
            rankdir=self.rankdir,
        )

    def _render_table_html(self, table):

        html = (
            '<<TABLE BORDER="1" CELLBORDER="0" CELLSPACING="0"><TR>'
            '<TD ALIGN="CENTER">%s</TD></TR><TR>'
            '<TD BORDER="1" CELLPADDING="0"></TD></TR>' % table.fullname
        )

        html += "".join(
            '<TR><TD ALIGN="LEFT" PORT="%s">%s</TD></TR>'
            % (col.name, format_col_str(col))
            for col in table.columns
        )
        if is_postgres(self.engine):
            # postgres engine doesn't reflect indexes

            filters = [f"tablename = '{table.name}'"]

            if table.schema:
                filters.append(f"schemaname = '{table.schema}'")
            sql_str = (
                "SELECT indexname, indexdef FROM pg_indexes WHERE "
                + " AND ".join(filters)
            )
            indexes = [*self.engine.execute(sa.text(sql_str))]

            html += '<TR><TD BORDER="1" CELLPADDING="0"></TD></TR>'
            for _, defin in indexes:
                ilabel = "UNIQUE" in defin and "UNIQUE " or "INDEX "
                sind = defin.index("(")
                ilabel += defin[sind:]
                html += '<TR><TD ALIGN="LEFT">%s</TD></TR>' % ilabel
        html += "</TABLE>>"
        return html

    @property
    def relation_kwargs(self):
        return {"fontsize": "7.0", **self.relation_options}

    @property
    def tables(self):
        return self.meta.tables


def format_col_str(col):
    return f"- {col.name} : {format_col_type(col)}"


def format_col_type(col):
    try:
        return col.type.get_col_spec()
    except (AttributeError, NotImplementedError):
        return str(col.type)
