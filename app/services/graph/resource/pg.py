from functools import partial

from aiopg import sa

from . import ABCGraphModel
from ....lib.graph import AcyclicDiGraph, InconsistentState


class PgEngine:

    def __init__(self, config, loop):
        self.config = config['postgres']
        self.loop = loop
        self._engine = None

    async def init_engine(self):
        self._engine = await sa.create_engine(
            database=self.config['database'],
            user=self.config['user'],
            password=self.config['password'],
            host=self.config['host'],
            port=self.config['port'],
            minsize=self.config['minsize'],
            maxsize=self.config['maxsize'],
            loop=self.loop
        )

    def engine(self) -> sa.Engine:
        return self._engine

    async def close(self):
        if self._engine:
            self._engine.close()
            await self._engine.wait_closed()


class PgGinGraphModel(ABCGraphModel):

    _cast = str

    def __init__(self, pg_engine: PgEngine):
        self.pg_engine = pg_engine

    async def init(self):
        async with self.pg_engine.engine().acquire() as conn:
            await conn.execute('DROP TABLE IF EXISTS graph')
            await conn.execute(
                '''CREATE TABLE graph (
                        vertex text PRIMARY KEY,
                        vertex_out text[]
                )'''
            )
            await conn.execute(
                '''CREATE INDEX vertex_out_gin_idx
                    ON graph
                    USING gin (vertex_out)
                    WITH (fastupdate = off);'''
            )

    async def has_vertex(self, vertex):
        async with self.pg_engine.engine().acquire() as conn:
            rows = await conn.execute(
                "select vertex from graph where vertex = %s", vertex
            )
            return rows.rowcount == 1

    async def vertexes(self):
        async with self.pg_engine.engine().acquire() as conn:
            rows = await conn.execute(
                'select vertex from graph'
            )
            for row in rows:
                yield row.vertex

    async def insert(self, edges):
        if len(edges) == 1:
            await self._insert_one(*self._normalize_edge(edges.pop()))
        elif len(edges) > 1:
            await self._insert_many(list(map(self._normalize_edge, edges)))

    async def _insert_one(self, v_from, v_to):
        async with self.pg_engine.engine().acquire() as conn:
            async with conn.begin():
                vtx_to = await self._descendants(v_from, conn) | {v_to}

                async def _descendants(v):
                    return vtx_to if v == v_from else await self._descendants(v, conn)  # noqa

                await conn.execute('lock graph in ROW EXCLUSIVE mode')

                if await AcyclicDiGraph.ahas_cycle(
                    _descendants,
                    {v_from},
                    seen=set(),
                ):
                    raise InconsistentState(
                        'Cycle for {} -> {}'.format(v_from, v_to),
                    )

                await self._insert_one_pg(v_from, v_to, conn)

    async def _insert_many(self, edges):
        tmp = AcyclicDiGraph()

        for v_from, v_to in edges:
            tmp.insert(v_from, v_to)

        async with self.pg_engine.engine().acquire() as conn:
            async with conn.begin():
                async def _descendants(v):
                    return await self._descendants(v, conn) | tmp.vertexes_to(v)  # noqa

                await conn.execute('lock graph in ROW EXCLUSIVE mode')

                if await AcyclicDiGraph.ahas_cycle(
                    _descendants,
                        set(filter(
                            lambda other_edge: len(
                                tmp.vertexes_to(other_edge)) > 0,
                            tmp.vertexes(),
                        )),
                        seen=set()
                ):
                    raise InconsistentState()

                for v_from, v_to in edges:
                    await self._insert_one_pg(v_from, v_to, conn)

    async def _insert_one_pg(self, v_from, v_to, conn):
        if v_to is None:
            await conn.execute(
                """insert into graph as g (vertex, vertex_out)
                values(%s, '{}')
                on conflict(vertex) do nothing
                """,
                v_from,
            )
        else:
            await conn.execute(
                """insert into graph as g (vertex, vertex_out)
                values(%s, %s)
                on conflict(vertex) do
                update set
                vertex_out = array_append(
                  array_remove(g.vertex_out, %s),
                  %s
                )
                """,
                v_from,
                [v_to],
                v_to,
                v_to
            )

    async def trees(self, vertex):
        async def collect_subtrees(
            descendants_f,
            start_vertex,
            stack=None,
        ) -> list:
            stack = stack or []

            stack.append(start_vertex)

            vs_out = await descendants_f(start_vertex)
            if len(vs_out) == 0:
                res = stack.copy()
                stack.pop()
                return [res]

            res = []
            for v_out in vs_out:
                res.extend(await collect_subtrees(descendants_f, v_out, stack))

            stack.pop()

            return res

        async with self.pg_engine.engine().acquire() as conn:
            child_subtrees = list(
                await collect_subtrees(
                    partial(self._descendants, conn=conn),
                    vertex,
                )
            )
            parent_subtrees = list(
                await collect_subtrees(
                    partial(self._ancestors, conn=conn),
                    vertex,
                )
            )

        result = []
        for parent_subtree in parent_subtrees:
            parent_subtree = list(reversed(parent_subtree))
            parent_subtree.pop()

            for child_subtree in child_subtrees:
                result.append(parent_subtree + child_subtree)

        return result

    async def _ancestors(self, vertex, conn=None):
        rows = await conn.execute(
            'select vertex from graph where vertex_out @> ARRAY[%s]', vertex
        )
        res = set()
        async for row in rows:
            res.add(row.vertex)

        return res

    async def _descendants(self, vertex, conn=None):
        rows = await conn.execute(
            'select vertex_out from graph where vertex = %s', vertex
        )

        if rows.rowcount == 0:
            return set()

        row = await rows.fetchone()
        return set(row.vertex_out)
