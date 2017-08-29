import abc
from functools import partial
from typing import Iterator

from aiopg import sa

from ....lib.graph import AcyclicDiGraph, DiGraph, InconsistentState


class ABCGraphModel(metaclass=abc.ABCMeta):

    _cast = None

    @abc.abstractmethod
    async def init(self):
        pass

    @abc.abstractmethod
    async def insert(self, vertexes):
        pass

    @abc.abstractmethod
    async def vertexes(self):
        pass

    @abc.abstractmethod
    async def has_vertex(self, vertex):
        pass

    @abc.abstractmethod
    async def trees(self, vertex):
        pass

    def _normalize_edge(self, edge):
        if edge.get('parent', None) is None:
            if self._cast:
                return self._cast(edge['node_id']), None
            else:
                return edge['node_id'], None
        else:
            if self._cast:
                return self._cast(edge['parent']), self._cast(edge['node_id'])
            else:
                return edge['parent'], edge['node_id']


class InMemoryGraphModel(ABCGraphModel):

    def __init__(self, graph: AcyclicDiGraph = None):
        self.graph = graph or AcyclicDiGraph()
        self.inv_graph = self.graph.reverse()

    async def init(self):
        pass

    async def vertexes(self):
        return self.graph.vertexes()

    async def has_vertex(self, edge):
        return self.graph.has_vertex(edge)

    async def insert(self, edges):
        if len(edges) == 1:
            await self._insert_one(edges.pop())
        elif len(edges) > 1:
            await self._insert_many(edges)

    async def _insert_one(self, edge):
        v_from, v_to = self._normalize_edge(edge)

        self.graph.insert(v_from, v_to)
        self.inv_graph.insert(v_to, v_from, strict=False)

    async def _insert_many(self, edges):
        tmp = AcyclicDiGraph(DiGraph())

        for edge in edges:
            v_from, v_to = self._normalize_edge(edge)
            tmp.insert(v_from, v_to)

        self.graph.union(tmp)
        self.inv_graph.union(tmp.reverse(), strict=False)

    async def trees(self, vertex) -> Iterator[list]:

        def collect_subtrees(
                graph: AcyclicDiGraph,
                start_node,
                stack=None,
        ) -> Iterator[list]:
            stack = stack or []

            stack.append(start_node)

            vs_out = graph.vertexes_to(start_node)
            if len(vs_out) == 0:
                yield stack.copy()
                stack.pop()
                return

            for v_out in vs_out:
                yield from collect_subtrees(graph, v_out, stack)

            stack.pop()

        child_subtrees = list(collect_subtrees(self.graph, vertex))
        parent_subtrees = list(collect_subtrees(self.inv_graph, vertex))

        result = []
        for parent_subtree in parent_subtrees:
            parent_subtree = list(reversed(parent_subtree))
            parent_subtree.pop()

            for child_subtree in child_subtrees:
                result.append(parent_subtree + child_subtree)

        return result


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
                "select vertex from graph where vertex == %s", vertex
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
        tmp = AcyclicDiGraph(DiGraph())

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
