import abc
import copy


class GraphException(Exception):
    pass


class InconsistentState(GraphException):
    pass


class ABCGraph(metaclass=abc.ABCMeta):

    @classmethod
    def merge(
        cls,
        di_graph_a: 'ABCGraph',
        di_graph_b: 'ABCGraph',
    ) -> 'ABCGraph':
        if len(di_graph_a) > len(di_graph_b):
            a, b = (copy.copy(di_graph_a), di_graph_b)
        else:
            a, b = (copy.copy(di_graph_b), di_graph_a)

        return a.union(b)

    @abc.abstractmethod
    def insert(self, v_from, v_to):
        pass

    @abc.abstractmethod
    def has_vertex(self, vertex) -> bool:
        pass

    @abc.abstractmethod
    def has_edge(self, v_from, v_to) -> bool:
        pass

    @abc.abstractmethod
    def vertexes_to(self, vertex) -> set:
        pass

    @abc.abstractmethod
    def vertexes(self) -> set:
        pass

    @abc.abstractmethod
    def union(self, other) -> 'ABCGraph':
        pass

    @abc.abstractmethod
    def __len__(self):
        pass

    @abc.abstractmethod
    def __copy__(self):
        pass

    @abc.abstractmethod
    def reverse(self) -> 'ABCGraph':
        pass


class DiGraph(ABCGraph):

    _sentinel = frozenset()

    def __init__(self, edges=None):
        self.edges = edges or {}

        self._vtxs = set(self.edges.keys())

        if len(self.edges):
            for _edges in self.edges.values():
                self._vtxs = self._vtxs | _edges

    def insert(self, e_from, e_to):
        if e_to is None:
            self.edges[e_from] = self._sentinel
            self._vtxs.add(e_from)
        else:
            self.edges[e_from] = set(self.edges.get(e_from, set()))
            self.edges[e_from].add(e_to)

            self._vtxs.add(e_from)
            self._vtxs.add(e_to)

    def has_vertex(self, vertex) -> bool:
        return vertex in self._vtxs

    def has_edge(self, v_from, v_to) -> bool:
        return v_from in self.edges and v_to in self.edges[v_from]

    def vertexes_to(self, vertex) -> set:
        return self.edges.get(vertex, self._sentinel)

    def vertexes(self) -> set:
        return self._vtxs

    def union(self, other: 'ABCGraph') -> 'DiGraph':
        for edge in other.vertexes():
            self._vtxs.add(edge)
            self._vtxs = self._vtxs | other.vertexes_to(edge)

            if edge in self.edges:
                self.edges[edge] = self.edges[edge] | (other.vertexes_to(edge))
            else:
                self.edges[edge] = other.vertexes_to(edge).copy()

        return self

    def __len__(self):
        return len(self.edges)

    def __copy__(self):
        return DiGraph(self.edges.copy())

    def reverse(self) -> 'DiGraph':
        tmp = DiGraph()

        for v_from in self.vertexes():
            for v_to in self.vertexes_to(v_from):
                tmp.insert(v_to, v_from)

        return tmp


class AcyclicDiGraph(ABCGraph):

    def __init__(self, di_graph=None):
        self.di_graph = di_graph or DiGraph()

        if di_graph:
            if self.has_cycle(
                self.vertexes_to,
                filter(
                    lambda edge: len(self.vertexes_to(edge)) > 0,
                    self.di_graph.vertexes(),
                ),
                seen=set()
            ):
                raise InconsistentState()

    def __len__(self):
        return len(self.di_graph)

    def __copy__(self):
        return AcyclicDiGraph(copy.copy(self.di_graph))

    def has_vertex(self, vertex) -> bool:
        return self.di_graph.has_vertex(vertex)

    def vertexes(self) -> set:
        return self.di_graph.vertexes()

    def has_edge(self, v_from, v_to) -> bool:
        return self.di_graph.has_edge(v_from, v_to)

    def vertexes_to(self, vertex)-> set:
        return self.di_graph.vertexes_to(vertex)

    def insert(self, v_from, v_to, strict=True):
        if self.has_edge(v_from, v_to):
            return

        vtx_to = self.vertexes_to(v_from) | {v_to}
        if strict and self.has_cycle(
            lambda e: vtx_to if e == v_from else self.vertexes_to(e),
            {v_from},
            seen=set()
        ):
            raise InconsistentState(
                'Cycle for {} -> {}'.format(v_from, v_to),
            )

        self.di_graph.insert(v_from, v_to)

    def union(self, other: ABCGraph, strict=True) -> 'AcyclicDiGraph':
        if strict and self.has_cycle(
            lambda e: self.vertexes_to(e) | other.vertexes_to(e),
            set(filter(
                lambda other_edge: len(other.vertexes_to(other_edge)) > 0,
                other.vertexes(),
            )),
            seen=set()
        ):
            raise InconsistentState()

        self.di_graph.union(other)

        return self

    @classmethod
    def has_cycle(cls, out_vs, from_vs, seen):
        if not from_vs:
            return False

        for from_edge in from_vs:
            seen.add(from_edge)

            for out_edge in out_vs(from_edge):
                if out_edge in seen:
                    return True

                seen.add(out_edge)
                if cls.has_cycle(out_vs, out_vs(out_edge), seen):
                    return True
                seen.remove(out_edge)
            seen.remove(from_edge)

        return False

    @classmethod
    async def ahas_cycle(cls, out_vs, from_vs, seen):
        if not from_vs:
            return False

        for from_edge in from_vs:
            seen.add(from_edge)

            for out_edge in await out_vs(from_edge):
                if out_edge in seen:
                    return True

                seen.add(out_edge)
                if await cls.ahas_cycle(out_vs, await out_vs(out_edge), seen):
                    return True
                seen.remove(out_edge)
            seen.remove(from_edge)

        return False

    def reverse(self) -> 'AcyclicDiGraph':
        return AcyclicDiGraph(self.di_graph.reverse())
