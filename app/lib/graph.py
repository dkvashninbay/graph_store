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
    def insert(self, edge_from, edge_to):
        pass

    @abc.abstractmethod
    def has_edge(self, edge) -> bool:
        pass

    @abc.abstractmethod
    def has_vertice(self, edge_from, edge_to) -> bool:
        pass

    @abc.abstractmethod
    def vertices(self, edge) -> set:
        pass

    @abc.abstractmethod
    def edges(self) -> set:
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

    def __init__(self, vertices=None):
        self.vtx = vertices or {}

        self._edges = set(self.vtx.keys())

        if len(self.vtx):
            for edges in self.vtx.values():
                self._edges = self._edges | edges

    def insert(self, edge_from, edge_to):
        if edge_to is None:
            self.vtx[edge_from] = self._sentinel
            self._edges.add(edge_from)
        else:
            self.vtx[edge_from] = set(self.vtx.get(edge_from, set()))
            self.vtx[edge_from].add(edge_to)

            self._edges.add(edge_from)
            self._edges.add(edge_to)

    def has_edge(self, edge) -> bool:
        return edge in self._edges

    def has_vertice(self, edge_from, edge_to) -> bool:
        return edge_from in self.vtx and edge_to in self.vtx[edge_from]

    def vertices(self, edge) -> set:
        return self.vtx.get(edge, self._sentinel)

    def edges(self) -> set:
        return self._edges

    def union(self, other: 'ABCGraph') -> 'DiGraph':
        for edge in other.edges():
            self._edges.add(edge)
            self._edges = self._edges | other.vertices(edge)

            if edge in self.vtx:
                self.vtx[edge] = self.vtx[edge] | (other.vertices(edge))
            else:
                self.vtx[edge] = other.vertices(edge).copy()

        return self

    def __len__(self):
        return len(self.vtx)

    def __copy__(self):
        return DiGraph(self.vtx.copy())

    def reverse(self) -> 'DiGraph':
        tmp = DiGraph()

        for edge_from in self.edges():
            for edge_to in self.vertices(edge_from):
                tmp.insert(edge_to, edge_from)

        return tmp


class AcyclicDiGraph(ABCGraph):

    def __init__(self, di_graph=None):
        self.di_graph = di_graph or DiGraph()

        if di_graph:
            if self._has_cycle(
                self.vertices,
                filter(
                    lambda edge: len(self.vertices(edge)) > 0,
                    self.di_graph.edges(),
                ),
                seen=set()
            ):
                raise InconsistentState()

    def __len__(self):
        return len(self.di_graph)

    def __copy__(self):
        return AcyclicDiGraph(copy.copy(self.di_graph))

    def has_edge(self, edge) -> bool:
        return self.di_graph.has_edge(edge)

    def edges(self) -> set:
        return self.di_graph.edges()

    def has_vertice(self, edge_from, edge_to) -> bool:
        return self.di_graph.has_vertice(edge_from, edge_to)

    def vertices(self, edge)-> set:
        return self.di_graph.vertices(edge)

    def insert(self, edge_from, edge_to):
        if self.has_vertice(edge_from, edge_to):
            return

        edges_to = self.vertices(edge_from) | {edge_to}
        if self._has_cycle(
            lambda e: edges_to if e == edge_from else self.vertices(e),
            {edge_from},
            seen=set()
        ):
            raise InconsistentState(
                'Cycle for {} -> {}'.format(edge_from, edge_to),
            )

        self.di_graph.insert(edge_from, edge_to)

    def union(self, other: ABCGraph) -> 'AcyclicDiGraph':
        if self._has_cycle(
            lambda e: self.vertices(e) | other.vertices(e),
            set(filter(
                lambda other_edge: len(other.vertices(other_edge)) > 0,
                other.edges(),
            )),
            seen=set()
        ):
            raise InconsistentState()

        self.di_graph.union(other)

        return self

    def _has_cycle(self, out_edges, from_edges, seen):
        if not from_edges:
            return False

        for from_edge in from_edges:
            seen.add(from_edge)

            for out_edge in out_edges(from_edge):
                if out_edge in seen:
                    return True

                seen.add(out_edge)
                if self._has_cycle(out_edges, out_edges(out_edge), seen):
                    return True
                seen.remove(out_edge)
            seen.remove(from_edge)

        return False

    def reverse(self) -> 'AcyclicDiGraph':
        return AcyclicDiGraph(self.di_graph.reverse())
