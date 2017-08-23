import abc
from typing import Iterator

from ....lib.graph import AcyclicDiGraph, DiGraph


class ABCGraphModel(metaclass=abc.ABCMeta):

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


class InMemoryGraphModel(ABCGraphModel):

    def __init__(self, graph: AcyclicDiGraph = None):
        self.graph = graph or AcyclicDiGraph()
        self.inv_graph = self.graph.reverse()

    async def vertexes(self):
        return self.graph.vertexes()

    async def has_vertex(self, edge):
        return self.graph.has_vertex(edge)

    async def insert(self, edges):
        if len(edges) == 1:
            edge = edges.pop()

            if edge.get('parent', None) is None:
                v_from, v_to = edge['node_id'], None
            else:
                v_from, v_to = edge['parent'], edge['node_id']

            self.graph.insert(v_from, v_to)
            self.inv_graph.insert(v_to, v_from)
        else:
            tmp = AcyclicDiGraph(DiGraph())

            for edge in edges:
                if edge.get('parent', None) is None:
                    v_from, v_to = edge['node_id'], None
                else:
                    v_from, v_to = edge['parent'], edge['node_id']
                tmp.insert(v_from, v_to)

            self.graph.union(tmp)
            self.inv_graph.union(tmp.reverse())

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
