import abc
from typing import Iterator

from ....lib.graph import AcyclicDiGraph, DiGraph


class ABCGraphModel(metaclass=abc.ABCMeta):

    @abc.abstractmethod
    async def insert(self, vertices):
        pass

    @abc.abstractmethod
    async def edges(self):
        pass

    @abc.abstractmethod
    async def has_edge(self, edge):
        pass

    @abc.abstractmethod
    async def trees(self, edge):
        pass


class InMemoryGraphModel(ABCGraphModel):

    def __init__(self, graph: AcyclicDiGraph = None):
        self.graph = graph or AcyclicDiGraph()
        self.inv_graph = self.graph.reverse()

    async def edges(self):
        return self.graph.edges()

    async def has_edge(self, edge):
        return self.graph.has_edge(edge)

    async def insert(self, vertices):
        if len(vertices) == 1:
            vertice = vertices.pop()

            if vertice.get('parent', None) is None:
                edge_from, edge_to = vertice['node_id'], None
            else:
                edge_from, edge_to = vertice['parent'], vertice['node_id']

            self.graph.insert(edge_from, edge_to)
            self.inv_graph.insert(edge_to, edge_from)
        else:
            tmp = AcyclicDiGraph(DiGraph())

            for vertice in vertices:
                if vertice.get('parent', None) is None:
                    edge_from, edge_to = vertice['node_id'], None
                else:
                    edge_from, edge_to = vertice['parent'], vertice['node_id']
                tmp.insert(edge_from, edge_to)

            self.graph.union(tmp)
            self.inv_graph.union(tmp.reverse())

    async def trees(self, edge) -> Iterator[list]:

        def collect_subtrees(
            graph: AcyclicDiGraph,
            start_node,
            stack=None,
        ) -> Iterator[list]:
            stack = stack or []

            stack.append(start_node)

            edges_out = graph.vertices(start_node)
            if len(edges_out) == 0:
                yield stack.copy()
                stack.pop()
                return

            for edge_out in edges_out:
                yield from collect_subtrees(graph, edge_out, stack)

            stack.pop()

        child_subtrees = list(collect_subtrees(self.graph, edge))
        parent_subtrees = list(collect_subtrees(self.inv_graph, edge))

        result = []
        for parent_subtree in parent_subtrees:
            parent_subtree = list(reversed(parent_subtree))
            parent_subtree.pop()

            for child_subtree in child_subtrees:
                result.append(parent_subtree + child_subtree)

        return result
