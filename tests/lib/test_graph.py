import unittest

from app.lib.graph import AcyclicDiGraph, DiGraph, InconsistentState

from ..utils import data_provider


class TestDiGraph(unittest.TestCase):

    def init_data_provider(self):
        return [
            (
                DiGraph(),
                [(0, 1), (0, 2)],
                {0, 1, 2},
                [(0, True), (1, True), (2, True), (3, False)],
                [((0, 1), True), ((0, 2), True), ((1, 2), False)],
                None,
                {1: {0}, 2: {0}, 0: DiGraph._sentinel},
            ),

            (
                DiGraph({0: {1, 2}}),
                [],
                {0, 1, 2},
                [(0, True), (1, True), (2, True), (3, False)],
                [((0, 1), True), ((0, 2), True), ((1, 2), False)],
                None,
                {1: {0}, 2: {0}, 0: DiGraph._sentinel},
            ),

            (
                DiGraph({0: {1, 2}, 1: {0}}),
                [],
                {0, 1, 2},
                [(0, True), (1, True), (2, True), (3, False)],
                [
                    ((0, 1), True),
                    ((0, 2), True),
                    ((1, 2), False),
                    ((1, 0), True),
                ],
                InconsistentState,
                {0: {1}, 1: {0}, 2: {0}}
            ),
        ]

    @data_provider(init_data_provider)
    def test_init(
        self,
        graph: DiGraph,
        vertices,
        edges,
        has_edges,
        has_vertices,
        exception,
        inverse_graph,
    ):
        for f, t in vertices:
            graph.insert(f, t)

        self.assertEquals(edges, graph.edges())

        for edge, has_edge in has_edges:
            self.assertEquals(has_edge, graph.has_edge(edge))

        for vertice, has_vertice in has_vertices:
            self.assertEquals(has_vertice, graph.has_vertice(*vertice))

        inv_graph = graph.reverse()
        for edge in inv_graph.edges():
            self.assertEqual(
                inverse_graph[edge],
                inv_graph.vertices(edge),
            )

    @data_provider(init_data_provider)
    def test_init_acycle(
        self,
        graph: DiGraph,
        vertices,
        edges,
        has_edges,
        has_vertices,
        exception,
        inverse_graph,
    ):
        if exception:
            with self.assertRaises(exception):
                graph = AcyclicDiGraph(graph)

        for f, t in vertices:
            graph.insert(f, t)

        self.assertEquals(edges, graph.edges())

        for edge, has_edge in has_edges:
            self.assertEquals(has_edge, graph.has_edge(edge))

        for vertice, has_vertice in has_vertices:
            self.assertEquals(has_vertice, graph.has_vertice(*vertice))

        inv_graph = graph.reverse()
        for edge in inv_graph.edges():
            self.assertEqual(
                inverse_graph[edge],
                inv_graph.vertices(edge),
            )

    def union_data_provider(self):
        return [
            (
                DiGraph({1: {2, 3}, 2: {4}}),
                DiGraph({3: {4}, 4: {5}}),
                {
                    1: {2, 3},
                    2: {4},
                    3: {4},
                    4: {5},
                    5: DiGraph._sentinel,
                },
                None,
            ),

            (
                DiGraph({1: {2, 3}, 2: {4}}),
                DiGraph({3: {4}, 4: {5}, 2: {1}}),
                {
                    1: {2, 3},
                    2: {4, 1},
                    3: {4},
                    4: {5},
                    5: DiGraph._sentinel,
                },
                InconsistentState,
            ),
        ]

    @data_provider(union_data_provider)
    def test_union(self, a: DiGraph, b: DiGraph, vertices, exception):
        a.union(b)

        self.assertEquals(set(vertices.keys()), a.edges())
        for edge, vertices in vertices.items():
            self.assertEquals(a.vertices(edge), vertices)

    @data_provider(union_data_provider)
    def test_union_acycle(self, a: DiGraph, b: DiGraph, vertices, exception):
        a = AcyclicDiGraph(a)
        b = AcyclicDiGraph(b)

        if exception:
            with self.assertRaises(exception):
                a.union(b)
        else:
            a.union(b)

            self.assertEquals(set(vertices.keys()), a.edges())
            for edge, vertices in vertices.items():
                self.assertEquals(a.vertices(edge), vertices)
