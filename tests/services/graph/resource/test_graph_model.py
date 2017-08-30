import asyncio
import unittest

from app.lib.graph import AcyclicDiGraph, DiGraph, InconsistentState
from app.services.graph.resource import mem, pg


class BaseGraphModelMix:

    cast = int

    def setUp(self):
        self.loop = asyncio.new_event_loop()
        self.graph_model = None

    def tearDown(self):
        self.loop.close()

    def test_insert_subtrees(self):
        graph_model = self.graph_model

        self.loop.run_until_complete(
            graph_model.insert(
                [
                    {'parent': 0, 'node_id': 1},
                ]
            )
        )

        self.loop.run_until_complete(
            graph_model.insert(
                [
                    {'parent': 0, 'node_id': 2},
                    {'parent': 1, 'node_id': 3},
                    {'parent': 2, 'node_id': 3},
                    {'parent': 3, 'node_id': 4},
                ]
            )
        )

        # emulate cycle
        with self.assertRaises(InconsistentState):
            self.loop.run_until_complete(
                graph_model.insert(
                    [{'parent': 4, 'node_id': 3}, ]
                ),
            )

        # emulate batch insert cycle
        with self.assertRaises(InconsistentState):
            self.loop.run_until_complete(
                graph_model.insert(
                    [
                        {'parent': 2, 'node_id': 4},
                        {'parent': 4, 'node_id': 3},
                    ]
                )
            )

        # subtrees
        expected_subtrees = {
            (0, 1, 3, 4),
            (0, 2, 3, 4),
        }

        subtrees = list(
            self.loop.run_until_complete(graph_model.trees(self.cast(3)))
        )
        self.assertEqual(len(subtrees), len(expected_subtrees))

        for subtree in subtrees:
            self.assertTrue(
                tuple(map(int, subtree)) in expected_subtrees
            )


class TestInMemoryGraphModel(BaseGraphModelMix, unittest.TestCase):

    def setUp(self):
        super().setUp()

        self.graph_model = mem.InMemoryGraphModel(
            AcyclicDiGraph(DiGraph())
        )

    def tearDown(self):
        super().tearDown()


class TestPgGinGraphModel(BaseGraphModelMix, unittest.TestCase):
    cast = str

    def setUp(self):
        super().setUp()

        config = {
            'postgres': {
                'database': 'graph_service',
                'user': 'graph_service',
                'password': 'graph_service',
                'host': 'localhost',
                'port': 5432,
                'minsize': 1,
                'maxsize': 5,
            },
        }

        self.engine = pg.PgEngine(
            config,
            self.loop,
        )
        self.loop.run_until_complete(self.engine.init_engine())
        self.graph_model = pg.PgGinGraphModel(self.engine)
        self.loop.run_until_complete(self.graph_model.init())

    def tearDown(self):
        self.loop.run_until_complete(self.engine.close())

        super().tearDown()
