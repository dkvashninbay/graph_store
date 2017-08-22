import asyncio
import unittest

from app.lib.graph import AcyclicDiGraph, DiGraph, InconsistentState
from app.services.graph.resource.graph_model import InMemoryGraphModel


class TestInMemoryGraphModel(unittest.TestCase):
    def setUp(self):
        self.loop = asyncio.get_event_loop()

    def tearDown(self):
        self.loop.close()

    def test_insert_subtrees(self):
        graph_model = InMemoryGraphModel(
            AcyclicDiGraph(DiGraph())
        )

        self.loop.run_until_complete(
            graph_model.insert(
                [
                    {'parent': 0, 'node_id': 1},
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

        subtrees = list(self.loop.run_until_complete(graph_model.trees(3)))
        self.assertEqual(len(subtrees), len(expected_subtrees))

        for subtree in subtrees:
            self.assertTrue(
                tuple(subtree) in expected_subtrees
            )
