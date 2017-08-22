from logging import Logger

from aiohttp import web

from ...lib.graph import InconsistentState
from .resource.graph_model import ABCGraphModel
from .trafarets import NodesTrafaret


class NodesHandler:

    def __init__(self, graph: ABCGraphModel, log: Logger):
        self.log = log
        self.graph = graph

    async def post_nodes(self, request):
        data = await request.json()

        data = NodesTrafaret.check(data)

        try:
            await self.graph.insert(data['nodes'])
        except InconsistentState as e:
            self.log.exception(e)
            return web.HTTPUnprocessableEntity(reason=e)

        return web.HTTPOk()

    async def get_edges(self, request):
        return web.json_response(data=list(await self.graph.edges()))

    async def get_node_trees(self, request):
        edge = request.match_info['node_id']

        if not await self.graph.has_edge(edge):
            self.log.warning('{edge} not found'.format(edge=edge))
            return web.HTTPNotFound()

        try:
            trees_json = list(await self.graph.trees(edge))
        except Exception as e:
            self.log.exception(e)
            return web.HTTPInternalServerError()

        return web.json_response(data={'trees': trees_json})
