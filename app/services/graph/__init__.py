from aiohttp import web

from .handlers import NodesHandler


class ServiceRunner:

    def __init__(self, config, log, models, resources, loop):
        self.resources = resources
        self.models = models
        self.loop = loop
        self.log = log
        self.config = config

    async def _init_resources(self):
        if self.config['db'] == 'mem':
            self._model = self.models.mem_graph()
        elif self.config['db'] == 'pg':
            # initi pg
            pg = self.resources.pg()
            await pg.init_engine()

            self._model = self.models.pg_graph()

    async def _on_close(self):
        pass

    async def _routes(self):
        handler = NodesHandler(
            self._model,
            log=self.log
        )

        self.app.router.add_post(
            '/nodes',
            handler.post_nodes,
        )
        self.app.router.add_get(
            '/nodes',
            handler.get_vertexes,
        )
        self.app.router.add_get(
            '/nodes/{node_id}/trees',
            handler.get_node_trees,
        )

    async def _middleware(self):
        pass

    async def init_app(self):
        await self._init_resources()
        await self._on_close()
        await self._routes()
        await self._middleware()

    def run(self):
        self.app = web.Application(loop=self.loop)

        self.loop.run_until_complete(self.init_app())

        web.run_app(
            self.app,
            host=self.config['api']['host'],
            port=self.config['api']['port'],
        )
