from aiohttp import web

from .handlers import NodesHandler


class ServiceRunner:

    def __init__(self, config, log, resources, loop):
        self.resources = resources
        self.loop = loop
        self.log = log
        self.config = config

    def _on_startup(self):
        pass

    def _on_close(self):
        pass

    def _handler(self) -> NodesHandler:
        # if self.config['config']['db'] == 'mem':
        return NodesHandler(
            self.resources.mem_graph(),
            log=self.log
        )

    def _routes(self):
        handler = self._handler()

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

    def _middleware(self):
        pass

    def run(self):
        self.app = web.Application(loop=self.loop)

        self._on_startup()
        self._on_close()
        self._routes()
        self._middleware()

        web.run_app(
            self.app,
            host=self.config['api']['host'],
            port=self.config['api']['port'],
        )
