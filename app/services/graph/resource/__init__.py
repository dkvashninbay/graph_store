import abc


class ABCGraphModel(metaclass=abc.ABCMeta):

    _cast = None

    @abc.abstractmethod
    async def init(self):
        pass

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

    def _normalize_edge(self, edge):
        if edge.get('parent', None) is None:
            if self._cast:
                return self._cast(edge['node_id']), None
            else:
                return edge['node_id'], None
        else:
            if self._cast:
                return self._cast(edge['parent']), self._cast(edge['node_id'])
            else:
                return edge['parent'], edge['node_id']
