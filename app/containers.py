import asyncio
import logging

import dependency_injector.containers as containers
import dependency_injector.providers as providers

from .services import graph as graph_service
from .services.graph.resource import mem, pg


class Core(containers.DeclarativeContainer):
    """IoC container of core component providers."""

    config = providers.Configuration('config')

    logger = providers.Singleton(logging.Logger, name='application')

    loop = providers.Singleton(asyncio.get_event_loop)


class Resources(containers.DeclarativeContainer):

    pg = providers.Singleton(
        pg.PgEngine,
        config=Core.config,
        loop=Core.loop,
    )


class Models(containers.DeclarativeContainer):

    mem_graph = providers.Factory(
        mem.InMemoryGraphModel
    )

    pg_graph = providers.Factory(
        pg.PgGinGraphModel,
        pg_engine=Resources.pg,
    )


class Services(containers.DeclarativeContainer):
    """IoC container of business service providers."""

    graph = providers.Factory(
        graph_service.ServiceRunner,
        config=Core.config,
        log=logging.Logger(name='graph-service'),
        models=Models,
        resources=Resources,
        loop=Core.loop,
    )
