import asyncio
import logging

import dependency_injector.containers as containers
import dependency_injector.providers as providers

from .services import graph as graph_service
from .services.graph.resource.graph_model import InMemoryGraphModel


class Core(containers.DeclarativeContainer):
    """IoC container of core component providers."""

    config = providers.Configuration('config')

    logger = providers.Singleton(logging.Logger, name='application')

    loop = providers.Singleton(asyncio._get_running_loop)


class Resources(containers.DeclarativeContainer):

    mem_graph = providers.Factory(
        InMemoryGraphModel
    )


class Services(containers.DeclarativeContainer):
    """IoC container of business service providers."""

    graph = providers.Factory(
        graph_service.ServiceRunner,
        config=Core.config,
        log=logging.Logger(name='graph-service'),
        resources=Resources,
        loop=Core.loop,
    )
