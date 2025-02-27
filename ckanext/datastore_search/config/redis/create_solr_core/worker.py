import rq
from redis import Redis, ConnectionPool
import click
from logging import getLogger

from typing import Dict, Optional, Tuple


log = getLogger(__name__)

_solr_queues: Dict[str, rq.Queue] = {}
_redis_connection_pool = None


@click.command(short_help="Work the given queues.")
@click.option('-r', '--redis-url', required=True,
              type=click.STRING, default=None,
              help='The REDIS connection URI.')
@click.option('-q', '--queues', default=None, required=True,
              multiple=True, help='Queue names to work on.')
@click.option('-b', '--burst', is_flag=True, default=False,
              type=click.BOOL, help='Burst the worker.')
@click.option('-v', '--verbose', is_flag=True, default=False,
              type=click.BOOL, help='Increase verbosity.')
def _worker(redis_url: Optional[str] = None,
            queues: Optional[Tuple[str]] = None,
            burst: bool = False,
            verbose: bool = False):
    """
    Work the given queues.
    """
    global _solr_queues
    global _redis_connection_pool

    if _redis_connection_pool is None:
        if verbose:
            click.echo('Using Redis at %s' % redis_url)
        _redis_connection_pool = ConnectionPool.from_url(redis_url)
    redis_conn = Redis(connection_pool=_redis_connection_pool)

    redis_queues = {}
    for queue in queues:
        if queue in _solr_queues:
            redis_queue = _solr_queues[queue]
        else:
            redis_queue = _solr_queues[queue] = \
                rq.Queue(queue, connection=redis_conn)
        redis_queues[queue] = redis_queue

    worker = rq.Worker(queues=redis_queues.values(),
                       connection=redis_conn)
    worker.work(burst=burst)


if __name__ == '__main__':
    _worker()
