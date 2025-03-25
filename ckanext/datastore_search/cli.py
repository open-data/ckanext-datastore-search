import click

from typing import Optional

from ckan.plugins.toolkit import ObjectNotFound
from ckanext.datastore_search.backend import (
    DatastoreSearchBackend,
    DatastoreSearchException
)
from ckanext.datastore_search.logic.action import SEARCH_INDEX_SKIP_MSG
from ckanext.datastore_search.utils import (
    get_datastore_count,
    get_datastore_create_dict,
    is_using_pusher,
    get_datastore_tables
)


@click.group(short_help="DataStore Search commands")
def datastore_search():
    """
    DataStore Search commands
    """
    pass


@datastore_search.command(
    short_help="Migrate DataStore Resources into Search Engine Indices.")
@click.option(
    '-r', '--resource-id',
    required=False,
    type=click.STRING,
    default=None,
    help='Resource ID to migrate, otherwise all resources.'
)
@click.option(
    "-v",
    "--verbose",
    is_flag=True,
    default=False,
    help="Increase verbosity"
)
def migrate(resource_id: Optional[str] = None,
            verbose: Optional[bool] = False):
    """
    Migrate DataStore Resources into Search Engine Indices.
    """
    backend = DatastoreSearchBackend.get_active_backend()
    if not resource_id:
        ds_resource_ids = get_datastore_tables()
        if not ds_resource_ids:
            click.echo("No DataStore Resources exist. Exiting...")
            return
        if verbose:
            click.echo("Gathered %s table names from the DataStore." %
                       len(ds_resource_ids))
    else:
        try:
            get_datastore_count(resource_id)
        except ObjectNotFound:
            click.echo('Resource not found or not a DataStore resource: %s' %
                       resource_id)
        ds_resource_ids = [resource_id]
    for ds_resource_id in ds_resource_ids:
        if (
            not backend.only_use_engine
            and (
                ds_count := get_datastore_count(ds_resource_id) <
                backend.min_rows_for_index
            )
        ):
            # do not try to create search index if not enough rows
            if verbose:
                click.echo(SEARCH_INDEX_SKIP_MSG %
                           (ds_resource_id,
                            ds_count,
                            backend.min_rows_for_index))
            continue
        create_dict = get_datastore_create_dict(
            ds_resource_id, upload_file=is_using_pusher(ds_resource_id))
        try:
            backend.create(create_dict)
            if verbose:
                click.echo('Migrated DataStore Resource %s into search index' %
                           ds_resource_id)
        except DatastoreSearchException:
            pass
    click.echo('Done!')
