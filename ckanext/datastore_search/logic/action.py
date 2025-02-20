from ckan.types import Context, DataDict, Action, ChainedAction

from ckan.plugins import toolkit

from ckanext.datastore_search.backend import DatastoreSearchBackend

from pprint import pprint
from logging import getLogger
log = getLogger(__name__)


@toolkit.chained_action
def datastore_create(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: create and reload in backend implement
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.create(context, func_result)
    return func_result


@toolkit.chained_action
def datastore_upsert(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: insert into backend implement
    func_result = up_func(context, data_dict)

    return func_result


@toolkit.chained_action
def datastore_delete(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: delete from backend implement
    func_result = up_func(context, data_dict)

    return func_result


@toolkit.chained_action
def datastore_search(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: transform search filters to SOLR filters
    func_result = up_func(context, data_dict)

    return func_result


@toolkit.chained_action
def datastore_run_triggers(up_func: Action,
                           context: Context,
                           data_dict: DataDict) -> ChainedAction:
    # TODO: re-index after run triggers??
    func_result = up_func(context, data_dict)

    return func_result
