from ckan.types import Context, DataDict, Action, ChainedAction

from ckan.plugins import toolkit

from ckanext.datastore_search.backend import DatastoreSearchBackend


@toolkit.chained_action
def datastore_create(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: create and reload in backend implement
    # records are not returned in datastore_create
    records = data_dict.get('records', None)
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.create(context, dict(func_result, records=records))
    return func_result


@toolkit.chained_action
def datastore_upsert(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: insert into backend implement
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.upsert(context, dict(func_result))
    return func_result


@toolkit.chained_action
def datastore_delete(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: delete from backend implement
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.delete(context, dict(func_result))
    return func_result


@toolkit.chained_action
def datastore_search(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: transform search filters to SOLR filters
    solr_data_dict = dict(data_dict)
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.search(context, solr_data_dict)
    return func_result


@toolkit.chained_action
def datastore_run_triggers(up_func: Action,
                           context: Context,
                           data_dict: DataDict) -> ChainedAction:
    # TODO: re-index after run triggers??
    func_result = up_func(context, data_dict)

    return func_result
