from ckan.types import Context, DataDict, Action, ChainedAction

from ckan.plugins import toolkit
from ckan.lib.navl.dictization_functions import validate

from ckanext.datastore.logic.schema import datastore_search_schema
from ckanext.datastore_search.backend import DatastoreSearchBackend

from pprint import pprint
from logging import getLogger
log = getLogger(__name__)

ignore_missing = toolkit.get_validator('ignore_missing')
ignore_not_sysadmin = toolkit.get_validator('ignore_not_sysadmin')


@toolkit.chained_action
def datastore_create(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: create and reload in backend implement
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.create(context, dict(func_result))
    return func_result


@toolkit.chained_action
def datastore_upsert(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: insert into backend implement
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.upsert(context, dict(func_result))
    return func_result


@toolkit.chained_action
def datastore_delete(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: delete from backend implement
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.delete(context, dict(func_result))
    return func_result


@toolkit.chained_action
def datastore_search(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    # TODO: transform search filters to SOLR filters
    schema = context.get('schema', datastore_search_schema())
    schema['skip_solr'] = [ignore_missing, ignore_not_sysadmin]
    data_dict, errors = validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)
    if data_dict.pop('skip_solr', False):
        return up_func(context, data_dict)
    meta_data_dict = {'resource_id': data_dict.get('resource_id'),
                      'limit': 0}
    ds_meta = up_func(context, meta_data_dict)
    solr_data_dict = dict(data_dict)
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    backend.search(context, solr_data_dict)
    # TODO: build the result dict with psql meta and solr records
    return func_result


@toolkit.chained_action
def datastore_run_triggers(up_func: Action,
                           context: Context,
                           data_dict: DataDict) -> ChainedAction:
    # TODO: re-index after run triggers??
    func_result = up_func(context, data_dict)

    return func_result
