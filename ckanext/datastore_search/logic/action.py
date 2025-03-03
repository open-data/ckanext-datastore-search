from typing import Dict, Any, Union
from ckan.types import Context, DataDict, Action, ChainedAction

from ckan.plugins import toolkit
from ckan.lib.navl.dictization_functions import validate

from ckanext.datastore.logic.schema import datastore_search_schema
from ckanext.datastore_search.backend import (
    DatastoreSearchBackend,
    DatastoreSearchException
)


ignore_missing = toolkit.get_validator('ignore_missing')
ignore_not_sysadmin = toolkit.get_validator('ignore_not_sysadmin')


def datastore_search_create_callback(data_dict: DataDict):
    backend = DatastoreSearchBackend.get_active_backend()
    backend.create_callback(data_dict)


@toolkit.chained_action
def datastore_create(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    try:
        backend.create(dict(func_result))
    except DatastoreSearchException:
        pass
    return func_result


@toolkit.chained_action
def datastore_upsert(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    try:
        backend.upsert(dict(func_result))
    except DatastoreSearchException:
        pass
    return func_result


@toolkit.chained_action
def datastore_delete(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    data_dict['include_records'] = True
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    try:
        backend.delete(dict(func_result))
    except DatastoreSearchException:
        pass
    return func_result


@toolkit.chained_action
def datastore_search(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> Union[ChainedAction,
                                                   Dict[str, Any]]:
    schema = context.get('schema', datastore_search_schema())
    schema['skip_search_engine'] = [ignore_missing, ignore_not_sysadmin]
    data_dict, errors = validate(data_dict, schema, context)
    if errors:
        raise toolkit.ValidationError(errors)
    if data_dict.pop('skip_search_engine', False):
        return up_func(context, data_dict)
    ds_meta = up_func(context, {'resource_id': data_dict.get('resource_id'),
                                'limit': 0})
    backend = DatastoreSearchBackend.get_active_backend()
    try:
        records = backend.search(dict(data_dict))
    except DatastoreSearchException:
        return up_func(context, data_dict)
    return dict(ds_meta, records=records)


@toolkit.chained_action
def datastore_run_triggers(up_func: Action,
                           context: Context,
                           data_dict: DataDict) -> ChainedAction:
    func_result = up_func(context, data_dict)
    backend = DatastoreSearchBackend.get_active_backend()
    try:
        backend.reindex(resource_id=data_dict.get('resource_id'))
    except DatastoreSearchException:
        pass
    return func_result
