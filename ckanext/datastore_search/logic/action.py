from logging import getLogger

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

log = getLogger(__name__)
DEBUG = toolkit.config.get('debug', False)
SEARCH_INDEX_SKIP_MSG = 'Skipping search index for DataStore Resource %s. ' \
                        '%s rows is less than minimum requirement %s'


def datastore_search_create_callback(data_dict: DataDict):
    """
    Callback action for REDIS queue processing.

    NOTE: This method does not get registered as a CKAN action.
          It is only ever called from the REDIS Python library.
    """
    backend = DatastoreSearchBackend.get_active_backend()
    backend.create_callback(data_dict)


def _get_datastore_count(context: Context, resource_id: str) -> int:
    ds_result = toolkit.get_action('datastore_search')(
        context, {'resource_id': resource_id, 'limit': 0,
                  'include_total': True,
                  'skip_search_engine': True})
    return int(ds_result['total'])


@toolkit.chained_action
def datastore_create(up_func: Action,
                     context: Context,
                     data_dict: DataDict) -> ChainedAction:
    data_dict['include_records'] = True
    backend = DatastoreSearchBackend.get_active_backend()
    if backend.only_use_engine:
        records = data_dict.pop('records', None)
    func_result = up_func(context, data_dict)
    if backend.only_use_engine:
        func_result['records'] = records
    elif ds_count := _get_datastore_count(context, data_dict['resource_id']) < \
        backend.min_rows_for_index:
            if DEBUG:
                log.debug(SEARCH_INDEX_SKIP_MSG %
                          (data_dict['resource_id'],
                           ds_count,
                           backend.min_rows_for_index))
            return func_result
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
    backend = DatastoreSearchBackend.get_active_backend()
    if backend.only_use_engine:
        data_dict['dry_run'] = True
    func_result = up_func(context, data_dict)
    if not backend.only_use_engine and \
        (ds_count := _get_datastore_count(context, data_dict['resource_id'])) < \
        backend.min_rows_for_index:
            if DEBUG:
                log.debug(SEARCH_INDEX_SKIP_MSG %
                          (data_dict['resource_id'],
                           ds_count,
                           backend.min_rows_for_index))
            return func_result
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
    backend = DatastoreSearchBackend.get_active_backend()
    func_result = up_func(context, data_dict)
    try:
        backend.delete(func_result)
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
    backend = DatastoreSearchBackend.get_active_backend()
    if not backend.only_use_engine and data_dict.pop('skip_search_engine', False):
        return up_func(context, data_dict)
    ds_meta = up_func(context, {'resource_id': data_dict.get('resource_id'),
                                'limit': 0})
    # FIXME: raise DatastoreSearchException if count less than min_rows_for_index??
    try:
        records = backend.search(dict(data_dict))
    except DatastoreSearchException:
        if not backend.only_use_engine:
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
