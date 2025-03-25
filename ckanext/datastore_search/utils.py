from sys import maxsize as INT_MAX

from typing import cast, List
from ckan.types import Context, DataDict

from ckan.plugins.toolkit import get_action

from ckanext.datastore_search.backend import DatastoreSearchBackend


def get_site_context() -> Context:
    """
    Return a CKAN Context for the system user.
    """
    site_user = get_action('get_site_user')({'ignore_auth': True}, {})
    return cast(Context, {'user': site_user['name']})


def get_datastore_tables() -> List[str]:
    """
    Returns a list of resource ids (table names) from the DataStore database.
    """
    tables = get_action('datastore_search')(get_site_context(),
                                            {"resource_id": "_table_metadata",
                                             "offset": 0,
                                             "limit": INT_MAX})
    if not tables:
        return []
    return [r.get('name') for r in tables.get('records', [])]


def get_datastore_create_dict(resource_id: str,
                              upload_file: bool = False) -> DataDict:
    backend = DatastoreSearchBackend.get_active_backend()
    ds_result = get_action('datastore_search')(
        get_site_context(),
        {'resource_id': resource_id,
         'limit': 0,
         'skip_search_engine': True})
    return {
        'resource_id': resource_id,
        'fields': [f for f in ds_result['fields'] if
                   f['id'] not in backend.default_search_fields],
        'upload_file': upload_file}


def get_datastore_count(resource_id: str) -> int:
    ds_result = get_action('datastore_search')(
        get_site_context(),
        {'resource_id': resource_id,
         'limit': 0,
         'include_total': True,
         'skip_search_engine': True})
    return int(ds_result['total'])


def is_using_pusher(resource_id: str) -> bool:
    has_puser_plugin = False
    try:
        get_action('xloader_submit')
        has_puser_plugin = True
    except KeyError:
        pass
    try:
        get_action('datapusher_submit')
        has_puser_plugin = True
    except KeyError:
        pass
    if not has_puser_plugin:
        return False
    res_dict = get_action('resource_show')(
        {'ignore_auth': True}, {'id': resource_id})
    if (res_dict.get('url_type') == 'upload' or not res_dict.get('url_type')):
        return True
    return False
