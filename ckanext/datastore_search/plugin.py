import ckan.plugins as plugins
from ckan.common import CKANConfig

from typing import Dict, Union, Type
from ckan.types import (
    Context,
    DataDict,
    Action,
    ChainedAction
)

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend
from ckanext.datastore_search.backend import DatastoreSearchBackend
from ckanext.datastore_search.backend.solr import DatastoreSolrBackend
from ckanext.datastore_search.logic import action

from ckanext.datapusher.interfaces import IDataPusher
try:
    # type_ignore_reason: catch ImportError
    from ckanext.xloader.interfaces import IXloader  # type: ignore
    # type_ignore_reason: not redifined if ImportError
    HAS_XLOADER = True  # type: ignore
except ImportError:
    # type_ignore_reason: not redifined if ImportError
    HAS_XLOADER = False  # type: ignore


@plugins.toolkit.blanket.config_declarations
class DataStoreSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(IDatastoreSearchBackend, inherit=True)
    plugins.implements(IDataPusher, inherit=True)
    if HAS_XLOADER:
        # type_ignore_reason: never unbound due to HAS_XLOADER
        plugins.implements(IXloader, inherit=True)  # type: ignore

    # IDatastoreSearchBackend
    def register_backends(self) -> Dict[str, Type[DatastoreSearchBackend]]:
        return {'solr': DatastoreSolrBackend}

    # IConfigurer
    def update_config(self, config: CKANConfig):
        DatastoreSearchBackend.register_backends()
        DatastoreSearchBackend.set_active_backend(config)

        self.backend = DatastoreSearchBackend.get_active_backend()

        config['ckan.datastore.sqlsearch.enabled'] = False

    # IConfigurable
    def configure(self, config: CKANConfig):
        self.config = config
        self.backend.configure(config)

        config['ckan.datastore.sqlsearch.enabled'] = False

    # IActions
    def get_actions(self) -> Dict[str, Union[Action, ChainedAction]]:
        return {
            'datastore_create': action.datastore_create,
            'datastore_upsert': action.datastore_upsert,
            'datastore_delete': action.datastore_delete,
            'datastore_search': action.datastore_search,
            'datastore_run_triggers': action.datastore_run_triggers,
        }

    # IXloader, IDataPusher
    def after_upload(self,
                     context: Context,
                     resource_dict: DataDict,
                     dataset_dict: DataDict):
        backend = DatastoreSearchBackend.get_active_backend()
        ds_result = plugins.toolkit.get_action('datastore_search')(
            context, {'resource_id': resource_dict.get('id'),
                      'limit': 0,
                      'skip_search_engine': True})
        create_dict = {
            'resource_id': resource_dict.get('id'),
            'fields': [f for f in ds_result['fields'] if
                       f['id'] not in backend.default_search_fields]}
        backend.create(create_dict)
