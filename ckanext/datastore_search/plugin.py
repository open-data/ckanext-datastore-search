import ckan.plugins as plugins
from ckan.common import CKANConfig

from ckan.types import Context, DataDict

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend
from ckanext.datastore_search.backend import DatastoreSearchBackend
from ckanext.datastore_search.backend.solr import DatastoreSolrBackend
from ckanext.datastore_search.logic import action

from ckanext.datapusher.interfaces import IDataPusher
try:
    from ckanext.xloader.interfaces import IXloader
    HAS_XLOADER = True
except ImportError:
    HAS_XLOADER = False


@plugins.toolkit.blanket.config_declarations
class DataStoreSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(IDatastoreSearchBackend, inherit=True)
    if HAS_XLOADER:
        try:
            plugins.toolkit.get_action('xloader_submit')
            plugins.implements(IXloader, inherit=True)
        except KeyError:
            pass
    else:
        try:
            plugins.toolkit.get_action('datapusher_submit')
            plugins.implements(IDataPusher, inherit=True)
        except KeyError:
            pass

    # IDatastoreSearchBackend
    def register_backends(self):
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
    def get_actions(self):
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
        backend.create(context, create_dict)
