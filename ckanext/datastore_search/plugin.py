import ckan.plugins as plugins
from ckan.common import CKANConfig

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend
from ckanext.datastore_search.backend import DatastoreSearchBackend
from ckanext.datastore_search.backend.solr import DatastoreSolrBackend
from ckanext.datastore_search.logic import action


@plugins.toolkit.blanket.config_declarations
class DataStoreSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(IDatastoreSearchBackend, inherit=True)

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

    #IActions
    def get_actions(self):
        return {
            'datastore_create': action.datastore_create,
            'datastore_upsert': action.datastore_upsert,
            'datastore_delete': action.datastore_delete,
            'datastore_search': action.datastore_search,
            'datastore_run_triggers': action.datastore_run_triggers,
        }
