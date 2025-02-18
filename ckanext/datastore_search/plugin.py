import ckan.plugins as plugins

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend
from ckanext.datastore_search.backend.solr import DatastoreSolrBackend

from logging import getLogger


log = getLogger(__name__)


class DataStoreSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(IDatastoreSearchBackend, inherit=True)

    # IConfigurer
    def update_config(self, config):
        return

    #IActions
    def get_actions(self):
        return {}

    # IDatastoreSearchBackend
    def register_backends(self):
        return {'solr': DatastoreSolrBackend}
