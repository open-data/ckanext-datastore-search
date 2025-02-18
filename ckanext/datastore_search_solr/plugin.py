import ckan.plugins as plugins

from ckanext.datastore.interfaces import IDatastoreBackend
from ckanext.datastore_search_solr.backend.solr import DatastoreSolrBackend

from logging import getLogger


log = getLogger(__name__)


class DataStoreSearchSolrPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)
    plugins.implements(IDatastoreBackend, inherit=True)

    # IConfigurer
    def update_config(self, config):
        return

    #IActions
    def get_actions(self):
        return {}

    # IDatastoreBackend
    def register_backends(self):
        return {'solr': DatastoreSolrBackend}
