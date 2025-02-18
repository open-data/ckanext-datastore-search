import ckan.plugins as plugins

from logging import getLogger


log = getLogger(__name__)


class DataStoreSearchPlusPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IActions)

    # IConfigurer

    def update_config(self, config_):
        return


    #IActions

    def get_actions(self):
        return {}
