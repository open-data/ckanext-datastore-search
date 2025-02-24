from ckan.types import Context, DataDict
from typing import Any

import ckan.plugins as plugins
from ckan.common import CKANConfig

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend


class DatastoreSearchException(Exception):
    pass


class DatastoreSearchBackend:
    """Base class for all datastore search backends.

    :prop _backend: mapping(engine, class) of all registered backends
    :type _backend: dictonary
    :prop _active_backend: current active backend
    :type _active_backend: DatastoreSearchBackend
    """

    _backends = {}
    _active_backend: "DatastoreSearchBackend"
    url = plugins.toolkit.config.get(
        'ckanext.datastore_search.url')
    prefix = plugins.toolkit.config.get(
        'ckanext.datastore_search.prefix', 'datastore_')

    @classmethod
    def register_backends(cls):
        """Register all backend implementations inside extensions.
        """
        for plugin in plugins.PluginImplementations(IDatastoreSearchBackend):
            cls._backends.update(plugin.register_backends())

    @classmethod
    def set_active_backend(cls, config: CKANConfig):
        """Choose most suitable backend depending on configuration

        ckanext.datastore_search.engine

        :param config: configuration object
        :rtype: ckan.common.CKANConfig

        """
        engine = config.get('ckanext.datastore_search.engine', 'solr')
        cls._active_backend = cls._backends[engine]()

    @classmethod
    def get_active_backend(cls):
        """Return currently used backend
        """
        return cls._active_backend

    @property
    def field_type_map(self):
        """
        Map of DataStore field types to their corresponding
        search index field types.
        """
        raise NotImplementedError()

    def configure(self, config: CKANConfig):
        """Configure backend, set inner variables, make some initial setup.

        :param config: configuration object
        :returns: config
        :rtype: CKANConfig

        """

        return config

    def reindex(self,
                resource_id: str,
                connection: Any,
                only_missing: bool = False) -> Any:
        """Reindex/sync records between the database and the search engine.
        """
        raise NotImplementedError()

    def create(
            self,
            context: Context,
            data_dict: DataDict) -> Any:
        """Create new resource inside the search index.

        Called by `datastore_create`.

        :param data_dict: See `ckanext.datastore_search.logic.action.datastore_create`
        :returns: The newly created data object
        :rtype: dictonary
        """
        raise NotImplementedError()

    def upsert(self, context: Context, data_dict: DataDict) -> Any:
        """Update or create resource depending on data_dict param.

        Called by `datastore_upsert`.

        :param data_dict: See `ckanext.datastore_search.logic.action.datastore_upsert`
        :returns: The modified data object
        :rtype: dictonary
        """
        raise NotImplementedError()

    def delete(self, context: Context, data_dict: DataDict) -> Any:
        """Remove resource from the search index.

        Called by `datastore_delete`.

        :param data_dict: See `ckanext.datastore_search.logic.action.datastore_delete`
        :returns: Original filters sent.
        :rtype: dictonary
        """
        raise NotImplementedError()

    def search(self, context: Context, data_dict: DataDict) -> Any:
        """Base search.

        Called by `datastore_search`.

        :param data_dict: See `ckanext.datastore_search.logic.action.datastore_search`
        :rtype: dictonary with following keys

        :param fields: fields/columns and their extra metadata
        :type fields: list of dictionaries
        :param offset: query offset value
        :type offset: int
        :param limit: query limit value
        :type limit: int
        :param filters: query filters
        :type filters: list of dictionaries
        :param total: number of total matching records
        :type total: int
        :param records: list of matching results
        :type records: list of dictionaries

        """
        raise NotImplementedError()

    def resource_exists(self, id: str) -> bool:
        """Define whether resource exists in the search index.
        """
        raise NotImplementedError()

    def resource_info(self, id: str) -> Any:
        """Return DataDictonary with resource's info - #3414
        """
        raise NotImplementedError()

    def resource_id_from_alias(self, alias: str) -> Any:
        """Convert resource's alias to real id.

        :param alias: resource's alias or id
        :type alias: string
        :returns: real id of resource
        :rtype: string

        """
        raise NotImplementedError()

    def get_all_ids(self) -> list[str]:
        """Return id of all resource registered in the search index.

        :returns: all resources ids
        :rtype: list of strings
        """
        raise NotImplementedError()
