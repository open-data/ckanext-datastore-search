from typing import Any

import ckan.plugins.interfaces as interfaces


class IDatastoreSearchBackend(interfaces.Interface):
    """Allow custom implementations of datastore backend"""
    def register_backends(self) -> dict[str, Any]:
        """
        Register classes that inherits from DatastoreBackend.

        Every registered class provides implementations of DatastoreBackend
        and, depending on `datastore.write_url`, one of them will be used
        inside actions.

        `ckanext.datastore.DatastoreBackend` has method `set_active_backend`
        which will define most suitable backend depending on schema of
        `ckan.datastore.write_url` config directive. eg. 'postgresql://a:b@x'
        will use 'postgresql' backend, but 'mongodb://a:b@c' will try to use
        'mongodb' backend(if such backend has been registered, of course).
        If read and write urls use different engines, `set_active_backend`
        will raise assertion error.


        :returns: the dictonary with backend name as key and backend class as
                  value
        """
        return {}
