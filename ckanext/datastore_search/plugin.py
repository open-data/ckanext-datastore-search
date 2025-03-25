from logging import getLogger

import ckan.plugins as plugins
from ckan.common import CKANConfig

from typing import Dict, Union, Type, List
from ckan.types import (
    Context,
    DataDict,
    Action,
    ChainedAction
)
from click import Command

from ckanext.datastore_search.interfaces import IDatastoreSearchBackend
from ckanext.datastore_search.backend import DatastoreSearchBackend
from ckanext.datastore_search.backend.solr import DatastoreSolrBackend
from ckanext.datastore_search.logic import action
from ckanext.datastore_search.cli import datastore_search
from ckanext.datastore_search.utils import get_datastore_create_dict

from ckanext.datapusher.interfaces import IDataPusher
try:
    # type_ignore_reason: catch ImportError
    from ckanext.xloader.interfaces import IXloader  # type: ignore
    HAS_XLOADER = True
except ImportError:
    # type_ignore_reason: not redifined if ImportError
    HAS_XLOADER = False  # type: ignore

log = getLogger(__name__)


@plugins.toolkit.blanket.config_declarations
class DataStoreSearchPlugin(plugins.SingletonPlugin):
    plugins.implements(plugins.IConfigurable, inherit=True)
    plugins.implements(plugins.IConfigurer)
    plugins.implements(plugins.IClick)
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

    # IClick
    def get_commands(self) -> List[Command]:
        return [datastore_search]

    # IActions
    def get_actions(self) -> Dict[str, Union[Action, ChainedAction]]:
        return {
            'datastore_create': action.datastore_create,
            'datastore_upsert': action.datastore_upsert,
            'datastore_delete': action.datastore_delete,
            'datastore_search': action.datastore_search,
            'datastore_run_triggers': action.datastore_run_triggers,
        }

    def _enqueue_pusher_create_index(self, resource_id: str):
        backend = DatastoreSearchBackend.get_active_backend()
        create_dict = get_datastore_create_dict(resource_id, upload_file=True)
        log.debug('Creating search index for XLoader/DataPusher '
                  'resource %s in background job...' % resource_id)
        core_name = f'{backend.prefix}{resource_id}'
        plugins.toolkit.enqueue_job(
            fn=backend.create,
            kwargs={'data_dict': create_dict},
            title='SOLR Core creation %s' % core_name,
            queue=backend.redis_callback_queue_name,
            rq_kwargs={'timeout': plugins.toolkit.config.get(
                'ckan.jobs.timeout', 300)})

    # IXloader, IDataPusher
    def after_upload(self,
                     context: Context,
                     resource_dict: DataDict,
                     dataset_dict: DataDict):
        # because after_upload happens in the HTTP web callback hook,
        # we need to enqueue a background job as this might take a long time,
        # longer than a web request would take to respond.
        self._enqueue_pusher_create_index(resource_dict['id'])

    def can_upload(self,
                   resource_id: str) -> bool:
        backend = DatastoreSearchBackend.get_active_backend()
        if backend.only_use_engine:
            self._enqueue_pusher_create_index(resource_id)
            return False
        return True
