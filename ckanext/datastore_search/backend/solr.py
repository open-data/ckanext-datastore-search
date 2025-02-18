import pysolr
import json

from typing import Any
from ckan.types import Context, DataDict

from ckan.plugins.toolkit import _, config

from ckanext.datastore_search.backend import (
    DatastoreSearchBackend,
    DatastoreSearchException
)

from pprint import pprint
from logging import getLogger
log = getLogger(__name__)


class DatastoreSolrBackend(DatastoreSearchBackend):
    '''
    SOLR class for datastore search backend.
    '''
    timeout = config.get('solr_timeout')

    def create(self,
               context: Context,
               data_dict: DataDict) -> Any:
        '''
        Create or update & reload a core if the fields have changed.
        '''
        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        errmsg = _('Could not create SOLR core %s') % core_name
        if not self.resource_exists(rid):
            solr = pysolr.Solr(f'{self.url}/solr/admin/cores',
                               timeout=self.timeout)
            # TODO: check if cloud mode is active and see about generating
            #       schemas from field and types, then uploading as configset.
            try:
                resp = json.loads(solr.create(core_name))
                if 'error' in resp:
                    raise DatastoreSearchException(
                        errmsg if not config.get('debug') else resp['error'].get('msg', errmsg))
            except (KeyError, ValueError):
                raise DatastoreSearchException(errmsg)

    def resource_exists(self, id: str) -> bool:
        '''
        If there is already a SOLR core for the resource.
        '''
        solr = pysolr.Solr(f'{self.url}/solr/{id}',
                           timeout=self.timeout)
        try:
            resp = json.loads(solr.ping())
            return resp.get('status') == 'OK'
        except (KeyError, ValueError, pysolr.SolrError):
            pass
        return False
