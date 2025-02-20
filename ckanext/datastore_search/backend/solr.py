import pysolr
import json
import requests

from typing import Any, Optional
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
    """
    SOLR class for datastore search backend.
    """
    timeout = config.get('solr_timeout')

    @property
    def field_type_map(self):
        """
        Map of DataStore field types to their corresponding
        SOLR field types.

        NOTE: These are all based off of postgres data types.
              This is mainly to support the extending of DataStore
              types. e.g. through TableDesigner interfaces.
        """
        return {
            # numeric types
            'smallint': 'int',
            'integer': 'int',
            'bigint': 'int',
            'decimal': 'float',
            'numeric': 'float',
            'real': 'double',
            'double precision': 'double',
            'smallserial': 'int',
            'serial': 'int',
            'bigserial': 'int',
            # monetary types
            'money': 'float',
            # char types
            'character varying': 'text',
            'varchar': 'text',
            'character': 'text',
            'char': 'text',
            'bpchar': 'text',
            'text': 'text',
            # binary types
            'bytea': 'binary',
            # datetime types
            'timestamp': 'date',
            'date': 'date',
            'time': 'date',
            'interval': 'date',
            # bool types
            'boolean': 'boolean',
            # TODO: map geometric types
            # TODO: map object/array types
        }

    def _make_connection(self, core_name: str) -> Optional[pysolr.Solr]:
        """
        Tries to make a SOLR connection to a core.
        """
        conn_string = f'{self.url}/solr/{core_name}'
        conn = pysolr.Solr(conn_string, timeout=self.timeout)
        try:
            resp = json.loads(conn.ping())
            if resp.get('status') == 'OK':
                return conn
        except (KeyError, ValueError, pysolr.SolrError):
            pass

    def _send_api_request(self,
                          method: str,
                          endpoint: str,
                          body: Optional[dict[str, Any]] = None):
        """
        Sends a SOLR API v2 request.

        NOTE: pysolr does not have an API v2 interface.
        """
        conn_string = f'{self.url}/api/{endpoint}'
        if method == 'POST':
            resp = requests.post(
                conn_string,
                headers={'Content-Type': 'application/json'},
                timeout=self.timeout,
                data=json.dumps(body) if body else None)
        else:
            resp = requests.get(conn_string,
                                timeout=self.timeout)
        return resp.json()

    def create(self,
               context: Context,
               data_dict: DataDict) -> Any:
        """
        Create or update & reload a core if the fields have changed.
        """
        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        conn = self._make_connection(core_name=core_name)
        if not conn:
            errmsg = _('Could not create SOLR core %s') % core_name
            req_body = {'create': [{
                            'name': core_name,
                            'configSet': 'datastore_resource'}]}
            try:
                resp = self._send_api_request(method='POST',
                                              endpoint='cores',
                                              body=req_body)
                if 'error' in resp:
                    raise DatastoreSearchException(
                        errmsg if not config.get('debug')
                        else resp['error'].get('msg', errmsg))
                conn = self._make_connection(core_name=core_name)
            except (KeyError, ValueError):
                raise DatastoreSearchException(errmsg)
        if not conn:
            raise DatastoreSearchException(
                _('Could not connect to SOLR core %s') % core_name)

        log.info('    ')
        log.info('DEBUGGING::DatastoreSolrBackend::create')
        log.info('    ')
        log.info(pprint(data_dict))
        log.info('    ')
        solr_fields = json.loads(
            conn._send_request(method='GET', path='schema/fields'))
        log.info('    ')
        log.info(pprint(solr_fields))
        log.info('    ')
        dev = []
        for field in data_dict.get('fields', []):
            dev.append({
                'name': field.get('id'),
                'type': self.field_type_map[field.get('type')],
                'stored': True,
                'indexed': True})

        log.info('    ')
        log.info(pprint(dev))
        log.info('    ')

