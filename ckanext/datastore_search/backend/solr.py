import pysolr
import json
import requests

from typing import Any, Optional, Dict, cast, List
from ckan.types import Context, DataDict

from ckan.plugins.toolkit import _, config, get_action

from ckanext.datastore.logic.action import datastore_search_sql
from ckanext.datastore.backend.postgres import identifier

from ckanext.datastore_search.backend import (
    DatastoreSearchBackend,
    DatastoreSearchException
)

MAX_ERR_LEN = 1000

from pprint import pprint
from logging import getLogger
log = getLogger(__name__)


class DatastoreSolrBackend(DatastoreSearchBackend):
    """
    SOLR class for datastore search backend.
    """
    timeout = config.get('solr_timeout')
    default_solr_fields = ['_id', '_version_', 'indexed_ts']

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
        except pysolr.SolrError:
            pass

    def _send_api_request(self,
                          method: str,
                          endpoint: str,
                          body: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
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

    def _get_site_context(self) -> Context:
        """
        Return a CKAN Context for the system user.
        """
        site_user = get_action('get_site_user')({'ignore_auth': True}, {})
        return cast(Context, {'user': site_user['name']})

    def _reindex(self,
                 resource_id: str,
                 connection: pysolr.Solr,
                 ds_field_ids: List[str],
                 only_missing: bool = False) -> Any:
        """
        Reindexes the SOLR core.
        """
        context = self._get_site_context()
        indexed_ids = None
        if only_missing:
            gathering_solr_records = True
            offset = 0
            indexed_ids = []
            while gathering_solr_records:
                solr_records = self.search(
                    context, {'resource_id': resource_id,
                              'limit': 1000,
                              'offset': offset},
                    connection)
                if not solr_records:
                    gathering_solr_records = False
                indexed_ids += [r['_id'] for r in solr_records]
                offset += 1000
        gathering_ds_records = True
        offset = 0
        ds_columns = ','.join([identifier(c) for c in ds_field_ids])
        table_name = identifier(resource_id)
        where_statement = 'WHERE _id NOT IN ({indexed_ids})'.format(
            indexed_ids=','.join(indexed_ids) if indexed_ids else '')
        core_name = f'{self.prefix}{resource_id}'
        errmsg = _('Failed to reindex records for %s' % core_name)
        existing_ids = []
        while gathering_ds_records:
            sql_string = '''
                SELECT _id,{columns} FROM {table} {where_statement}
                LIMIT 1000 OFFSET {offset}
            '''.format(columns=ds_columns,
                       table=table_name,
                       where_statement=where_statement,
                       offset=offset)
            ds_result = datastore_search_sql(
                context, {'sql': sql_string})
            if not ds_result['records']:
                gathering_ds_records = False
            for r in ds_result['records']:
                existing_ids.append(str(r['_id']))
                try:
                    connection.add(docs=[r], commit=False)
                    log.debug('Indexed DataStore record %s for Resource %s' %
                              (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    raise DatastoreSearchException(
                        errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            offset += 1000
        #FIXME: reindexing nightmare logic
        if indexed_ids:
            orphan_ids = set(indexed_ids) - set(existing_ids)
            for orphan_id in orphan_ids:
                try:
                    connection.delete(q='_id:%s' % orphan_id, commit=False)
                    log.debug('Unindexed DataStore record %s for Resource %s' %
                              (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    raise DatastoreSearchException(
                        errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
        connection.commit(waitSearcher=False)

    def _check_counts(self,
                      resource_id: str,
                      connection: pysolr.Solr) -> Any:
        """
        Checks if the record counts match between the DataStore and SOLR.
        """
        ds_result = get_action('datastore_search')(
            self._get_site_context(), {'resource_id': resource_id, 'limit': 0,
                                       'include_total': True, 'skip_solr': True})
        ds_total = ds_result['total']
        solr_result = connection.search(q='*:*', rows=0)
        solr_total = solr_result.hits

        ds_field_ids = []
        for ds_field in ds_result.get('fields', []):
            if ds_field['id'] not in self.default_solr_fields:
                ds_field_ids.append(ds_field['id'])
        #FIXME: reindexing nightmare logic...are counts updated quick enough??
        if int(ds_total) != int(solr_total):
            log.debug('SOLR and Postgres out of sync, '
                      'reindexing SOLR for DataStore Resource %s' %
                      resource_id)
            self._reindex(resource_id, connection, ds_field_ids, only_missing=True)

    def create(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Create or update & reload/reindex a core if the fields have changed.
        """
        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        conn = self._make_connection(core_name=core_name) if not connection else connection
        if not conn:
            errmsg = _('Could not create SOLR core %s') % core_name
            req_body = {'create': [{'name': core_name,
                                    'configSet': 'datastore_resource'}]}
            resp = self._send_api_request(method='POST',
                                            endpoint='cores',
                                            body=req_body)
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug')
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Created SOLR Core for DataStore Resource %s' % rid)
            conn = self._make_connection(core_name=core_name)
        if not conn:
            raise DatastoreSearchException(
                _('Could not connect to SOLR core %s') % core_name)

        try:
            solr_fields = json.loads(conn._send_request(
                method='GET', path='schema/fields'))['fields']
        except pysolr.SolrError as e:
            raise DatastoreSearchException(
                errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
        keyed_solr_fields = {}
        for solr_field in solr_fields:
            if solr_field['name'] in self.default_solr_fields:
                continue
            keyed_solr_fields[solr_field['name']] = solr_field
        ds_field_ids = []
        new_fields = []
        updated_fields = []
        remove_fields = []
        for ds_field in data_dict.get('fields', []):
            if ds_field['id'] not in self.default_solr_fields:
                ds_field_ids.append(ds_field['id'])
            if ds_field['id'] not in keyed_solr_fields:
                new_fields.append({
                    'name': ds_field['id'],
                    'type': self.field_type_map[ds_field['type']],
                    'stored': True,
                    'indexed': True})
                continue
            if self.field_type_map[ds_field['type']] == keyed_solr_fields[ds_field['id']]['type']:
                continue
            updated_fields.append(dict(keyed_solr_fields[ds_field['id']],
                                       type=self.field_type_map[ds_field['type']]))
        for field_name in [i for i in keyed_solr_fields.keys() if i not in ds_field_ids]:
            remove_fields.append({'name': field_name})

        for f in new_fields:
            errmsg = _('Could not add field %s to SOLR Schema %s' %
                       (f['name'], core_name))
            try:
                resp = json.loads(conn._send_request(
                    method='POST', path='schema',
                    body=json.dumps({'add-field': f}),
                    headers={'Content-Type': 'application/json'}))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug')
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Added SOLR Field %s for DataStore Resource %s' %
                      (f['name'], rid))

        for f in updated_fields:
            errmsg = _('Could not update field %s on SOLR Schema %s' %
                       (f['name'], core_name))
            try:
                resp = json.loads(conn._send_request(
                    method='POST', path='schema',
                    body=json.dumps({'replace-field': f}),
                    headers={'Content-Type': 'application/json'}))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug')
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Modified SOLR Field %s for DataStore Resource %s' %
                      (f['name'], rid))

        for f in remove_fields:
            errmsg = _('Could not delete field %s from SOLR Schema %s' %
                       (f['name'], core_name))
            try:
                resp = json.loads(conn._send_request(
                    method='POST', path='schema',
                    body=json.dumps({'delete-field': f}),
                    headers={'Content-Type': 'application/json'}))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug')
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Removed SOLR Field %s for DataStore Resource %s' %
                      (f['name'], rid))
        #FIXME: reindexing nightmare logic
        if 'records' in data_dict:
            self.upsert(context, data_dict, connection=conn)

        if new_fields or updated_fields or remove_fields:
            self._reindex(resource_id=rid, connection=conn, ds_field_ids=ds_field_ids)

        self._check_counts(resource_id=rid, connection=conn)

    def upsert(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Insert records into the SOLR index.
        """
        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        conn = self._make_connection(core_name=core_name) if not connection else connection

        if not conn:
            ds_result = get_action('datastore_search')(
                self._get_site_context(), {'resource_id':
                                           rid, 'limit': 0,
                                           'skip_solr': True})
            create_dict = {
                'resource_id': rid,
                'fields': [f for f in ds_result['fields'] if
                           f['id'] not in self.default_solr_fields]}
            self.create(context, create_dict)
            conn = self._make_connection(core_name=core_name)
        if not conn:
            errmsg = _('Failed to index records for %s' % core_name)
            raise DatastoreSearchException(errmsg)

        if data_dict['records']:
            for r in data_dict['records']:
                try:
                    conn.add(docs=[r], commit=False)
                    log.debug('Indexed DataStore record %s for Resource %s' %
                              (r['_id'], rid))
                except pysolr.SolrError as e:
                    errmsg = _('Failed to index records for %s' % core_name)
                    raise DatastoreSearchException(
                        errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)

        self._check_counts(resource_id=rid, connection=conn)

    def search(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Searches the SOLR records.
        """
        if data_dict.get('limit') == 0:
            return

        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        conn = self._make_connection(core_name=core_name) if not connection else connection

        #TODO: map out datastore_search keys to SOLR keys
        solr_query = {
            'q': '*:*',
            'q.op': 'AND',
            'start': data_dict.get('offset', 0),
            'rows': data_dict.get('limit', 1000)
        }
        results = conn.search(**solr_query)

        return results.docs

    def delete(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Removes records from the SOLR index, or deletes the core entirely.
        """
        rid = data_dict.get('resource_id')
        core_name = f'{self.prefix}{rid}'
        conn = self._make_connection(core_name=core_name) if not connection else connection

        if not conn:
            return

        if not data_dict.get('filters'):
            errmsg = _('Could not delete SOLR core %s') % core_name
            try:
                conn.delete(q='*:*', commit=False)
                log.debug('Unindexed all DataStore records for Resource %s' % rid)
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)
            resp = self._send_api_request(method='POST',
                                          endpoint=f'cores/{core_name}/unload')
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug')
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Unloaded SOLR Core for DataStore Resource %s' % rid)
            return

        for record in data_dict.get('deleted_records', []):
            errmsg = _('Could not delete DataStore record %s in SOLR core %s') % \
                       (record['_id'], core_name)
            try:
                conn.delete(q='_id:%s' % record['_id'], commit=False)
                log.debug('Unindexed DataStore record %s for Resource %s' %
                          (record['_id'], rid))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not config.get('debug') else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)

        self._check_counts(resource_id=rid, connection=conn)
