import pysolr
import json
import requests
import re
from logging import getLogger

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
PSQL_TO_SOLR_WILCARD_MATCH = re.compile('^_?|_?$')

log = getLogger(__name__)
DEBUG = config.get('debug', False)


class DatastoreSolrBackend(DatastoreSearchBackend):
    """
    SOLR class for datastore search backend.
    """
    timeout = config.get('solr_timeout')
    default_search_fields = ['_id', '_version_', 'indexed_ts', '_text_']

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

    def _make_connection(self, resource_id: str) -> Optional[pysolr.Solr]:
        """
        Tries to make a SOLR connection to a core.
        """
        core_name = f'{self.prefix}{resource_id}'
        conn_string = f'{self.url}/solr/{core_name}'
        conn = pysolr.Solr(conn_string, timeout=self.timeout)
        try:
            resp = json.loads(conn.ping())
            if resp.get('status') == 'OK':
                return conn
        except pysolr.SolrError:
            pass

    def _make_or_create_connection(self, resource_id: str) -> Optional[pysolr.Solr]:
        """
        Tries to make a SOLR connection to a core,
        otherwise tries to creates a new core.
        """
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id)
        if conn:
            return conn
        ds_result = get_action('datastore_search')(
            self._get_site_context(), {'resource_id': resource_id,
                                       'limit': 0,
                                       'skip_search_engine': True})
        create_dict = {
            'resource_id': resource_id,
            'fields': [f for f in ds_result['fields'] if
                       f['id'] not in self.default_search_fields]}
        self.create(self._get_site_context(), create_dict)
        conn = self._make_connection(resource_id)
        if conn:
            return conn
        raise DatastoreSearchException(
            _('Could not connect to SOLR core %s') % core_name)

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

    def reindex(self,
                resource_id: str,
                connection: Optional[pysolr.Solr] = None,
                only_missing: bool = False) -> Any:
        """
        Reindexes the SOLR core.
        """
        #FIXME: put this in a background task as a larger
        #       DS Resource could take a long time??
        context = self._get_site_context()
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_or_create_connection(resource_id) if not connection else connection

        errmsg = _('Could not reload SOLR core %s') % core_name
        resp = self._send_api_request(method='POST',
                                      endpoint=f'cores/{core_name}/reload')
        if 'error' in resp:
            raise DatastoreSearchException(
                errmsg if not DEBUG
                else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
        log.debug('Reloaded SOLR Core for DataStore Resource %s' % resource_id)

        ds_result = get_action('datastore_search')(
            context, {'resource_id': resource_id, 'limit': 0,
                      'include_total': True,
                      'skip_search_engine': True})
        ds_total = ds_result['total']
        ds_field_ids = []
        for ds_field in ds_result.get('fields', []):
            if ds_field['id'] not in self.default_search_fields:
                ds_field_ids.append(ds_field['id'])
        ds_columns = '_id,' + ','.join([identifier(c) for c in ds_field_ids])

        solr_result = conn.search(q='*:*', rows=0)
        solr_total = solr_result.hits

        indexed_ids = None
        gathering_solr_records = True
        offset = 0
        indexed_ids = []
        while gathering_solr_records:
            solr_records = self.search(
                context, {'resource_id': resource_id,
                          'limit': 1000,
                          'offset': offset},
                conn)
            if not solr_records:
                gathering_solr_records = False
            indexed_ids += [r['_id'] for r in solr_records]
            offset += 1000
        gathering_ds_records = True
        offset = 0
        table_name = identifier(resource_id)
        where_statement = 'WHERE _id NOT IN ({indexed_ids})'.format(
            indexed_ids=','.join(indexed_ids)) if \
            only_missing and indexed_ids and int(solr_total) <= int(ds_total) else ''
        core_name = f'{self.prefix}{resource_id}'
        errmsg = _('Failed to reindex records for %s' % core_name)
        existing_ids = []
        while gathering_ds_records:
            sql_string = '''
                SELECT {columns} FROM {table} {where_statement}
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
                if only_missing and indexed_ids and str(r['_id']) in indexed_ids:
                    continue
                try:
                    conn.add(docs=[r], commit=False)
                    if DEBUG:
                        log.debug('Indexed DataStore record _id=%s for Resource %s' %
                                  (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    raise DatastoreSearchException(
                        errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            offset += 1000
        orphan_ids = set(indexed_ids) - set(existing_ids)
        for orphan_id in orphan_ids:
            try:
                conn.delete(q='_id:%s' % orphan_id, commit=False)
                if DEBUG:
                    log.debug('Unindexed DataStore record _id=%s for Resource %s' %
                              (orphan_id, resource_id))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
        conn.commit(waitSearcher=False)
        log.debug('Reindexed SOLR Core for DataStore Resource %s' % resource_id)

    def _check_counts(self,
                      resource_id: str,
                      connection: pysolr.Solr) -> Any:
        """
        Checks if the record counts match between the DataStore and SOLR.
        """
        ds_result = get_action('datastore_search')(
            self._get_site_context(), {'resource_id': resource_id,
                                       'limit': 0,
                                       'include_total': True,
                                       'skip_search_engine': True})
        ds_total = ds_result['total']
        solr_result = connection.search(q='*:*', rows=0)
        solr_total = solr_result.hits

        if int(ds_total) != int(solr_total):
            log.debug('SOLR (count: %s) and Postgres (count: %s) out of sync, '
                      'reindexing SOLR for DataStore Resource %s' %
                      (solr_total, ds_total, resource_id))
            self.reindex(resource_id, connection, only_missing=True)

    def create(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Create or update & reload/reindex a core if the fields have changed.
        """
        resource_id = data_dict.get('resource_id')
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id) if not connection else connection
        if not conn:
            # FIXME: using configSet in API does not copy the configSet
            #        into the core conf directory. We need to send some type
            #        of signal to the SOLR server so it can run
            #        solr create -c core_name -d configsets/datastore_resource
            #        then does SOLR server need to send a signal back?
            #        or can we just keep retrying a couple of times??
            errmsg = _('Could not create SOLR core %s') % core_name
            req_body = {'create': [{'name': core_name,
                                    'configSet': 'datastore_resource'}]}
            resp = self._send_api_request(method='POST',
                                          endpoint='cores',
                                          body=req_body)
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not DEBUG
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Created SOLR Core for DataStore Resource %s' % resource_id)
            conn = self._make_connection(resource_id)
        if not conn:
            raise DatastoreSearchException(
                _('Could not connect to SOLR core %s') % core_name)

        try:
            solr_fields = json.loads(conn._send_request(
                method='GET', path='schema/fields'))['fields']
        except pysolr.SolrError as e:
            raise DatastoreSearchException(
                errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
        keyed_solr_fields = {}
        for solr_field in solr_fields:
            if solr_field['name'] in self.default_search_fields:
                continue
            keyed_solr_fields[solr_field['name']] = solr_field
        ds_field_ids = []
        new_fields = []
        updated_fields = []
        remove_fields = []
        for ds_field in data_dict.get('fields', []):
            if ds_field['id'] not in self.default_search_fields:
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
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not DEBUG
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Added SOLR Field %s for DataStore Resource %s' %
                      (f['name'], resource_id))

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
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not DEBUG
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Modified SOLR Field %s for DataStore Resource %s' %
                      (f['name'], resource_id))

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
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            if 'error' in resp:
                raise DatastoreSearchException(
                    errmsg if not DEBUG
                    else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
            log.debug('Removed SOLR Field %s for DataStore Resource %s' %
                      (f['name'], resource_id))

        if new_fields or updated_fields or remove_fields:
            self.reindex(resource_id, connection=conn)

        if 'records' in data_dict:
            self.upsert(context, data_dict, connection=conn)

        self._check_counts(resource_id, connection=conn)

    def upsert(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Insert records into the SOLR index.
        """
        resource_id = data_dict.get('resource_id')
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_or_create_connection(resource_id) if not connection else connection

        if data_dict['records']:
            for r in data_dict['records']:
                try:
                    conn.add(docs=[r], commit=False)
                    if DEBUG:
                        log.debug('Indexed DataStore record _id=%s for Resource %s' %
                                  (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    errmsg = _('Failed to index records for %s' % core_name)
                    raise DatastoreSearchException(
                        errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)

        self._check_counts(resource_id, connection=conn)

    def search(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Optional[List[Dict[str, Any]]]:
        """
        Searches the SOLR records.
        """
        if data_dict.get('limit') == 0:
            return

        resource_id = data_dict.get('resource_id')
        conn = self._make_or_create_connection(resource_id) if not connection else connection

        query = data_dict.get('q', {})
        filters = data_dict.get('filters', {})
        q = '*:*'
        fq = []

        # TODO: implement mappings for boolean operations
        #       see:  https://search.open.canada.ca/page/help/?opendata

        for key, value in filters.items():
            fq.append('%s:%s' % (key, re.sub(PSQL_TO_SOLR_WILCARD_MATCH,
                                             '*',
                                             value.replace(':*', ''))))
        if query and isinstance(query, str):
            # FIXME: solve the query of all _text_ field
            q = '*%s*' % re.sub(PSQL_TO_SOLR_WILCARD_MATCH,
                                '*',
                                query.replace(':*', ''))
        elif query and isinstance(query, dict):
            for key, value in query.items():
                fq.append('%s:%s' % (key, re.sub(PSQL_TO_SOLR_WILCARD_MATCH,
                                                 '*',
                                                 value.replace(':*', ''))))

        solr_query = {
            'q': q,
            'q.op': 'AND',
            'fq': fq,
            'df': data_dict.get('df', '_text_'),
            'start': data_dict.get('offset', 0),
            'rows': data_dict.get('limit', 1000),
            'sort': data_dict.get('sort', '_id asc')
        }

        try:
            results = conn.search(**solr_query)
        except pysolr.SolrError as e:
            errmsg = _('Failed to query records for resource %s' % resource_id)
            raise DatastoreSearchException(
                errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])

        return results.docs

    def delete(self,
               context: Context,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Removes records from the SOLR index, or deletes the core entirely.
        """
        resource_id = data_dict.get('resource_id')
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_or_create_connection(resource_id) if not connection else connection

        if not data_dict.get('filters'):
            errmsg = _('Could not delete SOLR core %s') % core_name
            try:
                conn.delete(q='*:*', commit=False)
                log.debug('Unindexed all DataStore records for Resource %s' % resource_id)
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)
            if data_dict.get('filters') is None:
                resp = self._send_api_request(
                    method='POST', endpoint=f'cores/{core_name}/unload')
                if 'error' in resp:
                    raise DatastoreSearchException(
                        errmsg if not DEBUG
                        else resp['error'].get('msg', errmsg)[:MAX_ERR_LEN])
                log.debug('Unloaded SOLR Core for DataStore Resource %s' % resource_id)
            return

        for record in data_dict.get('deleted_records', []):
            errmsg = _('Could not delete DataStore record _id=%s in SOLR core %s') % \
                       (record['_id'], core_name)
            try:
                conn.delete(q='_id:%s' % record['_id'], commit=False)
                if DEBUG:
                    log.debug('Unindexed DataStore record _id=%s for Resource %s' %
                              (record['_id'], resource_id))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)

        self._check_counts(resource_id, connection=conn)
