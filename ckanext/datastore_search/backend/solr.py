import pysolr
import json
import requests
import re
from logging import getLogger
from urllib.parse import urlsplit
import time
import hashlib
import tempfile
import math
from datetime import datetime

from typing import Any, Optional, Dict, List
from ckan.types import DataDict

from ckan.plugins.toolkit import _, config, get_action, enqueue_job
from ckan.lib.jobs import add_queue_name_prefix

from ckanext.datastore.logic.action import datastore_search_sql
from ckanext.datastore.backend.postgres import identifier
from ckanext.datastore.blueprint import dump_to

from ckanext.datastore_search.utils import (
    get_site_context,
    get_datastore_create_dict
)
from ckanext.datastore_search.backend import (
    DatastoreSearchBackend,
    DatastoreSearchException
)


MAX_ERR_LEN = 1000
PSQL_TO_SOLR_WILCARD_MATCH = re.compile('^_?|_?$')
CHUNK_SIZE = 16 * 1024
DOWNLOAD_TIMEOUT = 30

log = getLogger(__name__)
DEBUG = config.get('debug', False)


class DatastoreSolrBackend(DatastoreSearchBackend):
    """
    SOLR class for datastore search backend.
    """
    timeout = config.get('solr_timeout')
    default_search_fields = ['_id', '_version_', 'indexed_ts', 'index_id', '_text_']
    configset_name = config.get('ckanext.datastore_search.solr.configset',
                                'datastore_resource')

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
            'int2': 'int',
            'int4': 'int',
            'int8': 'int',
            'int16': 'int',
            'decimal': 'float',
            'numeric': 'float',
            'real': 'double',
            'double precision': 'double',
            'smallserial': 'int',
            'serial': 'int',
            'bigserial': 'int',
            'serial4': 'int',
            'serial8': 'int',
            'serial16': 'int',
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

    def _make_connection(self,
                         resource_id: Optional[str] = None) -> Optional[pysolr.Solr]:
        """
        Tries to make a SOLR connection to a core.
        """
        if not resource_id:
            return
        core_name = f'{self.prefix}{resource_id}'
        conn_string = f'{self.url}/solr/{core_name}'
        try:
            conn = pysolr.Solr(conn_string, timeout=self.timeout)
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

    def reindex(self,
                resource_id: Optional[str] = None,
                connection: Optional[pysolr.Solr] = None,
                only_missing: bool = False) -> Any:
        """
        Reindexes the SOLR core.
        """
        if not resource_id:
            return
        # FIXME: put this in a background task as a larger
        #        DS Resource could take a long time??
        context = get_site_context()
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id) if not connection else connection

        if not conn:
            raise DatastoreSearchException(
                _('SOLR core does not exist for DataStore Resource %s') % resource_id)

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
        indexed_records = []
        while gathering_solr_records:
            solr_records = self.search(
                {'resource_id': resource_id,
                 'limit': 1000,
                 'offset': offset},
                conn)
            if not solr_records:
                gathering_solr_records = False
            # type_ignore_reason: checking solr_records
            indexed_ids += [str(r['_id']) for r in solr_records]  # type: ignore
            if self.only_use_engine:
                # type_ignore_reason: checking solr_records
                indexed_records += solr_records  # type: ignore
            offset += 1000
        core_name = f'{self.prefix}{resource_id}'

        # clear index
        if not only_missing:
            log.debug('Emptying SOLR index for DataStore Resource %s' %
                      resource_id)
            conn.delete(q='*:*', commit=False)
            log.debug('Unindexed all DataStore records for DataStore Resource %s' %
                      resource_id)

        errmsg = _('Failed to reindex records for %s' % core_name)
        if not self.only_use_engine:
            gathering_ds_records = True
            offset = 0
            table_name = identifier(resource_id)
            where_statement = 'WHERE _id NOT IN ({indexed_ids})'.format(
                indexed_ids=','.join(indexed_ids)) if \
                only_missing and indexed_ids and \
                int(solr_total) <= int(ds_total) else ''
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
                    if only_missing and indexed_ids and str(r['_id']) in indexed_ids:
                        continue
                    try:
                        conn.add(docs=[r], commit=False)
                        if DEBUG:
                            log.debug('Indexed DataStore record '
                                      '_id=%s for Resource %s' %
                                      (r['_id'], resource_id))
                    except pysolr.SolrError as e:
                        raise DatastoreSearchException(
                            errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
                offset += 1000
        else:
            for r in indexed_records:
                try:
                    conn.add(docs=[r], commit=False)
                    if DEBUG:
                        log.debug('Indexed DataStore record '
                                  '_id=%s for Resource %s' %
                                  (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    raise DatastoreSearchException(
                        errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
        conn.commit(waitSearcher=False)
        log.debug('Reindexed SOLR Core for DataStore Resource %s' % resource_id)

    def _check_counts(self,
                      resource_id: Optional[str] = None,
                      connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Checks if the record counts match between the DataStore and SOLR.
        """
        if not resource_id:
            return

        conn = self._make_connection(resource_id) if not connection else connection

        if not conn:
            raise DatastoreSearchException(
                _('SOLR core does not exist for DataStore Resource %s') % resource_id)

        ds_result = get_action('datastore_search')(
            get_site_context(), {'resource_id': resource_id,
                                 'limit': 0,
                                 'include_total': True,
                                 'skip_search_engine': True})
        ds_total = ds_result['total']
        solr_result = conn.search(q='*:*', rows=0)
        solr_total = solr_result.hits

        if int(ds_total) != int(solr_total):
            log.debug('SOLR (count: %s) and Postgres (count: %s) out of sync, '
                      'reindexing SOLR for DataStore Resource %s' %
                      (solr_total, ds_total, resource_id))
            self.reindex(resource_id, connection=conn, only_missing=True)

    def create(self,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Create or update & reload/reindex a core if the fields have changed.
        """
        resource_id = data_dict.get('resource_id')
        upload_file = data_dict.pop('upload_file', False)
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id) if not connection else connection
        if not conn:
            errmsg = _('Could not create SOLR core %s') % core_name
            callback_queue = add_queue_name_prefix(self.redis_callback_queue_name)
            enqueue_job(
                # type_ignore_reason: incomplete typing
                fn='solr_utils.create_solr_core.proc.create_solr_core',  # type: ignore
                kwargs={
                    'core_name': core_name,
                    'config_set': self.configset_name,
                    'callback_fn': 'ckanext.datastore_search.logic.'
                                   'action.datastore_search_create_callback',
                    'callback_queue': callback_queue,
                    'callback_timeout': config.get('ckan.jobs.timeout', 300),
                    'callback_extras': {
                        'records': data_dict.get('records', None)
                        if self.only_use_engine else None,
                        'upload_file': upload_file}
                    },
                title='SOLR Core creation %s' % core_name,
                queue=self.redis_queue_name,
                rq_kwargs={'timeout': 60})
            log.debug('Enqueued SOLR Core creation for DataStore Resource %s ' %
                      resource_id)
            return
        if not conn:
            raise DatastoreSearchException(
                _('Could not connect to SOLR core %s') % core_name)

        try:
            solr_fields = json.loads(conn._send_request(
                method='GET', path='schema/fields'))['fields']
        except pysolr.SolrError as e:
            errmsg = _('Could not get SOLR fields from core %s') % core_name
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
                    'multiValued': False,
                    'indexed': True})
                continue
            if self.field_type_map[ds_field['type']] == \
               keyed_solr_fields[ds_field['id']]['type']:
                continue
            updated_fields.append(dict(keyed_solr_fields[ds_field['id']],
                                       type=self.field_type_map[ds_field['type']]))
        for field_name in [i for i in keyed_solr_fields.keys()
                           if i not in ds_field_ids]:
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

        if upload_file:
            res_dict = get_action('resource_show')(
                {'ignore_auth': True}, {'id': resource_id})
            if res_dict.get('format', '').lower() not in self.supported_file_formats:
                raise DatastoreSearchException('Unsupported file format')
            url = res_dict.get('url')
            url_parts = urlsplit(url)
            scheme = url_parts.scheme
            if scheme not in ('http', 'https', 'ftp'):
                raise DatastoreSearchException('Only http, https, and ftp '
                                               'resources may be fetched.')
            download_uri = url
            headers = {}
            site_user = get_action('get_site_user')({'ignore_auth': True}, {})
            if res_dict.get('url_type') == 'upload':
                headers['Authorization'] = site_user['apikey']
                download_uri = url_parts._replace(
                    query='{}&nonce={}'.format(url_parts.query, time.time()),
                    netloc=(self.download_proxy_address or url_parts.netloc)).geturl()
            log.debug('Fetching from: %s' % download_uri)
            tmp_file = tempfile.TemporaryFile()
            length = 0
            m = hashlib.md5()
            errmsg = _('Could not load file to SOLR core %s') % core_name
            try:
                if self.only_use_engine:
                    response = requests.get(
                        download_uri,
                        verify=self.download_verify_https,
                        stream=True,
                        headers=headers,
                        timeout=DOWNLOAD_TIMEOUT)
                    response.raise_for_status()
                    for chunk in response.iter_content(CHUNK_SIZE):
                        length += len(chunk)
                        tmp_file.write(chunk)
                        m.update(chunk)
                else:
                    # type_ignore_reason: incomplete typing
                    for chunk in dump_to(resource_id,  # type: ignore
                                         fmt=res_dict['format'].lower(),
                                         offset=0,
                                         limit=None,
                                         options={'bom': False},
                                         sort='_id',
                                         search_params={'skip_search_engine': True},
                                         user=site_user['name']):
                        length += len(chunk)
                        tmp_file.write(chunk)
                        m.update(chunk)
                human_length = '0 bytes'
                if length != 0:
                    size_name = ('bytes', 'KB', 'MB', 'GB', 'TB')
                    i = int(math.floor(math.log(length, 1024)))
                    p = math.pow(1024, i)
                    s = round(float(length) / p, 1)
                    human_length = "%s %s" % (s, size_name[i])
                log.debug('Downloaded ok - %s', human_length)
                file_hash = m.hexdigest()
                tmp_file.seek(0)
                if not self.always_reupload_file and res_dict.get('hash') == file_hash:
                    log.debug('The file hash has not changed, skipping loading into '
                              'SOLR index for DataStore Resource %s' % resource_id)
                    return
                log.debug('Emptying SOLR index for DataStore Resource %s' %
                          resource_id)
                conn.delete(q='*:*', commit=False)
                log.debug('Unindexed all DataStore records for DataStore Resource %s' %
                          resource_id)
                log.debug('Uploading file to SOLR core %s for DataStore Resource %s' %
                          (core_name, resource_id))
                separator = ','
                if res_dict['format'].lower() == 'tsv':
                    separator = '%09'
                solr_response = json.loads(conn._send_request(
                    method='POST',
                    path='update?commit=false&trim=true&overwrite=false'
                    '&header=true&separator=%s' % separator,
                    headers={'Content-Type': 'application/csv'},
                    body=tmp_file.read()))
                if solr_response.get('errors'):
                    raise DatastoreSearchException(
                        errmsg if not DEBUG else solr_response.get('errors'))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            except Exception as e:
                raise DatastoreSearchException(e)
            finally:
                tmp_file.close()
            conn.commit(waitSearcher=False)
            return

        if new_fields or updated_fields or remove_fields:
            self.reindex(resource_id, connection=conn)

        if 'records' in data_dict and not self.only_use_engine:
            self.upsert(data_dict, connection=conn)

        if not self.only_use_engine:
            self._check_counts(resource_id, connection=conn)

    def create_callback(self, data_dict: DataDict) -> Any:
        """
        Callback from the REDIS queue via SOLR server
        after successful creation of the SOLR core.
        """
        if data_dict.get('exit_code'):
            log.debug('SOLR core creation exit_code: %s' % data_dict.get('exit_code'))
        if data_dict.get('stdout'):
            log.debug('SOLR core creation stdout: %s' % data_dict.get('stdout'))
        if data_dict.get('stderr'):
            log.debug('SOLR core creation stderr: %s' % data_dict.get('stderr'))

        resource_id = data_dict.get('core_name', '').replace(self.prefix, '')
        upload_file = data_dict.get('extras', {}).get('upload_file', False)
        create_dict = get_datastore_create_dict(resource_id, upload_file)
        # call create again to get datastore fields and data types to build schema
        self.create(create_dict)

        if (
            not upload_file and
            self.only_use_engine and
            data_dict.get('extras', {}).get('records')
        ):
            # use datastore_upsert with insert to be able to use dry_run to get _id
            get_action('datastore_upsert')(
                get_site_context(),
                {'resource_id': resource_id,
                 'records': data_dict.get('extras', {}).get('records'),
                 'method': 'insert'})

    def upsert(self,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Insert records into the SOLR index.
        """
        resource_id = data_dict.get('resource_id')
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id) if not connection else connection

        if not conn:
            raise DatastoreSearchException(
                _('SOLR core does not exist for DataStore Resource %s') % resource_id)

        if data_dict['records']:
            for r in data_dict['records']:
                # TODO: do any other value type transforms that might be needed...
                for k, v in r.items():
                    if isinstance(v, datetime):
                        r[k] = v.isoformat() + 'Z'
                solr_record = self.search({
                    'resource_id': resource_id,
                    'offset': 0,
                    'limit': 1,
                    'query': '_id:%s' % r['_id']},
                    connection=conn)
                _r = dict(r)
                if solr_record:
                    _r = dict(solr_record[0], **_r)
                for f in self.default_search_fields:
                    if f == '_id' or f == 'index_id':
                        continue
                    _r.pop(f, None)
                try:
                    conn.add(docs=[_r], commit=False)
                    if DEBUG:
                        log.debug('Indexed DataStore record _id=%s for Resource %s' %
                                  (r['_id'], resource_id))
                except pysolr.SolrError as e:
                    errmsg = _('Failed to index records for %s' % core_name)
                    raise DatastoreSearchException(
                        errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)

        if not self.only_use_engine:
            self._check_counts(resource_id, connection=conn)

    def search(self,
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) \
            -> Optional[List[Dict[str, Any]]]:
        """
        Searches the SOLR records.
        """
        if data_dict.get('limit') == 0:
            return

        resource_id = data_dict.get('resource_id')
        conn = self._make_connection(resource_id) if not connection else connection

        if not conn:
            raise DatastoreSearchException(
                _('SOLR core does not exist for DataStore Resource %s') % resource_id)

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
            'fl': data_dict.get('fl', None),
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
               data_dict: DataDict,
               connection: Optional[pysolr.Solr] = None) -> Any:
        """
        Removes records from the SOLR index, or deletes the core entirely.
        """
        resource_id = data_dict.get('resource_id')
        core_name = f'{self.prefix}{resource_id}'
        conn = self._make_connection(resource_id) if not connection else connection

        if not conn:
            raise DatastoreSearchException(
                _('SOLR core does not exist for DataStore Resource %s') % resource_id)

        if not data_dict.get('filters'):
            errmsg = _('Could not delete SOLR core %s') % core_name
            try:
                conn.delete(q='*:*', commit=False)
                log.debug('Unindexed all DataStore records for DataStore Resource %s' %
                          resource_id)
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

        if self.only_use_engine:
            # will not have deleted_records returned, so need to do this by SOLR query
            fq = []
            for key, value in data_dict.get('filters', {}).items():
                fq.append('%s:%s' % (key, value))
            errmsg = _('Could not delete DataStore record(s) '
                       'q=%s in SOLR core %s') % (' AND '.join(fq), core_name)
            collecting_deleted_records = True
            offset = 0
            deleted_records = []
            try:
                # do a search before deleting to emulate deleted_records
                while collecting_deleted_records:
                    results = conn.search(q=' AND '.join(fq),
                                          start=offset,
                                          limit=1000,
                                          sort='_id asc')
                    if not results:
                        collecting_deleted_records = False
                    deleted_records += results
                    offset += 1000
                conn.delete(q=' AND '.join(fq), commit=False)
                if DEBUG:
                    log.debug('Unindexed DataStore record(s) q=%s for Resource %s' %
                              (' AND '.join(fq), resource_id))
            except pysolr.SolrError as e:
                raise DatastoreSearchException(
                    errmsg if not DEBUG else e.args[0][:MAX_ERR_LEN])
            conn.commit(waitSearcher=False)
            data_dict['deleted_records'] = deleted_records
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

        if not self.only_use_engine:
            self._check_counts(resource_id, connection=conn)
