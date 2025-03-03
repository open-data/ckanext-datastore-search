[![Tests](https://github.com/open-data/ckanext-datastore-search/workflows/Tests/badge.svg?branch=main)](https://github.com/open-data/ckanext-datastore-search/actions)

# CKANEXT DataStore Search

This plugin hooks into the creation and insertion of DataStore tables and records, and dynamically creates and alters search indices. By default, this plugin uses SOLR as the search engine.


## Requirements

https://github.com/ckan/ckan/pull/8684

**If using the SOLR engine:**
- SOLR 9+ running in Stand Alone mode. Currently, this plugin does NOT support Cloud Mode SOLR.
- [Python SOLR Utils](https://github.com/open-data/pysolr-utils) installed and service running on a SOLR server.
- A `ckan -c <INI> jobs worker ckan_ds_create_index_callback` service running on a CKAN server. *See: ckanext.datastore_search.redis.callback_queue_name below*

Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.6 and earlier | no    |
| 2.7             | no    |
| 2.8             | no    |
| 2.9             | no    |
| 2.10             | yes    |
| 2.11             | yes    |

Compatibility with Python versions:

| Python version    | Compatible?   |
| --------------- | ------------- |
| 2.7 and earlier | no    |
| 3.7 and later            | yes    |

Compatibility with SOLR versions:

| SOLR version    | Compatible?   |
| --------------- | ------------- |
| 8.x and earlier | no    |
| 9.x and later            | yes    |


## Prerequisites

This plugin requires you to have a configset in your SOLR configsets directory:

```
mkdir -p $SOLR_HOME/configsets/datastore_resource/conf
```

Copy the `managed-schema` and `solrconfig.xml` from this repository (`ckanext/datastore_search/config/solr`) into the above directory.

## Installation

To install ckanext-datastore-search:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv:
  ```
  git clone https://github.com/open-data/ckanext-datastore-search.git
  cd ckanext-datastore-search
  pip install -e .
	pip install -r requirements.txt
  ```
3. Add `development` to the `ckan.plugins` setting in your CKAN
   config file (by default the config file is located at
   `/etc/ckan/default/ckan.ini`).

4. Restart CKAN. For example if you've deployed CKAN with Apache on Ubuntu:

     sudo service apache2 reload


## Config settings

**ckanext.datastore_search.engine** controls which search engine you are using. this maps to your class via the `register_backends` method of `IDatastoreSearchBackend`:

	# (optional, default: solr).
	ckanext.datastore_search.engine = solr

**ckanext.datastore_search.url** is the URL to connect to your search engine's server:

	# (required, default: None).
	ckanext.datastore_search.url = http://solr-devm:8983

**ckanext.datastore_search.prefix** controls the prefix added to the SOLR core names, followed by the resource ID:

	# (optional, default: datastore_).
	ckanext.datastore_search.prefix = ds_res_

**ckanext.datastore_search.redis.queue_name** controls the REDIS queue name to enqueue SOLR core creations on.

	# (optional, default: ckan_ds_create_index).
	ckanext.datastore_search.redis.queue_name = ckan_ds_create_solr_core

**ckanext.datastore_search.redis.callback_queue_name** controls the REDIS queue name to enqueue optional callbacks from the create queue. *Note:* this will be used inside the CKAN framework and have `ckan:<ckan.site_id>:` prefixed to it.

	# (optional, default: ckan_ds_create_index_callback).
	ckanext.datastore_search.redis.callback_queue_name = ckan_ds_create_solr_core_callback

**ckanext.datastore_search.redis.callback_queue_name** controls the REDIS queue name to enqueue optional callbacks from the create queue. *Note:* this will be used inside the CKAN framework and have `ckan:<ckan.site_id>:` prefixed to it.

	# (optional, default: ckan_ds_create_index_callback).
	ckanext.datastore_search.redis.callback_queue_name = ckan_ds_create_solr_core_callback

**ckanext.datastore_search.solr.configset** controls the SOLR configset name.

	# (optional, default: datastore_resource).
	ckanext.datastore_search.solr.configset = ckan_ds_resource

## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
