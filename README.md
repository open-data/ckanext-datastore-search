[![Tests](https://github.com/JVickery-TBS/ckanext-datastore-search/workflows/Tests/badge.svg?branch=main)](https://github.com/JVickery-TBS/ckanext-datastore-search/actions)

# CKANEXT DataStore Search

This plugin hooks into the creation and insertion of DataStore tables and records, and dynamically creates and alters search indices. By default, this plugin uses SOLR as the search engine.


## Requirements


Compatibility with core CKAN versions:

| CKAN version    | Compatible?   |
| --------------- | ------------- |
| 2.6 and earlier | no    |
| 2.7             | no    |
| 2.8             | no    |
| 2.9             | no    |
| 2.10             | yes    |
| 2.11             | yes    |


## Installation

To install ckanext-datastore-search:

1. Activate your CKAN virtual environment, for example:

     . /usr/lib/ckan/default/bin/activate

2. Clone the source and install it on the virtualenv:
  ```
  git clone https://github.com/JVickery-TBS/ckanext-datastore-search.git
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

None at present

**TODO:** Document any optional config settings here. For example:

	# The minimum number of hours to wait before re-checking a resource
	# (optional, default: 24).
	ckanext.development.some_setting = some_default_value


## Tests

To run the tests, do:

    pytest --ckan-ini=test.ini

## License

[AGPL](https://www.gnu.org/licenses/agpl-3.0.en.html)
