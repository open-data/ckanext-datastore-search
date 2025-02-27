import subprocess

def _create_solr_core(core_name, config_set):
    subprocess.run(
        ['solr', 'create', '-c', core_name, '-d', config_set],
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        timeout=60)
