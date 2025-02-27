# encoding: utf-8
__all__ = ['proc', 'worker']
import create_solr_core.proc as mod_proc
import create_solr_core.worker as mod_worker

proc = mod_proc
worker = mod_worker

# this is a namespace package
try:
    import pkg_resources
    pkg_resources.declare_namespace(__name__)
except ImportError:
    import pkgutil
    __path__ = pkgutil.extend_path(__path__, __name__)
