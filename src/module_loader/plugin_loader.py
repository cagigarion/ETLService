import os

from plugin.discovery import PackagePathPluginFinder
from singleton_decorator import singleton
import logging

@singleton
class PluginLoader:
    def __init__(self):
        where = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
        logging.debug(f'Location to load plugins = {where}')
        operator_finder = PackagePathPluginFinder(where=where, include=(["module_loader.operator_plugins"]))
        source_finder = PackagePathPluginFinder(where=where, include=(["module_loader.source_plugins"]))
        sink_finder = PackagePathPluginFinder(where=where, include=(["module_loader.sink_plugins"]))

        self._operator_plugins = operator_finder.find_plugins()
        self._source_plugins = source_finder.find_plugins()
        self._sink_plugins = sink_finder.find_plugins()

    def get_operator_by_name(self, name):
        try:
            operator_spec = next(spec for spec in self._operator_plugins if spec.name == name.lower())
            return operator_spec.factory()
        except Exception as e:
            logging.debug(f'Exception when get Operator plugin by name {e}')
        return None

    def get_source_by_name(self, name):
        try:
            source_spec = next(spec for spec in self._source_plugins if spec.name == name.lower())
            # debug
            print("source_spec", source_spec)
            print("source_spec.factory()", source_spec.factory())
            # end debug
            return source_spec.factory()
        except Exception as e:
            logging.debug(f'Exception when get Source plugin by name {e}')
        return None
    
    def get_sink_by_name(self, name):
        try:
            sink_spec = next(spec for spec in self._sink_plugins if spec.name == name.lower())
            return sink_spec.factory()
        except Exception as e:
            logging.debug(f'Exception when get Sink plugin by name {e}')
        return None
