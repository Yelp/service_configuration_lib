import logging
from collections import defaultdict

from yaml import YAMLError

from service_configuration_lib import yaml_tools as yaml
from service_configuration_lib.cached_view import BaseCachedView

log = logging.getLogger(__name__)


class YamlConfigsCachedView(BaseCachedView):
    """Implementation of yaml configs view.
    The class keeps dictionary with updated and parsed content of yaml configs.
    To access files you can use `.configs['service_name']['config_name']`

    Example of usage:

        from service_configuration_lib.cached_view import ConfigsFileWatcher
        from service_configuration_lib.yaml_cached_view import YamlConfigsCachedView

        with ConfigsFileWatcher(
                configs_view=YamlConfigsCachedView(),
                configs_names = ['smartstack', 'authorization'],
                configs_suffixes=['.yaml']
            ) as watcher:
                watcher.process_events()
                print(watcher.configs_view.configs['schematizer']['smartstack']
    """

    def __init__(self):
        super().__init__()
        self.configs = defaultdict(dict)

    def add(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        try:
            with open(path, encoding='utf-8') as fd:
                self.configs[service_name][config_name] = yaml.safe_load(fd)
        except OSError as exn:
            log.warning(f'Error reading {path}: {exn}')
        except YAMLError as exn:
            log.warning(f'Error parsing {path}: {exn}')

    def remove(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        try:
            del self.configs[service_name][config_name]
        except KeyError:
            log.warning(f'Config {config_name} not found for {service_name}')
