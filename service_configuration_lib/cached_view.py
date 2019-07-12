import fnmatch
import logging
import os
from pathlib import Path
from typing import List
from typing import Optional
from typing import Tuple

import pyinotify

DEFAULT_SOA_DIR = '/nail/etc/services'

log = logging.getLogger(__name__)


class BaseCachedView:
    """This is the interface for ConfigsFileWatcher.
    Implementation of that interfaces should implemente 2 hooks on creation/removal of files.
    """

    def add(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        raise NotImplementedError()

    def remove(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        raise NotImplementedError()


class ConfigsFileWatcher:
    """This is a cache of the smartstack.yaml config file contents found in
    /nail/etc/services, with keys of the form '<servicename>.<namespace>'.

    What's special about this cache is that we watch for inotify events on these
    files, processing these events whenever the `process_events` method is
    invoked.

    The benefit of the explicit event processing is that it obviates the need
    for a background thread (which would bring its own set of problems).  The
    downside is that the `process_events` method must be periodically invoked
    in order to avoid overflowing the event queue.

    How frequently should we invoked the `process_events` method?  I've measured
    10 to 20 events per minute, and a queue size (max_queued_events) of 16384.
    This gives us about half a day of non-processing before the queue overflows
    and the cache becomes inconsistent.  In the unlikely case that the queue
    does overflow, then the cache will resetupt itself.

    Args:
    :param configs_view: implementation of BaseCachedView interface
    :param configs_folder: Optional, path to configs root folder, `/nail/etc/services`
    :param services_names: Optional, list of service names to watch
    :param configs_names: Optional, list of config names to watch
    :param configs_suffixes: Optional, list of file extentions to watch
    :param exclude_folders_filters: Optional, filter of masks to exclude folders from watching
    """

    def __init__(
        self,
        configs_view: BaseCachedView,
        configs_folder: str = DEFAULT_SOA_DIR,
        services_names: List[str] = ['*'],
        configs_names: List[str] = ['*'],
        configs_suffixes: List[str] = ['*'],
        exclude_folders_filters: List[str] = ['*.~tmp~'],
    ) -> None:
        super().__init__()
        self.configs_view = configs_view
        self._configs_folder = configs_folder
        self._services_names = services_names
        self._configs_names = configs_names
        self._configs_suffixes = configs_suffixes
        self._exclude_folders_filters = exclude_folders_filters
        self.setup()

    @staticmethod
    def log_inotify_constants() -> None:
        log.info(f'max_queued_events: {pyinotify.max_queued_events.value}')
        log.info(f'max_user_instances: {pyinotify.max_user_instances.value}')
        log.info(f'max_user_watches: {pyinotify.max_user_watches.value}')

    def process_events(self) -> None:
        """This method should be periodically invoked to process queued inotify
        events.  See the class docstring for more context.
        """
        while self._notifier.check_events(timeout=0):
            self._notifier.read_events()
            self._notifier.process_events()

    def setup(self) -> None:
        """Recreates state of ConfigsFileWatcher. Used as reaction on queue overflow in _EventHandler"""
        handler = _EventHandler(cache=self)
        watch_manager = pyinotify.WatchManager()
        self._notifier = pyinotify.Notifier(
            watch_manager=watch_manager,
            default_proc_fun=handler,
        )

        watch_manager.add_watch(
            path=self._configs_folder,
            mask=pyinotify.IN_MOVED_TO | pyinotify.IN_CREATE | pyinotify.IN_DELETE,
            rec=True,
            auto_add=True,
            exclude_filter=self._exclude_filter,
        )
        for root, _, files in os.walk(self._configs_folder):
            for path in files:
                self._maybe_add_path_to_cache(os.path.join(root, path))

    def _exclude_filter(self, path: str) -> bool:
        """Directories (not files) to exclude from watching.

        Attempting to watch tmp directories causes pyinotify to log an error,
        so let's just filter them out.
        Also it will exclude all folders which don't match with services_names param.
        """
        folder_name = Path(path).name

        for exclude_folder in self._exclude_folders_filters:
            if fnmatch.fnmatch(folder_name, exclude_folder):
                return True

        for service_name in self._services_names:
            if fnmatch.fnmatch(folder_name, service_name):
                return False

        return True

    def _maybe_add_path_to_cache(self, path: str) -> None:
        result = self._service_name_and_config_from_path(path)
        if result is None:
            return
        service_name, config_name, config_suffix = result
        log.info(f'{config_name}: Config changed for {service_name}')
        self.configs_view.add(path, service_name, config_name, config_suffix)

    def _maybe_remove_path_from_cache(self, path: str) -> None:
        result = self._service_name_and_config_from_path(path)
        if result is None:
            return
        service_name, config_name, config_suffix = result
        log.info(f'{config_name}: Removing config for {service_name}')
        self.configs_view.remove(path, service_name, config_name, config_suffix)

    def _service_name_and_config_from_path(self, path: str) -> Optional[Tuple[str, str, str]]:
        """Convert a config file path to a service name and config name.  For example,
        `/nail/etc/services/foo/smartstack.yaml` would be converted to `foo` with a
        config name of `smartstack`

        Returns `None` if path is invalid or we don't care about the file, e.g. not in self._config_names

        :param path: the config file path e.g. `/nail/etc/services/foo/smartstack.yaml`
        :return: a tuple of the service name, config and suffix e.g. (`foo`, `smartstack`, `yaml`),
        or `None` if the path is invalid
        """
        relpath = os.path.relpath(path, self._configs_folder)
        filename = Path(relpath)

        # when len == 2, thatss regular case e.g. service/config.yaml
        # when len == 3, that's case with secrets folder, e.g. service/secrets/config.json
        if len(filename.parts) == 2 or len(filename.parts) == 3:
            service = filename.parts[0]
            config_name = filename.stem
            config_suffix = filename.suffix
        else:
            return None

        if service == '.':
            return None

        if not any((fnmatch.fnmatch(config_name, config) for config in self._configs_names)):
            return None

        if not any((fnmatch.fnmatch(config_suffix, suffix) for suffix in self._configs_suffixes)):
            return None

        return str(service), config_name, config_suffix


class _EventHandler(pyinotify.ProcessEvent):
    def my_init(self, cache: ConfigsFileWatcher) -> None:
        self.cache = cache

    # When acceptance testing, sometimes a move is seen as a create
    def process_IN_CREATE(self, event: pyinotify.Event) -> None:
        self.cache._maybe_add_path_to_cache(event.pathname)

    def process_IN_MOVED_TO(self, event: pyinotify.Event) -> None:
        self.cache._maybe_add_path_to_cache(event.pathname)

    def process_IN_DELETE(self, event: pyinotify.Event) -> None:
        self.cache._maybe_remove_path_from_cache(event.pathname)

    def process_IN_Q_OVERFLOW(self, event: pyinotify.Event) -> None:
        self.cache.setup()
