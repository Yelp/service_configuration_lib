import fnmatch
import logging
import os
from abc import ABC
from abc import abstractmethod
from collections import namedtuple
from pathlib import Path
from typing import Optional
from typing import Sequence

import pyinotify

DEFAULT_SOA_DIR = '/nail/etc/services'
DEFAULT_PROCESS_EVENTS_LIMIT = 100

log = logging.getLogger(__name__)


class BaseCachedView(ABC):
    """This is the interface for ConfigsFileWatcher.
    Implementation of that interfaces should implemente 2 hooks on creation/removal of files.
    """

    @abstractmethod
    def add(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        raise NotImplementedError()

    @abstractmethod
    def remove(self, path: str, service_name: str, config_name: str, config_suffix: str) -> None:
        raise NotImplementedError()


_ServiceConfig = namedtuple('_ServiceConfig', ['service_name', 'config_name', 'config_suffix'])


class ConfigsFileWatcher:
    """This is a cache of the smartstack.yaml config file contents found in
    /nail/etc/services, with keys of the form '<servicename>.<namespace>'.

    What's special about this cache is that we watch for inotify events on these
    files, processing these events whenever the `process_events` method is
    invoked. By default `process_events` will process roughly 100 events per
    invocation.
    ConfigsFileWatcher instance must be closed with explicit `close()` call or
    with usage of `with...` statement.

    The benefit of the explicit event processing is that it obviates the need
    for a background thread (which would bring its own set of problems).  The
    downside is that the `process_events` method must be periodically invoked
    in order to avoid overflowing the event queue.

    How frequently should we invoked the `process_events` method?  I've measured
    10 to 20 events per minute, and a queue size (max_queued_events) of 16384.
    This gives us about half a day of non-processing before the queue overflows
    and the cache becomes inconsistent.  In the unlikely case that the queue
    does overflow, then the cache will re-setup itself.

    Args:
    :param configs_view: implementation of BaseCachedView interface
    :param configs_folder: Optional, path to configs root folder, `/nail/etc/services`
    :param services_names: Optional, list of service names to watch, can be wildcard or exact name
    :param configs_names: Optional, list of config names to watch, can be wildcard or exact name,
                          default None will watch for everything
    :param configs_suffixes: Optional, list of file extentions to watch, default None will watch for everything
    :param exclude_folders_filters: Optional, filter of masks to exclude folders from watching
    """

    def __init__(
        self,
        configs_view: BaseCachedView,
        configs_folder: str = DEFAULT_SOA_DIR,
        services_names: Sequence[str] = ['*'],
        configs_names: Optional[Sequence[str]] = None,
        configs_suffixes: Optional[Sequence[str]] = None,
        exclude_folders_filters: Sequence[str] = ['*.~tmp~'],
    ) -> None:
        super().__init__()
        self.configs_view = configs_view
        self._needs_reconfigure = False
        self._configs_folder = configs_folder
        self._services_names = services_names
        self._configs_names = None if configs_names is None else set(configs_names)
        self._configs_suffixes = None if configs_suffixes is None else set(configs_suffixes)
        self._exclude_folders_filters = exclude_folders_filters
        self._notifier = None
        self.setup()

    @staticmethod
    def log_inotify_constants() -> None:
        log.info(f'max_queued_events: {pyinotify.max_queued_events.value}')
        log.info(f'max_user_instances: {pyinotify.max_user_instances.value}')
        log.info(f'max_user_watches: {pyinotify.max_user_watches.value}')

    def process_events(self, limit: int = DEFAULT_PROCESS_EVENTS_LIMIT) -> None:
        """This method should be periodically invoked to process queued inotify
        events.  See the class docstring for more context.
        Args:
        :param limit: Optional, rough limit of events to process per call
        """
        assert self._notifier
        before_processed_count = self._processed_events_count
        currently_processed = 0
        while currently_processed < limit and self._notifier.check_events(timeout=0):
            self._notifier.read_events()
            self._notifier.process_events()

            if self._needs_reconfigure:
                self.setup()

            diff = self._processed_events_count - before_processed_count
            if diff <= 0:  # in case of overflow
                return
            currently_processed += diff
            before_processed_count = self._processed_events_count

    def close(self) -> None:
        if self._notifier is None:
            return
        self._notifier.stop()
        self._notifier = None
        self._needs_reconfigure = False

    def setup(self) -> None:
        """Recreates state of ConfigsFileWatcher. Used as reaction on queue overflow in _EventHandler"""
        # close previous in case of re-creation
        self.close()
        self._processed_events_count = 0
        handler = _EventHandler(cache=self)
        watch_manager = pyinotify.WatchManager()
        self._notifier = pyinotify.Notifier(
            watch_manager=watch_manager,
            default_proc_fun=handler,
        )

        watch_manager.add_watch(
            path=self._configs_folder,
            mask=pyinotify.IN_MOVED_TO | pyinotify.IN_CREATE | pyinotify.IN_DELETE | pyinotify.IN_DELETE_SELF,
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

        'autotuned_defaults' folders will be included so that their values can be used
        for configuring service timeouts.
        """
        folder_name = Path(path).name

        for exclude_folder in self._exclude_folders_filters:
            if fnmatch.fnmatch(folder_name, exclude_folder):
                return True

        for service_name in self._services_names:
            if folder_name == 'autotuned_defaults':
                service = Path(path).parent.name
                if fnmatch.fnmatch(service, service_name):
                    return False

            if fnmatch.fnmatch(folder_name, service_name):
                return False

        return True

    def _maybe_add_path_to_cache(self, path: str) -> None:
        result = self._service_name_and_config_from_path(path)
        if result is None:
            return
        log.info(f'{result.config_name}: Config changed for {result.service_name}')
        self.configs_view.add(path, result.service_name, result.config_name, result.config_suffix)

    def _maybe_remove_path_from_cache(self, path: str) -> None:
        result = self._service_name_and_config_from_path(path)
        if result is None:
            return
        log.info(f'{result.config_name}: Removing config for {result.service_name}')
        self.configs_view.remove(path, result.service_name, result.config_name, result.config_suffix)

    def _service_name_and_config_from_path(self, path: str) -> Optional[_ServiceConfig]:
        """Convert a config file path to a service name, config name and config suffix.  For example,
        `/nail/etc/services/foo/smartstack.yaml` would be converted to `foo` with a
        config name of `smartstack` and config suffix `.yaml`

        Returns `None` if path is invalid or we don't care about the file, e.g. not in self._configs_names

        :param path: the config file path e.g. `/nail/etc/services/foo/smartstack.yaml`
        :return: _ServiceConfig or `None` if the path is invalid
        """
        relpath = os.path.relpath(path, self._configs_folder)
        filename = Path(relpath)

        service = str(filename.parts[0]) if len(filename.parts) > 1 else None
        config_name = filename.stem
        config_suffix = filename.suffix

        if self._configs_names is not None and \
           not any(fnmatch.fnmatch(config_name, pattern) for pattern in self._configs_names):
            return None

        if self._configs_suffixes is not None and config_suffix not in self._configs_suffixes:
            return None

        return _ServiceConfig(service, config_name, config_suffix)

    def _process_inotify_event(self):
        self._processed_events_count += 1

    def __enter__(self):
        return self

    def __exit__(self, err_type, err_val, err_tb):
        self.close()


class _EventHandler(pyinotify.ProcessEvent):
    def my_init(self, cache: ConfigsFileWatcher) -> None:
        self.cache = cache

    # When acceptance testing, sometimes a move is seen as a create
    def process_IN_CREATE(self, event: pyinotify.Event) -> None:
        self.cache._process_inotify_event()
        self.cache._maybe_add_path_to_cache(event.pathname)

    def process_IN_MOVED_TO(self, event: pyinotify.Event) -> None:
        self.cache._process_inotify_event()
        self.cache._maybe_add_path_to_cache(event.pathname)

    def process_IN_DELETE(self, event: pyinotify.Event) -> None:
        self.cache._process_inotify_event()
        self.cache._maybe_remove_path_from_cache(event.pathname)

    def process_IN_Q_OVERFLOW(self, event: pyinotify.Event) -> None:
        log.warning('Got queue overflow! Recreating watchers.')
        self.cache._needs_reconfigure = True

    def process_IN_DELETE_SELF(self, event: pyinotify.Event) -> None:
        if event.pathname == os.path.abspath(self.cache._configs_folder):
            log.warning(f'{self.cache._configs_folder} was deleted! Recreating watchers.')
            self.cache._needs_reconfigure = True
