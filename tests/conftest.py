from unittest.mock import MagicMock
from unittest.mock import Mock
from unittest.mock import patch

import pytest

from service_configuration_lib.cached_view import BaseCachedView
from service_configuration_lib.cached_view import ConfigsFileWatcher
from service_configuration_lib.yaml_cached_view import YamlConfigsCachedView


@pytest.fixture
def mock_configs_file_watcher():
    return MagicMock(spec=ConfigsFileWatcher)


@pytest.fixture
def mock_event():
    return Mock(pathname='foo')


@pytest.fixture
def mock_inotify_constants():
    with patch('pyinotify.max_queued_events', Mock(value=101)), \
            patch('pyinotify.max_user_instances', Mock(value=102)), \
            patch('pyinotify.max_user_watches', Mock(value=103)):
        yield


@pytest.fixture
def mock_soa_dir(tmpdir):
    tmpdir.join('foo', 'smartstack.yaml').write('{main.fake: 42}', ensure=True)
    tmpdir.join('foo', 'something').ensure()
    tmpdir.join('foo', 'authorization.yaml').write('{authorization: {enabled: True}}', ensure=True)
    tmpdir.join('bar', 'other.yaml').ensure()
    tmpdir.join('bar', 'smartstack.yaml').write('{', ensure=True)  # YAMLError
    tmpdir.join('baz', 'smartstack.yaml').ensure().chmod(0o000)  # OSError
    return tmpdir


@pytest.fixture
def mock_watch_manager():
    with patch('pyinotify.WatchManager', autospec=True):
        yield


@pytest.fixture
def mock_notifier():
    with patch('pyinotify.Notifier', autospec=True):
        yield


@pytest.fixture
def mock_configs_view():
    configs_view = Mock(spec=BaseCachedView)
    return configs_view


@pytest.fixture
def configs_file_watcher(
    mock_soa_dir,
    mock_inotify_constants,
    mock_watch_manager,
    mock_notifier,
    mock_configs_view,
):
    yield ConfigsFileWatcher(configs_view=mock_configs_view, configs_folder=mock_soa_dir)


@pytest.fixture
def yaml_configs_file_watcher(
    mock_soa_dir,
    mock_inotify_constants,
    mock_watch_manager,
    mock_notifier,
):
    yield ConfigsFileWatcher(
        configs_view=YamlConfigsCachedView(),
        configs_names=['smartstack', 'authorization'],
        configs_suffixes=['.yaml'],
        configs_folder=mock_soa_dir,
    )
