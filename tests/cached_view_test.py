import mock
import pytest

from service_configuration_lib.cached_view import _EventHandler
from service_configuration_lib.cached_view import ConfigsFileWatcher
from service_configuration_lib.yaml_cached_view import YamlConfigsCachedView


def test_event_handler_create(mock_configs_file_watcher, mock_event):
    event_handler = _EventHandler(cache=mock_configs_file_watcher)
    event_handler.process_IN_CREATE(event=mock_event)
    mock_configs_file_watcher._maybe_add_path_to_cache.assert_called_once_with(mock_event.pathname)


def test_event_handler_move(mock_configs_file_watcher, mock_event):
    event_handler = _EventHandler(cache=mock_configs_file_watcher)
    event_handler.process_IN_MOVED_TO(event=mock_event)
    mock_configs_file_watcher._maybe_add_path_to_cache.assert_called_once_with(mock_event.pathname)


def test_event_handler_delete(mock_configs_file_watcher, mock_event):
    event_handler = _EventHandler(cache=mock_configs_file_watcher)
    event_handler.process_IN_DELETE(event=mock_event)
    mock_configs_file_watcher._maybe_remove_path_from_cache.assert_called_once_with(mock_event.pathname)


def test_event_handler_overflow(mock_configs_file_watcher, mock_event):
    event_handler = _EventHandler(cache=mock_configs_file_watcher)
    event_handler.process_IN_Q_OVERFLOW(event=mock_event)
    assert mock_configs_file_watcher._needs_reconfigure


@pytest.mark.parametrize('deleted_folder', ['/foo', '/foo/bar'])
def test_event_handler_delete_self(mock_configs_file_watcher, mock_event, deleted_folder):
    event_handler = _EventHandler(cache=mock_configs_file_watcher)
    mock_event.pathname = deleted_folder
    event_handler.process_IN_DELETE_SELF(event=mock_event)
    assert mock_configs_file_watcher._needs_reconfigure == (deleted_folder == '/foo')


def test_exclude_filter_exclude_folders(configs_file_watcher):
    assert configs_file_watcher._exclude_filter('/foo/bar/.~tmp~')
    assert not configs_file_watcher._exclude_filter('/foo/bar/baz')


def test_exclude_filter_service_names_filtering(configs_file_watcher):
    configs_file_watcher._services_names = ['myservice', 'another_service', 'star_service*']

    assert configs_file_watcher._exclude_filter('/foo/bar/baz')
    assert not configs_file_watcher._exclude_filter('/foo/myservice')
    assert not configs_file_watcher._exclude_filter('/foo/another_service')
    assert configs_file_watcher._exclude_filter('/foo/another_service2')
    assert not configs_file_watcher._exclude_filter('/foo/star_service2')
    assert not configs_file_watcher._exclude_filter('/foo/star_service')
    assert not configs_file_watcher._exclude_filter('/foo/star_services/autotuned_defaults')


def test_stopping_notifier(configs_file_watcher):
    notifier = configs_file_watcher._notifier
    assert notifier is not None
    configs_file_watcher.close()

    assert configs_file_watcher._notifier is None
    notifier.stop.assert_called_once()


def test_service_name_and_config_from_path(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/new_service/config.json',
    )

    assert ('new_service', 'config', '.json') == result


def test_service_name_and_config_from_path_too_deep_config(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/new_service/1/2/3/config.json',
    )
    assert result.service_name == 'new_service'
    assert result.config_name == 'config'
    assert result.config_suffix == '.json'


def test_service_name_and_config_from_path_top_level_file(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/config.json',
    )
    assert result.service_name is None
    assert result.config_name == 'config'
    assert result.config_suffix == '.json'


def test_service_name_and_config_from_path_with_configs_names(configs_file_watcher):
    configs_file_watcher._configs_names = ['config', 'new_config*']
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/service12/configX.json',
    )

    assert result is None


def test_service_name_and_config_from_path_with_config_suffixes(configs_file_watcher):
    configs_file_watcher._configs_suffixes = ['.yaml']
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/service12/config.json',
    )

    assert result is None


def test_configs_file_watcher_process_events_with_limit(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, True, True, True, False]
    configs_file_watcher._notifier.process_events.side_effect = configs_file_watcher._process_inotify_event
    assert configs_file_watcher._processed_events_count == 0

    configs_file_watcher.process_events(limit=2)

    assert configs_file_watcher._processed_events_count == 2
    assert configs_file_watcher._notifier.read_events.call_count == 2
    assert configs_file_watcher._notifier.process_events.call_count == 2


def test_configs_file_watcher_process_events_with_limit_2nd_iteration(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, True, True, True, False]
    configs_file_watcher._notifier.process_events.side_effect = configs_file_watcher._process_inotify_event

    configs_file_watcher.process_events(limit=2)
    configs_file_watcher.process_events(limit=2)

    assert configs_file_watcher._processed_events_count == 4
    assert configs_file_watcher._notifier.read_events.call_count == 4
    assert configs_file_watcher._notifier.process_events.call_count == 4


def test_configs_file_watcher_process_events_with_overflow_in_the_middle(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, True, True, True, False]
    configs_file_watcher._processed_events_count = 2
    configs_file_watcher._notifier.process_events.side_effect = configs_file_watcher.setup

    configs_file_watcher.process_events(limit=3)

    assert configs_file_watcher._processed_events_count == 0  # because overflow was resetting the counter
    assert configs_file_watcher._notifier.read_events.call_count == 1
    assert configs_file_watcher._notifier.process_events.call_count == 1


def test_configs_file_watcher_process_events(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, False]

    assert configs_file_watcher._notifier.read_events.call_count == 0
    assert configs_file_watcher._notifier.process_events.call_count == 0

    configs_file_watcher.setup = mock.Mock()
    configs_file_watcher.process_events()

    assert configs_file_watcher._notifier.read_events.call_count == 1
    assert configs_file_watcher._notifier.process_events.call_count == 1
    assert configs_file_watcher.setup.call_count == 0


def test_configs_file_watcher_process_events_reconfigure(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, True, False]
    configs_file_watcher._needs_reconfigure = True

    assert configs_file_watcher._notifier.read_events.call_count == 0
    assert configs_file_watcher._notifier.process_events.call_count == 0

    configs_file_watcher.setup = mock.Mock()
    configs_file_watcher.process_events()

    assert configs_file_watcher._notifier.read_events.call_count == 1
    assert configs_file_watcher._notifier.process_events.call_count == 1
    assert configs_file_watcher.setup.call_count == 1


def test_wildcard_configs_names(
    tmpdir,
    mock_inotify_constants,
    mock_watch_manager,
    mock_notifier,
):
    tmpdir.join('foo', 'bar-dev.yaml').write('{baz: 42}', ensure=True)
    tmpdir.join('foo', 'bar-prod.yaml').write('{baz: 3939}', ensure=True)

    watcher = ConfigsFileWatcher(
        configs_view=YamlConfigsCachedView(),
        configs_names=['bar-*'],
        configs_suffixes=['.yaml'],
        configs_folder=tmpdir,
    )

    assert watcher.configs_view.configs == {
        'foo': {
            'bar-dev': {
                'baz': 42,
            },
            'bar-prod': {
                'baz': 3939,
            },
        },
    }
