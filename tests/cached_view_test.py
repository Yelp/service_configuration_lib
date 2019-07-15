from service_configuration_lib.cached_view import _EventHandler


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
    mock_configs_file_watcher.setup.assert_called_once_with()


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


def test_service_name_and_config_from_path(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/new_service/config.json',
    )

    assert ('new_service', 'config', '.json') == result


def test_service_name_and_config_from_path_too_deep_config(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/new_service/1/2/3/config.json',
    )
    assert ('new_service', 'config', '.json') == result


def test_service_name_and_config_from_path_top_level_file(configs_file_watcher):
    result = configs_file_watcher._service_name_and_config_from_path(
        configs_file_watcher._configs_folder + '/config.json',
    )
    assert (None, 'config', '.json') == result


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


def test_configs_file_watcher_process_events(configs_file_watcher):
    configs_file_watcher._notifier.check_events.side_effect = [True, False]

    assert configs_file_watcher._notifier.read_events.call_count == 0
    assert configs_file_watcher._notifier.process_events.call_count == 0

    configs_file_watcher.process_events()

    assert configs_file_watcher._notifier.read_events.call_count == 1
    assert configs_file_watcher._notifier.process_events.call_count == 1
