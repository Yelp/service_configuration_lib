def test_yaml_configs_file_watcher_creation(yaml_configs_file_watcher):
    assert len(yaml_configs_file_watcher.configs_view.configs['foo']) == 2
    assert yaml_configs_file_watcher.configs_view.configs['foo']['smartstack'] == {'main.fake': 42}
    assert yaml_configs_file_watcher.configs_view.configs['foo']['authorization'] == \
        {'authorization': {'enabled': True}}


def test_yaml_configs_file_watcher_delete_not_presented_files(yaml_configs_file_watcher, mock_soa_dir):

    # Not present in cache, excluded by filter
    yaml_configs_file_watcher._maybe_remove_path_from_cache(mock_soa_dir.join('foo', 'smartstackX.yaml'))
    assert len(yaml_configs_file_watcher.configs_view.configs['foo']) == 2

    # Not present in cache, not excluded by filter
    assert len(yaml_configs_file_watcher.configs_view.configs['bar']) == 0
    yaml_configs_file_watcher._maybe_remove_path_from_cache(mock_soa_dir.join('bar', 'smartstack.yaml'))
    assert len(yaml_configs_file_watcher.configs_view.configs['foo']) == 2
    assert len(yaml_configs_file_watcher.configs_view.configs['bar']) == 0


def test_yaml_configs_file_watcher_delete(yaml_configs_file_watcher, mock_soa_dir):
    assert len(yaml_configs_file_watcher.configs_view.configs['foo']) == 2
    yaml_configs_file_watcher._maybe_remove_path_from_cache(mock_soa_dir.join('foo', 'smartstack.yaml'))
    assert len(yaml_configs_file_watcher.configs_view.configs['foo']) == 1
