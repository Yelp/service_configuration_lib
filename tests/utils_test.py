import uuid
from socket import SO_REUSEADDR
from socket import socket as Socket
from socket import SOL_SOCKET
from typing import cast
from unittest import mock
from unittest.mock import mock_open
from unittest.mock import patch

import pytest
import yaml
from typing_extensions import Literal

from service_configuration_lib import utils
from service_configuration_lib.utils import ephemeral_port_reserve_range
from service_configuration_lib.utils import LOCALHOST


MOCK_ENV_NAME = 'mock_env_name'


def _bind_reuse(ip, port):
    sock = Socket()
    sock.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    sock.bind((ip, port))
    return sock


def test_preferred_port():
    port = ephemeral_port_reserve_range()
    port2 = ephemeral_port_reserve_range(port, port + 1)
    assert port == port2
    assert _bind_reuse(LOCALHOST, port2)


def test_preferred_port_in_use():
    """if preferred port is in use, it will find an unused port"""
    port = ephemeral_port_reserve_range()
    sock = _bind_reuse(LOCALHOST, port)
    sock.listen(1)  # make the port in-use

    port_end = port + 10
    port2 = ephemeral_port_reserve_range(port, port_end)
    assert port != port2
    assert port2 > port and port2 <= port_end
    assert _bind_reuse(LOCALHOST, port2)


def test_get_random_string():
    length = 50
    result = utils.get_random_string(50)
    assert len(result) == length


@pytest.mark.parametrize(
    'instance_name,expected_instance_label',
    (
        ('my_job.do_something', 'my_job.do_something'),
        (
            f"my_job.{'a'* 100}",
            'my_job.aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-6xhe',
        ),
    ),
)
def test_get_k8s_resource_name_limit_size_with_hash(instance_name, expected_instance_label):
    assert expected_instance_label == utils.get_k8s_resource_name_limit_size_with_hash(instance_name)


@pytest.mark.parametrize(
    'hex_value', [
        '656bf9032f014bc0a5e8a7be9e25d414',
        '4185e3acdb4b4317af2659f6dd16ffb2',
    ],
)
def test_generate_pod_template_path(hex_value):
    with mock.patch.object(uuid, 'uuid4', return_value=uuid.UUID(hex=hex_value)):
        assert utils.generate_pod_template_path() == f'/nail/tmp/spark-pt-{hex_value}.yaml'


@pytest.mark.parametrize(
    'mem_str,unit_str,expected_mem',
    (
        ('13425m', 'm', 13425),  # Simple case
        ('138412032', 'm', 132),  # Bytes to MB
        ('65536k', 'g', 0.0625),  # KB to GB
        ('1t', 'g', 1024),  # TB to GB
        ('1.5g', 'm', 1536),  # GB to MB with decimal
        ('2048k', 'm', 2),  # KB to MB
        ('0.5g', 'k', 524288),  # GB to KB
        ('32768m', 't', 0.03125),  # MB to TB
        ('1.5t', 'm', 1572864),  # TB to MB with decimal
    ),
)
def test_get_spark_memory_in_unit(mem_str, unit_str, expected_mem):
    assert expected_mem == utils.get_spark_memory_in_unit(mem_str, cast(Literal['k', 'm', 'g', 't'], unit_str))


@pytest.mark.parametrize(
    'mem_str,unit_str',
    [
        ('invalid', 'm'),
        ('1024mb', 'g'),
    ],
)
def test_get_spark_memory_in_unit_exceptions(mem_str, unit_str):
    with pytest.raises((ValueError, IndexError)):
        utils.get_spark_memory_in_unit(mem_str, cast(Literal['k', 'm', 'g', 't'], unit_str))


@pytest.mark.parametrize(
    'spark_conf,expected_mem',
    [
        ({'spark.driver.memory': '13425m'}, 13425),  # Simple case
        ({'spark.driver.memory': '138412032'}, 132),  # Bytes to MB
        ({'spark.driver.memory': '65536k'}, 64),  # KB to MB
        ({'spark.driver.memory': '1g'}, 1024),  # GB to MB
        ({'spark.driver.memory': 'invalid'}, utils.SPARK_DRIVER_MEM_DEFAULT_MB),  # Invalid case
        ({'spark.driver.memory': '1.5g'}, 1536),  # GB to MB with decimal
        ({'spark.driver.memory': '2048k'}, 2),  # KB to MB
        ({'spark.driver.memory': '0.5t'}, 524288),  # TB to MB
        ({'spark.driver.memory': '1024m'}, 1024),  # MB to MB
        ({'spark.driver.memory': '1.5t'}, 1572864),  # TB to MB with decimal
    ],
)
def test_get_spark_driver_memory_mb(spark_conf, expected_mem):
    assert expected_mem == utils.get_spark_driver_memory_mb(spark_conf)


@pytest.mark.parametrize(
    'spark_conf,expected_mem_overhead',
    [
        ({'spark.driver.memoryOverhead': '1024'}, 1024),  # Simple case
        ({'spark.driver.memoryOverhead': '1g'}, 1024),  # GB to MB
        ({'spark.driver.memory': '10240m', 'spark.driver.memoryOverheadFactor': '0.2'}, 2048),  # Custom OverheadFactor
        ({'spark.driver.memory': '10240m'}, 1024),  # Using default overhead factor
        (
            {'spark.driver.memory': 'invalid'},
            utils.SPARK_DRIVER_MEM_DEFAULT_MB * utils.SPARK_DRIVER_MEM_OVERHEAD_FACTOR_DEFAULT,
        ),
        # Invalid case
        ({'spark.driver.memoryOverhead': '1.5g'}, 1536),  # GB to MB with decimal
        ({'spark.driver.memory': '2048k', 'spark.driver.memoryOverheadFactor': '0.05'}, 0.1),
        # KB to MB with custom factor
        ({'spark.driver.memory': '0.5t', 'spark.driver.memoryOverheadFactor': '0.15'}, 78643.2),
        # TB to MB with custom factor
        ({'spark.driver.memory': '1024m', 'spark.driver.memoryOverheadFactor': '0.25'}, 256),
        # MB to MB with custom factor
        ({'spark.driver.memory': '1.5t', 'spark.driver.memoryOverheadFactor': '0.05'}, 78643.2),
        # TB to MB with custom factor
    ],
)
def test_get_spark_driver_memory_overhead_mb(spark_conf, expected_mem_overhead):
    assert expected_mem_overhead == utils.get_spark_driver_memory_overhead_mb(spark_conf)


@pytest.fixture
def mock_runtimeenv():
    # Clear the lru_cache before applying the mock
    utils.get_runtime_env.cache_clear()
    with patch('builtins.open', mock_open(read_data=MOCK_ENV_NAME)) as m:
        yield m


def test_get_runtime_env(mock_runtimeenv):
    result = utils.get_runtime_env()
    assert result == MOCK_ENV_NAME
    mock_runtimeenv.assert_called_once_with('/nail/etc/runtimeenv', mode='r')


@pytest.fixture
def mock_utils_log(monkeypatch):
    mock_log = mock.Mock()
    monkeypatch.setattr(utils, 'log', mock_log)
    return mock_log


class TestClogConfiguration:

    @pytest.fixture
    def mock_load_spark_srv_conf(self, monkeypatch):
        mock_fn = mock.Mock(
            return_value=(
                {},  # spark_srv_conf
                {},  # spark_constants
                {},  # default_spark_srv_conf
                {},  # mandatory_default_spark_srv_conf
                {},  # spark_costs
                [],  # module_configs
            ),
        )
        monkeypatch.setattr(utils, 'load_spark_srv_conf', mock_fn)
        return mock_fn

    @pytest.fixture
    def mock_srv_configs_use_file(self, monkeypatch):
        mock_fn = mock.Mock()
        monkeypatch.setattr(utils.srv_configs, 'use_file', mock_fn)
        return mock_fn

    @pytest.fixture
    def mock_srv_configs_get_namespace(self, monkeypatch):
        mock_fn = mock.Mock()
        monkeypatch.setattr(utils.srv_configs, 'get_namespace_as_dict', mock_fn)
        return mock_fn

    @pytest.fixture
    def mock_os_path_exists(self, monkeypatch):
        mock_fn = mock.Mock(return_value=False)
        monkeypatch.setattr(utils.os.path, 'exists', mock_fn)
        return mock_fn

    @pytest.fixture
    def mock_monk_handler(self, monkeypatch):
        mock_class = mock.Mock()
        monkeypatch.setattr(utils, 'MonkHandler', mock_class)
        return mock_class

    @pytest.fixture
    def mock_clog_configure(self, monkeypatch):
        mock_fn = mock.Mock()
        monkeypatch.setattr(utils.clog.config, 'configure_from_dict', mock_fn)
        return mock_fn

    @pytest.fixture
    def mock_os_getenv(self, monkeypatch):
        mock_fn = mock.Mock(return_value=None)
        monkeypatch.setattr(utils.os, 'getenv', mock_fn)
        return mock_fn

    # Tests for _load_default_service_configurations_for_clog
    def test_load_clog_config_no_module_configs(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, [])  # Empty module_configs
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.warning.assert_called_once_with(
            f"Could not find 'clog' namespace entry in 'module_config' "
            f'section within {utils.DEFAULT_SPARK_RUN_CONFIG}.',
        )

    def test_load_clog_config_no_clog_namespace(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        module_configs = [{'namespace': 'other', 'config': {'key': 'val'}}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.warning.assert_called_once_with(
            f"Could not find 'clog' namespace entry in 'module_config' "
            f'section within {utils.DEFAULT_SPARK_RUN_CONFIG}.',
        )

    def test_load_clog_config_clog_namespace_no_file_no_config(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        module_configs = [{'namespace': 'clog'}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.info.assert_any_call(
            f"No 'file' specified for 'clog' namespace in 'module_config' of {utils.DEFAULT_SPARK_RUN_CONFIG}. "
            'Not loading any external file for clog via module_config.',
        )

    def test_load_clog_config_file_does_not_exist(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_os_path_exists,
    ):
        test_file_path = '/fake/clog.yaml'
        module_configs = [{'namespace': 'clog', 'file': test_file_path}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        mock_os_path_exists.return_value = False

        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_os_path_exists.assert_called_once_with(test_file_path)
        mock_utils_log.error.assert_called_once_with(
            f"Clog configuration file specified in 'module_config' of {utils.DEFAULT_SPARK_RUN_CONFIG} "
            f'does not exist: {test_file_path}.',
        )

    def test_load_clog_config_file_exists_use_file_ok_no_inline_config(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_os_path_exists, mock_srv_configs_use_file,
    ):
        test_file_path = '/fake/clog.yaml'
        module_configs = [{'namespace': 'clog', 'file': test_file_path}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        mock_os_path_exists.return_value = True

        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_os_path_exists.assert_called_once_with(test_file_path)
        mock_srv_configs_use_file.assert_called_once_with(test_file_path, namespace='clog')
        mock_utils_log.info.assert_any_call(
            f"Successfully loaded clog configuration file {test_file_path} into namespace 'clog'.",
        )

    def test_load_clog_config_file_exists_use_file_raises_error(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_os_path_exists, mock_srv_configs_use_file,
    ):
        test_file_path = '/fake/clog.yaml'
        module_configs = [{'namespace': 'clog', 'file': test_file_path}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        mock_os_path_exists.return_value = True
        mock_srv_configs_use_file.side_effect = Exception('Mock srv_configs error')

        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_srv_configs_use_file.assert_called_once_with(test_file_path, namespace='clog')
        mock_utils_log.error.assert_called_once_with(
            f'Error loading clog configuration file {test_file_path} '
            f'using srv_configs.use_file: Mock srv_configs error',
        )

    def test_load_clog_config_no_file_with_valid_inline_config(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        inline_conf = {'log_stream_name': 'test_stream'}
        module_configs = [{'namespace': 'clog', 'config': inline_conf}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)

        result = utils._load_default_service_configurations_for_clog()
        assert result == inline_conf
        mock_utils_log.info.assert_any_call(
            f"No 'file' specified for 'clog' namespace in 'module_config' of {utils.DEFAULT_SPARK_RUN_CONFIG}. "
            'Not loading any external file for clog via module_config.',
        )

    def test_load_clog_config_no_file_with_invalid_inline_config(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        inline_conf = 'not_a_dict'
        module_configs = [{'namespace': 'clog', 'config': inline_conf}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)

        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.warning.assert_called_once_with(
            f"Inline 'config' for 'clog' namespace in {utils.DEFAULT_SPARK_RUN_CONFIG} is not a dictionary.",
        )

    def test_load_clog_config_main_config_file_not_found(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        mock_load_spark_srv_conf.side_effect = FileNotFoundError(f'File not found: {utils.DEFAULT_SPARK_RUN_CONFIG}')
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.error.assert_called_once_with(
            f'Error: Main Spark run config file {utils.DEFAULT_SPARK_RUN_CONFIG} not found. '
            'Cannot process clog configurations.',
        )

    def test_load_clog_config_yaml_error(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        mock_load_spark_srv_conf.side_effect = yaml.YAMLError('Mock YAML error')
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.error.assert_called_once_with(
            f'Error parsing YAML from {utils.DEFAULT_SPARK_RUN_CONFIG}: Mock YAML error',
        )

    def test_load_clog_config_unexpected_error(
        self, mock_utils_log, mock_load_spark_srv_conf,
    ):
        mock_load_spark_srv_conf.side_effect = Exception('Unexpected mock error')
        result = utils._load_default_service_configurations_for_clog()
        assert result is None
        mock_utils_log.error.assert_called_once_with(
            'An unexpected error occurred in _load_default_service_configurations_for_clog: Unexpected mock error',
        )

    # Tests for get_clog_handler
    def test_get_clog_handler_stream_override(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        test_stream = 'override_stream'
        test_client_id = 'test_client'
        mock_os_getenv.return_value = 'env_user'  # To ensure client_id is not None

        handler_instance = mock.Mock()
        mock_monk_handler.return_value = handler_instance

        result = utils.get_clog_handler(client_id=test_client_id, stream_name_override=test_stream)

        assert result == handler_instance
        mock_clog_configure.assert_called_once_with({'monk_disable': False})
        mock_monk_handler.assert_called_once_with(
            client_id=test_client_id,
            host=utils.monk_host,
            port=utils.monk_port,
            stream=test_stream,
        )
        # _load_default_service_configurations_for_clog should still be called
        mock_load_spark_srv_conf.assert_called_once()

    def test_get_clog_handler_inline_config_stream(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        inline_stream = 'inline_config_stream'
        inline_conf = {'log_stream_name': inline_stream}
        module_configs = [{'namespace': 'clog', 'config': inline_conf}]
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, module_configs)
        mock_os_getenv.return_value = 'test_user'

        handler_instance = mock.Mock()
        mock_monk_handler.return_value = handler_instance

        result = utils.get_clog_handler()

        assert result == handler_instance
        mock_utils_log.info.assert_any_call(
            f"Using log_stream_name '{inline_stream}' from inline module_config in {utils.DEFAULT_SPARK_RUN_CONFIG}.",
        )
        mock_clog_configure.assert_called_once_with({'monk_disable': False})
        mock_monk_handler.assert_called_once_with(
            client_id='test_user',
            host=utils.monk_host,
            port=utils.monk_port,
            stream=inline_stream,
        )

    def test_get_clog_handler_srv_configs_stream(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_srv_configs_get_namespace,
        mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        srv_configs_stream = 'srv_configs_stream'
        # No inline config for 'clog'
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, [{'namespace': 'clog'}])
        mock_srv_configs_get_namespace.return_value = {'log_stream_name': srv_configs_stream}
        mock_os_getenv.return_value = 'test_user'

        handler_instance = mock.Mock()
        mock_monk_handler.return_value = handler_instance

        result = utils.get_clog_handler()

        assert result == handler_instance
        mock_srv_configs_get_namespace.assert_called_once_with('clog')
        mock_utils_log.info.assert_any_call(
            f"Using log_stream_name '{srv_configs_stream}' from srv_configs 'clog' namespace (likely from external file).",  # noqa E501
        )
        mock_clog_configure.assert_called_once_with({'monk_disable': False})
        mock_monk_handler.assert_called_once_with(
            client_id='test_user',
            host=utils.monk_host,
            port=utils.monk_port,
            stream=srv_configs_stream,
        )

    def test_get_clog_handler_no_stream_name_found(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_srv_configs_get_namespace, mock_monk_handler,
    ):
        # No inline config, and srv_configs doesn't provide it either
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, [{'namespace': 'clog'}])
        mock_srv_configs_get_namespace.return_value = {}  # No log_stream_name

        result = utils.get_clog_handler()

        assert result is None
        mock_utils_log.error.assert_called_once_with(
            'Clog stream_name could not be determined. It was not provided as an argument, '
            'not found in the inline module_config for "clog", and not found in the '
            '"clog" srv_configs namespace. Clog handler cannot be configured.',
        )
        mock_monk_handler.assert_not_called()

    def test_get_clog_handler_default_client_id(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        test_stream = 'default_client_stream'
        mock_os_getenv.return_value = 'current_user'  # USER env var is set

        handler_instance = mock.Mock()
        mock_monk_handler.return_value = handler_instance

        result = utils.get_clog_handler(stream_name_override=test_stream)

        assert result == handler_instance
        mock_monk_handler.assert_called_once_with(
            client_id='current_user',
            host=utils.monk_host,
            port=utils.monk_port,
            stream=test_stream,
        )

    def test_get_clog_handler_client_id_unknown_spark_user(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        test_stream = 'unknown_user_stream'
        mock_os_getenv.return_value = None  # USER env var is NOT set

        handler_instance = mock.Mock()
        mock_monk_handler.return_value = handler_instance

        result = utils.get_clog_handler(stream_name_override=test_stream)

        assert result == handler_instance
        mock_monk_handler.assert_called_once_with(
            client_id='unknown_spark_user',  # Fallback client_id
            host=utils.monk_host,
            port=utils.monk_port,
            stream=test_stream,
        )

    def test_get_clog_handler_monk_handler_creation_fails(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        test_stream = 'handler_fail_stream'
        mock_os_getenv.return_value = 'test_user'
        mock_monk_handler.side_effect = Exception('MonkHandler creation failed')

        result = utils.get_clog_handler(stream_name_override=test_stream)

        assert result is None
        mock_utils_log.error.assert_called_once_with(
            f"Failed to create MonkHandler for clog with stream '{test_stream}'. Error: MonkHandler creation failed",
        )

    def test_get_clog_handler_srv_configs_get_namespace_fails(
        self, mock_utils_log, mock_load_spark_srv_conf, mock_srv_configs_get_namespace,
        mock_monk_handler, mock_clog_configure, mock_os_getenv,
    ):
        # No inline config
        mock_load_spark_srv_conf.return_value = ({}, {}, {}, {}, {}, [{'namespace': 'clog'}])
        mock_srv_configs_get_namespace.side_effect = Exception('srv_configs error')
        mock_os_getenv.return_value = 'test_user'

        result = utils.get_clog_handler()  # No stream_name_override

        assert result is None  # Should fail as stream name cannot be determined
        mock_utils_log.warning.assert_any_call(
            "Could not get 'clog' namespace from srv_configs or 'log_stream_name' key missing. "
            'This may be okay if stream_name_override or inline config provides it. Error: srv_configs error',
        )
        mock_utils_log.error.assert_called_with(
            'Clog stream_name could not be determined. It was not provided as an argument, '
            'not found in the inline module_config for "clog", and not found in the '
            '"clog" srv_configs namespace. Clog handler cannot be configured.',
        )
        mock_monk_handler.assert_not_called()
