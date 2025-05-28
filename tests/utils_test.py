import uuid
from socket import SO_REUSEADDR
from socket import socket as Socket
from socket import SOL_SOCKET
from typing import cast
from unittest import mock
from unittest.mock import mock_open
from unittest.mock import patch

import pytest
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
