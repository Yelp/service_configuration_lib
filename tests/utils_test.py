import uuid
from socket import SO_REUSEADDR
from socket import socket as Socket
from socket import SOL_SOCKET
from unittest import mock
from unittest.mock import mock_open
from unittest.mock import patch

import pytest

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


@pytest.fixture
def mock_runtimeenv():
    with patch('builtins.open', mock_open(read_data=MOCK_ENV_NAME)) as m:
        yield m


def test_get_runtime_env(mock_runtimeenv):
    result = utils.get_runtime_env()
    assert result == MOCK_ENV_NAME
    mock_runtimeenv.assert_called_once_with('/nail/etc/runtimeenv', mode='r')
