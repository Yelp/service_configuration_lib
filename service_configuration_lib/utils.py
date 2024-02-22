import base64
import contextlib
import errno
import hashlib
import logging
import random
import string
import uuid
from functools import lru_cache
from socket import error as SocketError
from socket import SO_REUSEADDR
from socket import socket
from socket import SOL_SOCKET
from typing import Mapping
from typing import Tuple

import yaml

DEFAULT_SPARK_RUN_CONFIG = '/nail/srv/configs/spark.yaml'
POD_TEMPLATE_PATH = '/nail/tmp/spark-pt-{file_uuid}.yaml'
SPARK_EXECUTOR_POD_TEMPLATE = '/nail/srv/configs/spark_executor_pod_template.yaml'

LOCALHOST = '127.0.0.1'
EPHEMERAL_PORT_START = 49152
EPHEMERAL_PORT_END = 65535


log = logging.Logger(__name__)
log.setLevel(logging.INFO)


def load_spark_srv_conf(preset_values=None) -> Tuple[Mapping, Mapping, Mapping, Mapping, Mapping]:
    if preset_values is None:
        preset_values = dict()
    try:
        with open(DEFAULT_SPARK_RUN_CONFIG, 'r') as fp:
            loaded_values = yaml.safe_load(fp.read())
            spark_srv_conf = {**preset_values, **loaded_values}
            spark_constants = spark_srv_conf['spark_constants']
            default_spark_srv_conf = spark_constants['defaults']
            mandatory_default_spark_srv_conf = spark_constants['mandatory_defaults']
            spark_costs = spark_constants['cost_factor']
            return (
                spark_srv_conf, spark_constants, default_spark_srv_conf,
                mandatory_default_spark_srv_conf, spark_costs,
            )
    except Exception as e:
        log.warning(f'Failed to load {DEFAULT_SPARK_RUN_CONFIG}: {e}')
        raise e


def ephemeral_port_reserve_range(
    preferred_port_start: int = EPHEMERAL_PORT_START,
    preferred_port_end: int = EPHEMERAL_PORT_END,
    ip: str = LOCALHOST,
) -> int:
    """
    Pick an available from the preferred port range. If all ports from the port range are unavailable,
    pick a random available ephemeral port.

    Implemetation referenced from upstream:
    https://github.com/Yelp/ephemeral-port-reserve/blob/master/ephemeral_port_reserve.py

    This function is used to pick a Spark UI (API) port from a pre-defined port range which is used by
    our Jupyter server metric aggregator. The aggregator API collects Prometheus metrics from multiple
    Spark sessions and exposes them through a single endpoint.
    """
    assert preferred_port_start <= preferred_port_end

    with contextlib.closing(socket()) as s:
        binded = False
        for port in range(preferred_port_start, preferred_port_end + 1):
            s.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
            try:
                s.bind((ip, port))
                binded = True
                break
            except SocketError as e:
                # socket.error: EADDRINUSE Address already in use
                if e.errno == errno.EADDRINUSE:
                    continue
                else:
                    raise
        if not binded:
            s.bind((ip, 0))

        # the connect below deadlocks on kernel >= 4.4.0 unless this arg is greater than zero
        s.listen(1)

        sockname = s.getsockname()

        # these three are necessary just to get the port into a TIME_WAIT state
        with contextlib.closing(socket()) as s2:
            s2.connect(sockname)
            sock, _ = s.accept()
            with contextlib.closing(sock):
                return sockname[1]


def get_random_string(length: int) -> str:
    return ''.join(random.choice(string.ascii_lowercase + string.digits) for _ in range(length))


def generate_pod_template_path() -> str:
    return POD_TEMPLATE_PATH.format(file_uuid=uuid.uuid4().hex)


def create_pod_template(pod_template_path: str, app_base_name: str) -> None:
    try:
        with open(SPARK_EXECUTOR_POD_TEMPLATE, 'r') as fp:
            parsed_pod_template = fp.read()
        parsed_pod_template = parsed_pod_template.format(spark_pod_label=get_k8s_resource_name_limit_size_with_hash(
            f'exec-{app_base_name}',
        ))
        parsed_pod_template = yaml.safe_load(parsed_pod_template)
        with open(pod_template_path, 'w') as f:
            yaml.dump(parsed_pod_template, f)
    except Exception as e:
        log.warning(f'Failed to read and process {SPARK_EXECUTOR_POD_TEMPLATE}: {e}')
        raise e


def get_k8s_resource_name_limit_size_with_hash(name: str, limit: int = 63, suffix: int = 4) -> str:
    """ Returns `name` unchanged if it's length does not exceed the `limit`.
        Otherwise, returns truncated `name` with its hash of size `suffix`
        appended.

        base32 encoding is chosen as it satisfies the common requirement in
        various k8s names to be alphanumeric.

        NOTE: This function is the same as paasta/paasta_tools/kubernetes_tools.py
    """
    if len(name) > limit:
        digest = hashlib.md5(name.encode()).digest()
        hashed = base64.b32encode(digest).decode().replace('=', '').lower()
        return f'{name[:(limit-suffix-1)]}-{hashed[:suffix]}'
    else:
        return name


@lru_cache(maxsize=1)
def get_runtime_env() -> str:
    try:
        with open('/nail/etc/runtimeenv', mode='r') as f:
            return f.read()
    except OSError:
        log.error('Unable to read runtimeenv - this is not expected if inside Yelp.')
        # we could also just crash or return None, but this seems a little easier to find
        # should we somehow run into this at Yelp
        return 'unknown'
