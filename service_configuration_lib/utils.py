import contextlib
import errno
import logging
from socket import error as SocketError
from socket import SO_REUSEADDR
from socket import socket
from socket import SOL_SOCKET
from typing import Mapping
from typing import Tuple

import yaml


DEFAULT_SPARK_RUN_CONFIG = '/nail/srv/configs/spark.yaml'

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


def ephemeral_port_reserve_range(preferred_port_start: int, preferred_port_end: int, ip='127.0.0.1') -> int:
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
