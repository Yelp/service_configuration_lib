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


def ephemeral_port_reserve_range(preffered_port_start: int, preferred_port_end: int, ip='127.0.0.1') -> int:
    """
    Bind to an ephemeral port, force it into the TIME_WAIT state, and unbind it.

    This means that further ephemeral port alloctions won't pick this "reserved" port,
    but subprocesses can still bind to it explicitly, given that they use SO_REUSEADDR.
    By default on linux you have a grace period of 60 seconds to reuse this port.
    To check your own particular value:
    $ cat /proc/sys/net/ipv4/tcp_fin_timeout
    60

    By default, the port will be reserved for localhost (aka 127.0.0.1).
    To reserve a port for a different ip, provide the ip as the first argument.
    Note that IP 0.0.0.0 is interpreted as localhost.

    Referenced from: https://github.com/Yelp/ephemeral-port-reserve
    """
    assert preffered_port_start <= preferred_port_end

    with contextlib.closing(socket()) as s:
        binded = False
        for port in range(preffered_port_start, preferred_port_end + 1):
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
