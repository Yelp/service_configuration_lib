import base64
import functools
import hashlib
import itertools
import json
import logging
import os
import time
from typing import Any
from typing import Dict
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional
from typing import Tuple
from urllib.parse import urlparse

import boto3
import ephemeral_port_reserve
import requests
import yaml
from boto3 import Session

AWS_CREDENTIALS_DIR = '/etc/boto_cfg/'
AWS_TEMP_CREDENTIALS_PROVIDER = 'org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider'
GPU_POOLS_YAML_FILE_PATH = '/nail/srv/configs/gpu_pools.yaml'
DEFAULT_PAASTA_VOLUME_PATH = '/etc/paasta/volumes.json'
DEFAULT_SPARK_MESOS_SECRET_FILE = '/nail/etc/paasta_spark_secret'
DEFAULT_SPARK_RUN_CONFIG = '/nail/srv/configs/spark.yaml'
DEFAULT_SPARK_SERVICE = 'spark'
GPUS_HARD_LIMIT = 15
CLUSTERMAN_METRICS_YAML_FILE_PATH = '/nail/srv/configs/clusterman_metrics.yaml'
CLUSTERMAN_YAML_FILE_PATH = '/nail/srv/configs/clusterman.yaml'
DEFAULT_MAX_CORES = 4
DEFAULT_EXECUTOR_CORES = 2
DEFAULT_EXECUTOR_INSTANCES = 2
DEFAULT_EXECUTOR_MEMORY = '4g'
DEFAULT_K8S_LABEL_LENGTH = 63
DEFAULT_K8S_BATCH_SIZE = 512
DEFAULT_RESOURCES_WAITING_TIME_PER_EXECUTOR = 2  # seconds
DEFAULT_CLUSTERMAN_OBSERVED_SCALING_TIME = 15  # minutes
DEFAULT_SQL_SHUFFLE_PARTITIONS = 128
DEFAULT_DRA_EXECUTOR_ALLOCATION_RATIO = 0.8
DEFAULT_DRA_CACHED_EXECUTOR_IDLE_TIMEOUT = '900s'
DEFAULT_DRA_MIN_EXECUTOR_RATIO = 0.25


NON_CONFIGURABLE_SPARK_OPTS = {
    'spark.master',
    'spark.ui.port',
    'spark.mesos.principal',
    'spark.mesos.secret',
    'spark.mesos.executor.docker.image',
    'spark.mesos.executor.docker.parameters',
    'spark.executorEnv.PAASTA_SERVICE',
    'spark.executorEnv.PAASTA_INSTANCE',
    'spark.executorEnv.PAASTA_CLUSTER',
    'spark.executorEnv.SPARK_EXECUTOR_DIRS',
    'spark.kubernetes.pyspark.pythonVersion',
    'spark.kubernetes.container.image',
    'spark.kubernetes.namespace',
    'spark.kubernetes.authenticate.caCertFile',
    'spark.kubernetes.authenticate.clientKeyFile',
    'spark.kubernetes.authenticate.clientCertFile',
    'spark.kubernetes.container.image.pullPolicy',
    'spark.kubernetes.executor.label.yelp.com/paasta_service',
    'spark.kubernetes.executor.label.yelp.com/paasta_instance',
    'spark.kubernetes.executor.label.yelp.com/paasta_cluster',
    'spark.kubernetes.executor.label.paasta.yelp.com/service',
    'spark.kubernetes.executor.label.paasta.yelp.com/instance',
    'spark.kubernetes.executor.label.paasta.yelp.com/cluster',
}
K8S_AUTH_FOLDER = '/etc/pki/spark'
K8S_BASE_VOLUMES: List[Dict[str, str]] = [
    {'containerPath': K8S_AUTH_FOLDER, 'hostPath': K8S_AUTH_FOLDER, 'mode': 'RO'},
    {'containerPath': '/etc/passwd', 'hostPath': '/etc/passwd', 'mode': 'RO'},
    {'containerPath': '/etc/group', 'hostPath': '/etc/group', 'mode': 'RO'},
]

log = logging.Logger(__name__)
log.setLevel(logging.INFO)


SUPPORTED_CLUSTER_MANAGERS = ['mesos', 'kubernetes', 'local']


class UnsupportedClusterManagerException(Exception):

    def __init__(self, manager: str):
        msg = f'Unsupported cluster manager {manager}, must be one of {SUPPORTED_CLUSTER_MANAGERS}'
        super().__init__(msg)


def _load_aws_credentials_from_yaml(yaml_file_path) -> Tuple[str, str, Optional[str]]:
    with open(yaml_file_path, 'r') as yaml_file:
        try:
            credentials_yaml = yaml.safe_load(yaml_file.read())
            return (
                credentials_yaml['aws_access_key_id'],
                credentials_yaml['aws_secret_access_key'],
                credentials_yaml.get('aws_session_token', None),
            )
        except Exception as e:
            raise ValueError(
                f'Encountered {type(e)} when trying to parse AWS credentials yaml {yaml_file_path}'
                'Suppressing further output to avoid leaking credentials.',
            )


def get_aws_credentials(
    service: Optional[str] = DEFAULT_SPARK_SERVICE,
    no_aws_credentials: bool = False,
    aws_credentials_yaml: Optional[str] = None,
    profile_name: Optional[str] = None,
    session: Optional[boto3.Session] = None,
    aws_credentials_json: Optional[str] = None,
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """load aws creds using different method/file"""
    if no_aws_credentials:
        return None, None, None
    elif aws_credentials_yaml:
        return _load_aws_credentials_from_yaml(aws_credentials_yaml)
    elif aws_credentials_json:
        with open(aws_credentials_json, 'r') as f:
            creds = json.load(f)
        return (creds.get('accessKeyId'), creds.get('secretAccessKey'), None)
    elif service != DEFAULT_SPARK_SERVICE:
        service_credentials_path = os.path.join(AWS_CREDENTIALS_DIR, f'{service}.yaml')
        if os.path.exists(service_credentials_path):
            return _load_aws_credentials_from_yaml(service_credentials_path)
        elif not session:
            log.warning(
                f'Did not find service AWS credentials at {service_credentials_path}. '
                'Falling back to user credentials.',
            )

    session = session or Session(profile_name=profile_name)
    creds = session.get_credentials()
    return (
        creds.access_key,
        creds.secret_key,
        creds.token,
    )


def _pick_random_port(app_name):
    """Return a random port. """
    hash_key = f'{app_name}_{time.time()}'.encode('utf-8')
    hash_number = int(hashlib.sha1(hash_key).hexdigest(), 16)
    preferred_port = 33000 + (hash_number % 25000)
    return ephemeral_port_reserve.reserve('0.0.0.0', preferred_port)


def _get_mesos_docker_volumes_conf(
    spark_opts: Mapping[str, str],
    extra_volumes: Optional[List[Mapping[str, str]]] = None,
    load_paasta_default_volumes: bool = False,
) -> Dict[str, str]:
    """return volume str to be configured for spark.mesos.executor.docker.volume
    if no extra_volumes and volumes_from_spark_opts, it will read from
    DEFAULT_PAASTA_VOLUME_PATH and parse it.

    Also spark required to have `/etc/passwd` and `/etc/group` being mounted as
    well. This will ensure it does have those files in the list.
    """
    volume_str = spark_opts.get('spark.mesos.executor.docker.volumes')
    volumes = volume_str.split(',') if volume_str else []

    if load_paasta_default_volumes:
        with open(DEFAULT_PAASTA_VOLUME_PATH) as fp:
            extra_volumes = (extra_volumes or []) + json.load(fp)['volumes']

    for volume in (extra_volumes or []):
        if os.path.exists(volume['hostPath']):
            volumes.append(f"{volume['hostPath']}:{volume['containerPath']}:{volume['mode'].lower()}")
        else:
            log.warning(f"Path {volume['hostPath']} does not exist on this host. Skipping this bindings.")

    distinct_volumes = set(volumes)

    # docker.parameters user needs /etc/passwd and /etc/group to be mounted
    for required in ['/etc/passwd', '/etc/group']:
        full_mount_str = f'{required}:{required}:ro'
        if full_mount_str not in distinct_volumes:
            distinct_volumes.add(full_mount_str)

    volume_str = ','.join(distinct_volumes)  # ensure we don't have duplicated files
    return {'spark.mesos.executor.docker.volumes': volume_str}


def _get_k8s_docker_volumes_conf(
    volumes: Optional[List[Mapping[str, str]]] = None,
):
    env = {}
    mounted_volumes = set()
    k8s_volumes = volumes or []
    k8s_volumes.extend(K8S_BASE_VOLUMES)
    _get_k8s_volume = functools.partial(_get_k8s_volume_hostpath_dict, count=itertools.count())

    for volume in k8s_volumes:
        host_path, container_path, mode = volume['hostPath'], volume['containerPath'], volume['mode']
        if os.path.exists(host_path) and host_path not in mounted_volumes:
            env.update(_get_k8s_volume(host_path, container_path, mode))
            mounted_volumes.add(host_path)
        else:
            log.warning(
                f'Path {host_path} does not exist on this host or it has already been mounted.'
                ' Skipping this bindings.',
            )
    return env


def _get_k8s_volume_hostpath_dict(host_path: str, container_path: str, mode: str, count: itertools.count):
    volume_name = next(count)
    return {
        f'spark.kubernetes.executor.volumes.hostPath.{volume_name}.mount.path': container_path,
        f'spark.kubernetes.executor.volumes.hostPath.{volume_name}.options.path': host_path,
        f'spark.kubernetes.executor.volumes.hostPath.{volume_name}.mount.readOnly': (
            'true' if mode.lower() == 'ro' else 'false'
        ),
    }


def _append_spark_config(spark_opts: Dict[str, str], config_name: str, config_value: str) -> Dict[str, str]:
    # config already defined by the user, don't modify
    if config_name in spark_opts:
        return spark_opts

    # append the config
    spark_opts[config_name] = config_value
    return spark_opts


def _append_sql_shuffle_partitions_conf(spark_opts: Dict[str, str]) -> Dict[str, str]:
    if 'spark.sql.shuffle.partitions' in spark_opts:
        return spark_opts

    num_partitions = 2 * (
        int(spark_opts.get('spark.cores.max', 0)) or
        int(spark_opts.get('spark.executor.instances', 0)) *
        int(spark_opts.get('spark.executor.cores', DEFAULT_EXECUTOR_CORES))
    )

    if (
        'spark.dynamicAllocation.enabled' in spark_opts and
        str(spark_opts['spark.dynamicAllocation.enabled']) == 'true' and
        'spark.dynamicAllocation.maxExecutors' in spark_opts and
        str(spark_opts['spark.dynamicAllocation.maxExecutors']) != 'infinity'
    ):

        num_partitions_dra = 2 * (
            int(spark_opts.get('spark.dynamicAllocation.maxExecutors', 0)) *
            int(spark_opts.get('spark.executor.cores', DEFAULT_EXECUTOR_CORES))
        )
        num_partitions = max(num_partitions, num_partitions_dra)

    num_partitions = num_partitions or DEFAULT_SQL_SHUFFLE_PARTITIONS

    log.warning(
        f'spark.sql.shuffle.partitions has been set to {num_partitions} '
        'to be equal to twice the number of requested cores, but you should '
        'consider setting a higher value if necessary.'
        ' Follow y/spark for help on partition sizing',
    )

    spark_opts['spark.sql.shuffle.partitions'] = str(num_partitions)
    return spark_opts


def get_dra_configs(spark_opts: Dict[str, str]) -> Dict[str, str]:
    if (
        'spark.dynamicAllocation.enabled' not in spark_opts or
        str(spark_opts['spark.dynamicAllocation.enabled']) != 'true'
    ):
        return spark_opts

    # set defaults if not provided already
    _append_spark_config(spark_opts, 'spark.dynamicAllocation.shuffleTracking.enabled', 'true')
    _append_spark_config(
        spark_opts, 'spark.dynamicAllocation.executorAllocationRatio',
        str(DEFAULT_DRA_EXECUTOR_ALLOCATION_RATIO),
    )
    _append_spark_config(
        spark_opts, 'spark.dynamicAllocation.cachedExecutorIdleTimeout',
        str(DEFAULT_DRA_CACHED_EXECUTOR_IDLE_TIMEOUT),
    )
    log.warning(
        f'\nSetting spark.dynamicAllocation.cachedExecutorIdleTimeout as {DEFAULT_DRA_CACHED_EXECUTOR_IDLE_TIMEOUT}. '
        f'Executor with cached data block will be released if it has been idle for this duration. '
        f'If you wish to change the value of cachedExecutorIdleTimeout, please provide the exact value of '
        f'spark.dynamicAllocation.cachedExecutorIdleTimeout in --spark-args. If your job is performing bad because '
        f'the cached data was lost, please consider increasing this value.\n',
    )

    if 'spark.dynamicAllocation.minExecutors' not in spark_opts:
        # the ratio of total executors to be used as minExecutors
        min_executor_ratio = spark_opts.get('spark.yelp.dra.minExecutorRatio', DEFAULT_DRA_MIN_EXECUTOR_RATIO)
        # set minExecutors default as a ratio of spark.executor.instances
        min_executors = int(int(spark_opts.get('spark.executor.instances', DEFAULT_EXECUTOR_INSTANCES)) *
                            float(min_executor_ratio))
        # minExecutors should not be more than initialExecutors
        if 'spark.dynamicAllocation.initialExecutors' in spark_opts:
            min_executors = min(min_executors, int(spark_opts['spark.dynamicAllocation.initialExecutors']))
        # minExecutors should not be more than maxExecutors
        if 'spark.dynamicAllocation.maxExecutors' in spark_opts:
            min_executors = min(
                min_executors, int(int(spark_opts['spark.dynamicAllocation.maxExecutors']) *
                                   float(min_executor_ratio)),
            )

        spark_opts['spark.dynamicAllocation.minExecutors'] = str(min_executors)
        log.warning(
            f'\nSetting spark.dynamicAllocation.minExecutors as {min_executors}. If you wish to '
            f'change the value of minimum executors, please provide the exact value of '
            f'spark.dynamicAllocation.minExecutors in --spark-args\n',
        )

        if 'spark.yelp.dra.minExecutorRatio' not in spark_opts:
            log.debug(
                f'\nspark.yelp.dra.minExecutorRatio not provided. This specifies the ratio of total executors '
                f'to be used as minimum executors for Dynamic Resource Allocation. More info: y/spark-dra. Using '
                f'default ratio: {DEFAULT_DRA_MIN_EXECUTOR_RATIO}. If you wish to change this value, please provide '
                f'the desired spark.yelp.dra.minExecutorRatio in --spark-args\n',
            )

    if 'spark.dynamicAllocation.maxExecutors' not in spark_opts:
        # set maxExecutors default equal to spark.executor.instances
        max_executors = int(spark_opts.get('spark.executor.instances', DEFAULT_EXECUTOR_INSTANCES))
        # maxExecutors should not be less than initialExecutors
        if 'spark.dynamicAllocation.initialExecutors' in spark_opts:
            max_executors = max(max_executors, int(spark_opts['spark.dynamicAllocation.initialExecutors']))

        spark_opts['spark.dynamicAllocation.maxExecutors'] = str(max_executors)
        log.warning(
            f'\nSetting spark.dynamicAllocation.maxExecutors as {max_executors}. If you wish to '
            f'change the value of maximum executors, please provide the exact value of '
            f'spark.dynamicAllocation.maxExecutors in --spark-args\n',
        )

    spark_opts['spark.executor.instances'] = spark_opts['spark.dynamicAllocation.minExecutors']
    return spark_opts


def _append_event_log_conf(
    spark_opts: Dict[str, str],
    access_key: Optional[str],
    secret_key: Optional[str],
    session_token: Optional[str] = None,
) -> Dict[str, str]:
    enabled = spark_opts.setdefault('spark.eventLog.enabled', 'true').lower()
    if enabled != 'true':
        # user configured to disable log, not continue
        return spark_opts

    event_log_dir = spark_opts.get('spark.eventLog.dir')
    if event_log_dir is not None:
        # we don't want to overwrite user's settings
        return spark_opts

    try:
        with open(DEFAULT_SPARK_RUN_CONFIG) as fp:
            spark_run_conf = yaml.safe_load(fp.read())
    except Exception as e:
        log.warning(f'Failed to load {DEFAULT_SPARK_RUN_CONFIG}: {e}, disable event log')
        return spark_opts

    try:
        account_id = (
            boto3.client(
                'sts',
                aws_access_key_id=access_key,
                aws_secret_access_key=secret_key,
                aws_session_token=session_token,
            )
            .get_caller_identity()
            .get('Account')
        )
    except Exception as e:
        log.warning('Failed to identify account ID, error: {}'.format(str(e)))
        spark_opts['spark.eventLog.enabled'] = 'false'
        return spark_opts

    for conf in spark_run_conf.get('environments', {}).values():
        if account_id == conf['account_id']:
            spark_opts['spark.eventLog.enabled'] = 'true'
            spark_opts['spark.eventLog.dir'] = conf['default_event_log_dir']
            return spark_opts

    log.warning(f'Disable event log because No preset event log dir for account: {account_id}')
    spark_opts['spark.eventLog.enabled'] = 'false'
    return spark_opts


def _append_aws_credentials_conf(
    spark_opts: Dict[str, str],
    access_key: Optional[str],
    secret_key: Optional[str],
    session_token: Optional[str] = None,
) -> Dict[str, str]:
    """It is important that we set the aws creds via the spark configs and not via the AWS
    environment variables.  See HADOOP-18233 for details

    We set both s3a and s3 credentials because s3a is the actual hadoop-aws driver, but our
    glue-metatore integration will attempt to use s3 path drivers which are monkeypatched
    to use the the s3a driver.
    """
    if access_key:
        spark_opts['spark.hadoop.fs.s3a.access.key'] = access_key
        spark_opts['spark.hadoop.fs.s3.access.key'] = access_key
    if secret_key:
        spark_opts['spark.hadoop.fs.s3a.secret.key'] = secret_key
        spark_opts['spark.hadoop.fs.s3.secret.key'] = secret_key
    if session_token:
        spark_opts['spark.hadoop.fs.s3a.session.token'] = session_token
        spark_opts['spark.hadoop.fs.s3.session.token'] = session_token
    return spark_opts


def _adjust_spark_requested_resources(
    user_spark_opts: Dict[str, str],
    cluster_manager: str,
    pool: str,
) -> Dict[str, str]:
    executor_memory = user_spark_opts.setdefault('spark.executor.memory', DEFAULT_EXECUTOR_MEMORY)
    executor_cores = int(user_spark_opts.setdefault('spark.executor.cores', str(DEFAULT_EXECUTOR_CORES)))
    if cluster_manager == 'mesos':
        max_cores = int(user_spark_opts.setdefault('spark.cores.max', str(DEFAULT_MAX_CORES)))
        executor_instances = max_cores / executor_cores
    elif cluster_manager == 'kubernetes':
        # TODO(gcoll|COREML-2697): Consider cleaning this part of the code up
        # once mesos is not longer around at Yelp.
        if 'spark.executor.instances' not in user_spark_opts:

            executor_instances = int(user_spark_opts.get(
                'spark.cores.max',
                str(DEFAULT_MAX_CORES),
            )) // executor_cores
            user_spark_opts['spark.executor.instances'] = str(executor_instances)

            if user_spark_opts['spark.executor.instances'] == str(DEFAULT_EXECUTOR_INSTANCES):
                log.warning(
                    f'spark.executor.instances not provided. Setting spark.executor.instances as '
                    f'{executor_instances}. If you wish to change the number of executors, please '
                    f'provide the exact value of spark.executor.instances in --spark-args',
                )

        if (
            'spark.mesos.executor.memoryOverhead' in user_spark_opts and
            'spark.executor.memoryOverhead' not in user_spark_opts
        ):
            user_spark_opts['spark.executor.memoryOverhead'] = user_spark_opts['spark.mesos.executor.memoryOverhead']

        executor_instances = int(
            user_spark_opts.setdefault('spark.executor.instances', str(DEFAULT_EXECUTOR_INSTANCES)),
        )
        max_cores = executor_instances * executor_cores
        user_spark_opts.setdefault(
            'spark.kubernetes.allocation.batch.size',
            str(DEFAULT_K8S_BATCH_SIZE),
        )
        user_spark_opts.setdefault('spark.kubernetes.executor.limit.cores', str(executor_cores))
        waiting_time = (
            DEFAULT_CLUSTERMAN_OBSERVED_SCALING_TIME +
            executor_instances * DEFAULT_RESOURCES_WAITING_TIME_PER_EXECUTOR // 60
        )
        user_spark_opts.setdefault(
            'spark.scheduler.maxRegisteredResourcesWaitingTime',
            str(waiting_time) + 'min',
        )
    elif cluster_manager == 'local':
        executor_instances = int(
            user_spark_opts.setdefault('spark.executor.instances', str(DEFAULT_EXECUTOR_INSTANCES)),
        )
        max_cores = executor_instances * executor_cores
    else:
        raise UnsupportedClusterManagerException(cluster_manager)

    if max_cores < executor_cores:
        raise ValueError(f'Total number of cores {max_cores} is less than per-executor cores {executor_cores}')

    memory = parse_memory_string(executor_memory)
    if memory > 32 * 1024:
        log.warning('Executor memory is {memory / 32}g, greater than recommended value: 32g')

    num_gpus = int(user_spark_opts.get('spark.mesos.gpus.max', '0'))
    task_cpus = int(user_spark_opts.get('spark.task.cpus', '1'))
    # we can skip this step if user is not using gpu or do not configure
    # task cpus and executor cores
    if num_gpus == 0 or (task_cpus != 1 and executor_cores != 1):
        return user_spark_opts

    if num_gpus != 0 and cluster_manager != 'mesos':
        raise ValueError('spark.mesos.gpus.max is only available for mesos')

    if num_gpus > GPUS_HARD_LIMIT:
        raise ValueError(
            'Requested {num_gpus} GPUs, which exceeds hard limit of {GPUS_HARD_LIMIT}',
        )
    gpus_per_inst = 0
    cpus_per_inst = 0

    with open(GPU_POOLS_YAML_FILE_PATH) as fp:
        pool_def = yaml.safe_load(fp).get(pool)

    if pool_def is not None:
        gpus_per_inst = int(pool_def['gpus_per_instance'])
        cpus_per_inst = int(pool_def['cpus_per_instance'])
        if gpus_per_inst == 0 or cpus_per_inst == 0:
            raise ValueError(
                'Unable to adjust spark.task.cpus and spark.executor.cores because '
                f'pool {pool} does not appear to have any GPUs and/or CPUs',
            )
    else:
        raise ValueError(
            'Unable to adjust spark.task.cpus and spark.executor.cores because '
            f"pool \"{pool}\" not found in gpu_pools",
        )

    instances = num_gpus // gpus_per_inst
    if (instances * gpus_per_inst) != num_gpus:
        raise ValueError(
            'Unable to adjust spark.task.cpus and spark.executor.cores because '
            'spark.mesos.gpus.max=%i is not a multiple of %i'
            % (num_gpus, gpus_per_inst),
        )

    cpus_per_gpu = cpus_per_inst // gpus_per_inst
    total_cpus = cpus_per_gpu * num_gpus
    num_cpus = (
        int(max_cores) if cluster_manager == 'mesos'
        else int(executor_instances) * int(executor_cores)
    )
    if num_cpus != total_cpus:
        log.warning(
            f'spark.cores.max has been adjusted to {total_cpus}. '
            'See y/horovod for sizing of GPU pools.',
        )

    user_spark_opts.update({
        # Mesos limitation - need this to access GPUs
        'spark.mesos.containerizer': 'mesos',
        # For use by horovod.spark.run(...) in place of num_proc
        'spark.default.parallelism': str(num_gpus),
        # we need to adjust the requirements to meet the gpus requriements
        'spark.task.cpus': str(cpus_per_gpu),
        'spark.executor.cores': str(cpus_per_gpu * gpus_per_inst),
        'spark.cores.max': str(total_cpus),
    })
    return user_spark_opts


def find_spark_master(paasta_cluster):
    """Finds the Mesos leader of a PaaSTA cluster.

    :param str paasta_cluster: Name of a PaaSTA cluster.
    :return str: The Mesos cluster manager to connect to. Callers are expected to check result.
    """
    try:
        response = requests.get(f'http://paasta-{paasta_cluster}.yelp:5050/redirect')
    except requests.RequestException:
        raise ValueError(f'Cannot find spark master for cluster {paasta_cluster}')
    return f'mesos://{urlparse(response.url).hostname}:5050'


def _get_mesos_spark_env(
    user_spark_opts: Mapping[str, Any],
    paasta_cluster: str,
    paasta_pool: str,
    paasta_service: str,
    paasta_instance: str,
    docker_img: str,
    extra_volumes: Optional[List[Mapping[str, str]]],
    extra_docker_params: Optional[Mapping[str, str]],
    with_secret: bool,
    needs_docker_cfg: bool,
    mesos_leader: Optional[str],
    load_paasta_default_volumes: bool,
) -> Dict[str, str]:

    if mesos_leader is None:
        mesos_leader = find_spark_master(paasta_cluster)
    else:
        mesos_leader = f'mesos://{mesos_leader}'

    docker_parameters = [
        # Limit a container's cpu usage
        f"cpus={user_spark_opts['spark.executor.cores']}",
        f'label=paasta_service={paasta_service}',
        f'label=paasta_instance={paasta_instance}',
    ]
    if extra_docker_params:
        docker_parameters.extend(f'{key}={value}' for key, value in extra_docker_params.items())

    auth_configs = {}
    if with_secret:
        try:
            with open(DEFAULT_SPARK_MESOS_SECRET_FILE, 'r') as f:
                secret = f.read()
        except IOError as e:
            log.error(
                'Cannot load mesos secret from %s' % DEFAULT_SPARK_MESOS_SECRET_FILE,
            )
            raise ValueError(str(e))
        auth_configs = {'spark.mesos.secret': secret}

    spark_env = {
        'spark.master': mesos_leader,
        'spark.executorEnv.PAASTA_SERVICE': paasta_service,
        'spark.executorEnv.PAASTA_INSTANCE': paasta_instance,
        'spark.executorEnv.PAASTA_CLUSTER': paasta_cluster,
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.SPARK_USER': 'root',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        'spark.mesos.executor.docker.parameters': ','.join(docker_parameters),
        'spark.mesos.executor.docker.image': docker_img,
        'spark.mesos.constraints': f'pool:{paasta_pool}',
        'spark.mesos.executor.docker.forcePullImage': 'true',
        'spark.mesos.principal': 'spark',
        **auth_configs,
        **_get_mesos_docker_volumes_conf(
            user_spark_opts, extra_volumes,
            load_paasta_default_volumes,
        ),
    }
    if needs_docker_cfg:
        spark_env['spark.mesos.uris'] = 'file:///root/.dockercfg'

    return spark_env


def _get_k8s_spark_env(
    paasta_cluster: str,
    paasta_service: str,
    paasta_instance: str,
    docker_img: str,
    volumes: Optional[List[Mapping[str, str]]],
    paasta_pool: str,
) -> Dict[str, str]:
    # RFC 1123: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
    # technically only paasta instance can be longer than 63 chars. But we apply the normalization regardless.
    # NOTE: this affects only k8s labels, not the pod names.
    _paasta_cluster = _get_k8s_resource_name_limit_size_with_hash(paasta_cluster)
    _paasta_service = _get_k8s_resource_name_limit_size_with_hash(paasta_service)
    _paasta_instance = _get_k8s_resource_name_limit_size_with_hash(paasta_instance)
    spark_env = {
        'spark.master': f'k8s://https://k8s.{paasta_cluster}.paasta:6443',
        'spark.executorEnv.PAASTA_SERVICE': paasta_service,
        'spark.executorEnv.PAASTA_INSTANCE': paasta_instance,
        'spark.executorEnv.PAASTA_CLUSTER': paasta_cluster,
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        'spark.kubernetes.pyspark.pythonVersion': '3',
        'spark.kubernetes.container.image': docker_img,
        'spark.kubernetes.namespace': 'paasta-spark',
        'spark.kubernetes.authenticate.caCertFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-ca.crt',
        'spark.kubernetes.authenticate.clientKeyFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-client.key',
        'spark.kubernetes.authenticate.clientCertFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-client.crt',
        'spark.kubernetes.container.image.pullPolicy': 'Always',
        'spark.kubernetes.executor.label.yelp.com/paasta_service': _paasta_service,
        'spark.kubernetes.executor.label.yelp.com/paasta_instance': _paasta_instance,
        'spark.kubernetes.executor.label.yelp.com/paasta_cluster': _paasta_cluster,
        'spark.kubernetes.executor.label.paasta.yelp.com/service': _paasta_service,
        'spark.kubernetes.executor.label.paasta.yelp.com/instance': _paasta_instance,
        'spark.kubernetes.executor.label.paasta.yelp.com/cluster': _paasta_cluster,
        'spark.kubernetes.node.selector.yelp.com/pool': paasta_pool,
        'spark.kubernetes.executor.label.yelp.com/pool': paasta_pool,
        **_get_k8s_docker_volumes_conf(volumes),
    }
    return spark_env


def _get_local_spark_env(
    paasta_cluster: str,
    paasta_service: str,
    paasta_instance: str,
    volumes: Optional[List[Mapping[str, str]]],
    num_threads: int = 4,
) -> Dict[str, str]:
    return {
        'spark.master': f'local[{num_threads}]',
        'spark.executorEnv.PAASTA_SERVICE': paasta_service,
        'spark.executorEnv.PAASTA_INSTANCE': paasta_instance,
        'spark.executorEnv.PAASTA_CLUSTER': paasta_cluster,
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        # Adding k8s docker volume params is a bit of a hack.  PaasSTA
        # looks at the spark config to find the volumes it needs to mount
        # and so we are borrowing the spark k8s configs.
        **_get_k8s_docker_volumes_conf(volumes),
    }


def _get_k8s_resource_name_limit_size_with_hash(name: str, limit: int = 63, suffix: int = 4) -> str:
    """ Returns `name` unchanged if it's length does not exceed the `limit`.
        Otherwise, returns truncated `name` with it's hash of size `suffix`
        appended.

        base32 encoding is chosen as it satisfies the common requirement in
        various k8s names to be alphanumeric.

        NOTE: This function is the same as paasta/paasta_tools/kubernetes_tools.py
    """
    if len(name) > limit:
        digest = hashlib.md5(name.encode()).digest()
        hash = base64.b32encode(digest).decode().replace('=', '').lower()
        return f'{name[:(limit-suffix-1)]}-{hash[:suffix]}'
    else:
        return name


def stringify_spark_env(spark_env: Mapping[str, str]) -> str:
    return ' '.join([f'--conf {k}={v}' for k, v in spark_env.items()])


def _filter_user_spark_opts(user_spark_opts: Mapping[str, str]) -> MutableMapping[str, str]:
    non_configurable_opts = set(user_spark_opts.keys()) & set(NON_CONFIGURABLE_SPARK_OPTS)
    if non_configurable_opts:
        log.warning(f'The following options are configured by Paasta: {non_configurable_opts} instead')
    return {
        key: value
        for key, value in user_spark_opts.items()
        if key not in NON_CONFIGURABLE_SPARK_OPTS
    }


def _convert_user_spark_opts_value_to_str(user_spark_opts: Mapping[str, Any]) -> Dict[str, str]:
    output = {}
    for key, val in user_spark_opts.items():
        # it's likely the yaml config would convert true/false as bool instead
        # of str, therefore verify this is configured correctly
        if isinstance(val, bool):
            output[key] = str(val).lower()
        elif not isinstance(val, str):
            output[key] = str(val)
            log.warning(
                f"user_spark_opts[{key}]={val} is not in str type, will convert it to '{str(val)}'",
            )
        else:
            output[key] = val
    return output


def get_spark_conf(
    cluster_manager: str,
    spark_app_base_name: str,
    user_spark_opts: Mapping[str, Any],
    paasta_cluster: str,
    paasta_pool: str,
    paasta_service: str,
    paasta_instance: str,
    docker_img: str,
    aws_creds: Tuple[Optional[str], Optional[str], Optional[str]],
    extra_volumes: List[Mapping[str, str]] = None,
    # the follow arguments only being used for mesos
    extra_docker_params: Optional[MutableMapping[str, str]] = None,
    with_secret: bool = True,
    needs_docker_cfg: bool = False,
    mesos_leader: Optional[str] = None,
    spark_opts_from_env: Optional[Mapping[str, str]] = None,
    load_paasta_default_volumes: bool = False,
    auto_set_temporary_credentials_provider: bool = True,
) -> Dict[str, str]:
    """Build spark config dict to run with spark on paasta

    :param cluster_manager: which manager to use, must be in SUPPORTED_CLUSTER_MANAGERS
    :param spark_app_base_name: the base name to create spark app, we will append port
        and time to make the app name unique for easier to separate the output. Note that
        this is noop if `spark_opts_from_env` have `spark.app.name` configured.
    :param user_spark_opts: user specified spark config. We will filter out some configs
        that is not supposed to be configured by users before adding these changes.
    :param paasta_cluster: the cluster name to run the spark job
    :param paasta_pool: the pool name to launch the spark job.
    :param paasta_service: the service name of the job
    :param paasta_instance: the instance name of the job
    :param docker_img: the docker image used to launch container for spark executor.
    :param aws_creds: the aws creds to be used for this spark job. If a key triplet is passed,
        we configure a different credentials provider to support this workflow.
    :param extra_volumes: extra files to mount on the spark executors
    :param extra_docker_params: extra docker parameters to launch the spark executor
        container. This is only being used when `cluster_manager` is set to `mesos`
    :param with_secret: whether the output spark config should include mesos secrets.
        This is only being used when `cluster_manager` is set to `mesos`
    :param needs_docker_cfg: whether we should add docker.cfg file for accessing
        docker registry. This is only being used when `cluster_manager` is set to `mesos`
    :param mesos_leader: the mesos leader to be used. leave empty to use the default
        pnw-devc master. This is only being used when `cluster_manager` is set to `mesos`
    :param spark_opts_from_env: different from user_spark_opts, configuration in this
        dict will not be filtered. This options is left for people who use `paasta spark-run`
        to launch the batch, and inside the batch use `spark_tools.paasta` to create
        spark session.
    :param load_paasta_default_volumes: whether to include default paasta mounted volumes
        into the spark executors.
    :param auto_set_temporary_credentials_provider: whether to set the temporary credentials
        provider if the session token exists.  In hadoop-aws 3.2.1 this is needed, but
        in hadoop-aws 3.3.1 the temporary credentials provider is the new default and
        causes errors if explicitly set for unknown reasons.
    :returns: spark opts in a dict.
    """
    # for simplicity, all the following computation are assuming spark opts values
    # is str type.
    user_spark_opts = _convert_user_spark_opts_value_to_str(user_spark_opts)

    app_base_name = (
        user_spark_opts.get('spark.app.name') or
        spark_app_base_name
    )

    ui_port = (spark_opts_from_env or {}).get('spark.ui.port') or _pick_random_port(
        app_base_name + str(time.time()),
    )

    # app_name from env is already appended port and time to make it unique
    app_name = (spark_opts_from_env or {}).get('spark.app.name')
    if not app_name:
        # We want to make the app name more unique so that we can search it
        # from history server.
        app_name = f'{app_base_name}_{ui_port}_{int(time.time())}'

    spark_conf = {**(spark_opts_from_env or {}), **_filter_user_spark_opts(user_spark_opts)}

    if aws_creds[2] is not None and auto_set_temporary_credentials_provider:
        spark_conf['spark.hadoop.fs.s3a.aws.credentials.provider'] = AWS_TEMP_CREDENTIALS_PROVIDER

    spark_conf.update({
        'spark.app.name': app_name,
        'spark.ui.port': str(ui_port),
    })

    # adjusted with spark default resources
    spark_conf = _adjust_spark_requested_resources(spark_conf, cluster_manager, paasta_pool)

    if cluster_manager == 'mesos':
        spark_conf.update(_get_mesos_spark_env(
            spark_conf,
            paasta_cluster,
            paasta_pool,
            paasta_service,
            paasta_instance,
            docker_img,
            extra_volumes,
            extra_docker_params,
            with_secret,
            needs_docker_cfg,
            mesos_leader,
            load_paasta_default_volumes,
        ))
    elif cluster_manager == 'kubernetes':
        spark_conf.update(_get_k8s_spark_env(
            paasta_cluster,
            paasta_service,
            paasta_instance,
            docker_img,
            extra_volumes,
            paasta_pool,
        ))
    elif cluster_manager == 'local':
        spark_conf.update(_get_local_spark_env(
            paasta_cluster,
            paasta_service,
            paasta_instance,
            extra_volumes,
        ))
    else:
        raise UnsupportedClusterManagerException(cluster_manager)

    # configure dynamic resource allocation configs
    spark_conf = get_dra_configs(spark_conf)

    # configure spark_event_log
    spark_conf = _append_event_log_conf(spark_conf, *aws_creds)

    # configure sql shuffle partitions
    spark_conf = _append_sql_shuffle_partitions_conf(spark_conf)

    # configure spark conf log
    spark_conf = _append_spark_config(spark_conf, 'spark.logConf', 'true')

    # configure spark Console Progress
    spark_conf = _append_spark_config(spark_conf, 'spark.ui.showConsoleProgress', 'true')

    spark_conf = _append_aws_credentials_conf(spark_conf, *aws_creds)
    return spark_conf


def parse_memory_string(memory_string: Optional[str]) -> int:
    if memory_string is None:
        return 0
    else:
        return (
            int(memory_string[:-1]) * 1024 if memory_string.endswith('g')
            else int(memory_string)
        )


def compute_requested_memory_overhead(spark_opts: Mapping[str, str], executor_memory):
    return max(
        parse_memory_string(spark_opts.get('spark.executor.memoryOverhead')),
        parse_memory_string(spark_opts.get('spark.mesos.executor.memoryOverhead')),
        float(spark_opts.get('spark.kubernetes.memoryOverheadFactor', 0)) * executor_memory,
    )


def get_signalfx_url(spark_conf: Mapping[str, str]) -> str:
    return (
        'https://app.signalfx.com/#/dashboard/DJzYJDkAcAA?density=4'
        '&variables%5B%5D=Instance%3Dinstance_name:'
        '&variables%5B%5D=Service%3Dservice_name:%5B%22spark%22%5D'
        f"&variables%5B%5D=PaaSTA%20Cluster%3Dpaasta_cluster:{spark_conf['spark.executorEnv.PAASTA_CLUSTER']}"
        f"&variables%5B%5D=PaaSTA%20Service%3Dpaasta_service:{spark_conf['spark.executorEnv.PAASTA_SERVICE']}"
        f"&variables%5B%5D=PaaSTA%20Instance%3Dpaasta_instance:{spark_conf['spark.executorEnv.PAASTA_INSTANCE']}"
        '&startTime=-6h&endTime=Now'
    )


def get_history_url(spark_conf: Mapping[str, str]) -> Optional[str]:
    if spark_conf.get('spark.eventLog.enabled') != 'true':
        return None
    event_log_dir = spark_conf.get('spark.eventLog.dir')
    with open(DEFAULT_SPARK_RUN_CONFIG) as fp:
        spark_run_conf = yaml.safe_load(fp.read())
    for env, env_conf in spark_run_conf['environments'].items():
        if event_log_dir == env_conf['default_event_log_dir']:
            return env_conf['history_server']
    return None


def get_resources_requested(spark_opts: Mapping[str, str]) -> Mapping[str, int]:
    num_executors = (
        # spark on k8s directly configure num instances
        int(spark_opts.get('spark.executor.instances', 0)) or
        # spark on mesos use cores.max and executor.core to calculate number of
        # executors.
        int(spark_opts.get('spark.cores.max', 0)) // int(spark_opts.get('spark.executor.cores', 0))
    )
    num_cpus = (
        # spark on k8s
        int(spark_opts.get('spark.executor.instances', 0)) * int(spark_opts.get('spark.executor.cores', 0)) or
        # spark on mesos
        int(spark_opts.get('spark.cores.max', 0))
    )
    num_gpus = int(spark_opts.get('spark.mesos.gpus.max', 0))

    executor_memory = parse_memory_string(spark_opts.get('spark.executor.memory', ''))
    requested_memory = compute_requested_memory_overhead(spark_opts, executor_memory)
    # by default, spark adds an overhead of 10% of the executor memory, with a
    # minimum of 384mb
    memory_overhead: int = (
        requested_memory
        if requested_memory > 0
        else max(384, int(0.1 * executor_memory))
    )
    total_memory = (executor_memory + memory_overhead) * num_executors
    log.info(f'Requested total memory of {total_memory} MiB')
    return {
        'cpus': num_cpus,
        'mem': total_memory,
        # rough guess since spark does not collect this information
        'disk': total_memory,
        'gpus': num_gpus,
    }


def generate_clusterman_metrics_entries(
    clusterman_metrics,
    resources: Mapping[str, int],
    app_name: str,
    spark_web_url: str,
) -> Dict[str, int]:
    """convert the requested resources to clusterman key. Note that the clusterman
    is not availble externally. You will need to initialize the clusterman_metrics
    with
    .. code-block:: python

    import clusterman_metrics
    import srv_configs
    srv_configs.use_file(
        CLUSTERMAN_METRICS_YAML_FILE_PATH, namespace="clusterman_metrics"
    )

    :param clusterman_metrics: the clusterman_metrics module
    :param resources: the requested resources dict
    :param app_name: the spark job app name
    :param spark_web_url: the monitor url for the spark job.
    """
    dimensions = {
        'framework_name': app_name,
        'webui_url': spark_web_url,
    }
    return {
        clusterman_metrics.generate_key_with_dimensions(f'requested_{resource_key}', dimensions):
        required_quantity
        for resource_key, required_quantity in resources.items()
    }


def _emit_resource_requirements(
    clusterman_metrics,
    resources: Mapping[str, int],
    app_name: str,
    spark_web_url: str,
    cluster: str,
    pool: str,
) -> None:

    with open(CLUSTERMAN_YAML_FILE_PATH, 'r') as clusterman_yaml_file:
        clusterman_yaml = yaml.safe_load(clusterman_yaml_file.read())
    aws_region = clusterman_yaml['clusters'][cluster]['aws_region']

    client = clusterman_metrics.ClustermanMetricsBotoClient(
        region_name=aws_region, app_identifier=pool,
    )
    metrics_entries = generate_clusterman_metrics_entries(
        clusterman_metrics,
        resources,
        app_name,
        spark_web_url,
    )
    with client.get_writer(
        clusterman_metrics.APP_METRICS, aggregate_meteorite_dims=True,
    ) as writer:
        for metric_key, required_quantity in metrics_entries.items():
            writer.send((metric_key, int(time.time()), required_quantity))


def get_spark_hourly_cost(
    clusterman_metrics,
    resources: Mapping[str, int],
    cluster: str,
    pool: str,
) -> float:
    cpus = resources['cpus']
    mem = resources['mem']
    return clusterman_metrics.util.costs.estimate_cost_per_hour(
        cluster=cluster, pool=pool, cpus=cpus, mem=mem,
    )


def send_and_calculate_resources_cost(
    clusterman_metrics,
    spark_conf: Mapping[str, str],
    spark_web_url: str,
    pool: str,
) -> Tuple[float, Mapping[str, int]]:
    """Calculated the cost and send the requested resources to clusterman.
    return the resources used and the hourly cost for the job.

    Note that the clusterman is not availble externally. You will need to initialize
    the clusterman_metrics with
    .. code-block:: python

    import clusterman_metrics
    import srv_configs
    srv_configs.use_file(
        CLUSTERMAN_METRICS_YAML_FILE_PATH, namespace="clusterman_metrics"
    )

    :param clusterman_metrics: the clusterman_metrics module
    :param spark_conf: the spark configuration so that we can calculate the
        requested resources
    :param spark_web_url: the spark monitor url.
    :param pool: the pool name to launch the spark job.
    :returns: Tuple, the first element is the hourly cost and the second element
        is the requested resources.
    """
    cluster = spark_conf['spark.executorEnv.PAASTA_CLUSTER']
    app_name = spark_conf['spark.app.name']
    resources = get_resources_requested(spark_conf)
    hourly_cost = get_spark_hourly_cost(clusterman_metrics, resources, cluster, pool)
    _emit_resource_requirements(
        clusterman_metrics, resources, app_name, spark_web_url, cluster, pool,
    )
    return hourly_cost, resources
