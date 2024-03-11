import functools
import itertools
import json
import logging
import math
import os
import re
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
import requests
import yaml
from boto3 import Session

from service_configuration_lib import utils
from service_configuration_lib.text_colors import TextColors

AWS_CREDENTIALS_DIR = '/etc/boto_cfg/'
AWS_ENV_CREDENTIALS_PROVIDER = 'com.amazonaws.auth.EnvironmentVariableCredentialsProvider'
GPU_POOLS_YAML_FILE_PATH = '/nail/srv/configs/gpu_pools.yaml'
DEFAULT_PAASTA_VOLUME_PATH = '/etc/paasta/volumes.json'
DEFAULT_SPARK_MESOS_SECRET_FILE = '/nail/etc/paasta_spark_secret'
DEFAULT_SPARK_SERVICE = 'spark'
GPUS_HARD_LIMIT = 15
CLUSTERMAN_METRICS_YAML_FILE_PATH = '/nail/srv/configs/clusterman_metrics.yaml'
CLUSTERMAN_YAML_FILE_PATH = '/nail/srv/configs/clusterman.yaml'

NON_CONFIGURABLE_SPARK_OPTS = {
    'spark.master',
    'spark.app.id',
    'spark.ui.port',
    'spark.mesos.principal',
    'spark.mesos.secret',
    'spark.mesos.executor.docker.image',
    'spark.mesos.executor.docker.parameters',
    'spark.executorEnv.PAASTA_SERVICE',
    'spark.executorEnv.PAASTA_INSTANCE',
    'spark.executorEnv.PAASTA_CLUSTER',
    'spark.executorEnv.SPARK_EXECUTOR_DIRS',
    'spark.executorEnv.AWS_ACCESS_KEY_ID',
    'spark.executorEnv.AWS_SECRET_ACCESS_KEY',
    'spark.executorEnv.AWS_SESSION_TOKEN',
    'spark.executorEnv.AWS_DEFAULT_REGION',
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
    'spark.kubernetes.executor.label.spark.yelp.com/user',
}

K8S_AUTH_FOLDER = '/etc/pki/spark'
K8S_BASE_VOLUMES: List[Dict[str, str]] = [
    {'containerPath': K8S_AUTH_FOLDER, 'hostPath': K8S_AUTH_FOLDER, 'mode': 'RO'},
    {'containerPath': '/etc/passwd', 'hostPath': '/etc/passwd', 'mode': 'RO'},
    {'containerPath': '/etc/group', 'hostPath': '/etc/group', 'mode': 'RO'},
]

SUPPORTED_CLUSTER_MANAGERS = ['kubernetes', 'local']
DEFAULT_SPARK_RUN_CONFIG = '/nail/srv/configs/spark.yaml'

log = logging.Logger(__name__)
log.setLevel(logging.INFO)


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
    aws_credentials_yaml: Optional[str] = None,
    profile_name: Optional[str] = None,
    session: Optional[boto3.Session] = None,
    aws_credentials_json: Optional[str] = None,
    assume_aws_role_arn: Optional[str] = None,
    session_duration: int = 3600,
    assume_role_user_creds_file: str = '/nail/etc/spark_role_assumer/spark_role_assumer.yaml',
) -> Tuple[Optional[str], Optional[str], Optional[str]]:
    """load aws creds using different method/file"""
    if aws_credentials_yaml:
        return _load_aws_credentials_from_yaml(aws_credentials_yaml)
    elif aws_credentials_json:
        with open(aws_credentials_json, 'r') as f:
            creds = json.load(f)
        return creds.get('accessKeyId'), creds.get('secretAccessKey'), None
    elif assume_aws_role_arn:
        credentials = assume_aws_role(assume_aws_role_arn, session_duration, assume_role_user_creds_file)
        if credentials:
            return credentials['AccessKeyId'], credentials['SecretAccessKey'], credentials['SessionToken']
        else:
            log.warning(
                'Tried to assume role with web identity but something went wrong ',
            )
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


def assume_aws_role(
    role_arn: str,
    session_duration: int,
    key_file: str,
) -> Dict[str, str]:
    """
    Checks that a web identity token is available, and if it is,
    get an aws session and return a credentials dictionary
    """
    try:
        with open(key_file) as creds_file:
            creds_dict = yaml.load(creds_file.read(), Loader=yaml.SafeLoader)
            access_key = creds_dict['AccessKeyId']
            secret_key = creds_dict['SecretAccessKey']
    except PermissionError:
        log.warning(
            'If using spark-run as a human, you must manually export '
            'AWS session credentials first. See y/spark-run-aws-role',
        )
        raise
    timestamp = int(time.time())
    client = boto3.client('sts', aws_access_key_id=access_key, aws_secret_access_key=secret_key)
    resp = client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=f'SparkRun-{timestamp}',
        DurationSeconds=session_duration,
    )
    return resp['Credentials']


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


def _is_jupyterhub_job(spark_app_name: str) -> bool:
    # TODO: add regex to better match Jupyterhub Spark session app name
    return 'jupyterhub' in spark_app_name


def _append_aws_credentials_conf(
    spark_opts: Dict[str, str],
    access_key: Optional[str],
    secret_key: Optional[str],
    session_token: Optional[str] = None,
    aws_region: Optional[str] = None,
) -> Dict[str, str]:
    if access_key:
        spark_opts['spark.executorEnv.AWS_ACCESS_KEY_ID'] = access_key
    if secret_key:
        spark_opts['spark.executorEnv.AWS_SECRET_ACCESS_KEY'] = secret_key
    if session_token:
        spark_opts['spark.executorEnv.AWS_SESSION_TOKEN'] = session_token
    if aws_region:
        spark_opts['spark.executorEnv.AWS_DEFAULT_REGION'] = aws_region
    return spark_opts


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


def _get_k8s_spark_env(
    paasta_cluster: str,
    paasta_service: str,
    paasta_instance: str,
    docker_img: str,
    pod_template_path: str,
    volumes: Optional[List[Mapping[str, str]]],
    paasta_pool: str,
    driver_ui_port: int,
    service_account_name: Optional[str] = None,
    include_self_managed_configs: bool = True,
    k8s_server_address: Optional[str] = None,
    user: Optional[str] = None,
) -> Dict[str, str]:
    # RFC 1123: https://kubernetes.io/docs/concepts/overview/working-with-objects/names/#dns-label-names
    # technically only paasta instance can be longer than 63 chars. But we apply the normalization regardless.
    # NOTE: this affects only k8s labels, not the pod names.
    _paasta_cluster = utils.get_k8s_resource_name_limit_size_with_hash(paasta_cluster)
    _paasta_service = utils.get_k8s_resource_name_limit_size_with_hash(paasta_service)
    _paasta_instance = utils.get_k8s_resource_name_limit_size_with_hash(paasta_instance)
    if not user:
        user = os.environ.get('USER', 'UNSPECIFIED')

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
        'spark.kubernetes.executor.label.yelp.com/paasta_service': _paasta_service,
        'spark.kubernetes.executor.label.yelp.com/paasta_instance': _paasta_instance,
        'spark.kubernetes.executor.label.yelp.com/paasta_cluster': _paasta_cluster,
        'spark.kubernetes.executor.label.paasta.yelp.com/service': _paasta_service,
        'spark.kubernetes.executor.label.paasta.yelp.com/instance': _paasta_instance,
        'spark.kubernetes.executor.label.paasta.yelp.com/cluster': _paasta_cluster,
        'spark.kubernetes.executor.label.spark.yelp.com/user': user,
        'spark.kubernetes.executor.label.spark.yelp.com/driver_ui_port': str(driver_ui_port),
        'spark.kubernetes.node.selector.yelp.com/pool': paasta_pool,
        'spark.kubernetes.executor.label.yelp.com/pool': paasta_pool,
        'spark.kubernetes.executor.label.paasta.yelp.com/pool': paasta_pool,
        'spark.kubernetes.executor.label.yelp.com/owner': 'core_ml',
        'spark.kubernetes.executor.podTemplateFile': pod_template_path,
        **_get_k8s_docker_volumes_conf(volumes),
    }
    if service_account_name is not None:
        spark_env.update(
            {
                'spark.kubernetes.authenticate.serviceAccountName': service_account_name,
            },
        )
    elif not include_self_managed_configs:
        spark_env.update({
            'spark.master': f'k8s://{k8s_server_address}',
        })
    elif include_self_managed_configs:
        spark_env.update(
            {
                'spark.kubernetes.authenticate.caCertFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-ca.crt',
                'spark.kubernetes.authenticate.clientKeyFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-client.key',
                'spark.kubernetes.authenticate.clientCertFile': f'{K8S_AUTH_FOLDER}/{paasta_cluster}-client.crt',
            },
        )

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


def stringify_spark_env(spark_env: Mapping[str, str]) -> str:
    return ' '.join([f'--conf {k}={v}' for k, v in spark_env.items()])


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


def _filter_user_spark_opts(user_spark_opts: Mapping[str, str]) -> MutableMapping[str, str]:
    non_configurable_opts = set(user_spark_opts.keys()) & set(NON_CONFIGURABLE_SPARK_OPTS)
    if non_configurable_opts:
        log.warning(f'The following options are configured by Paasta: {non_configurable_opts} instead')
    return {
        key: value
        for key, value in user_spark_opts.items()
        if key not in NON_CONFIGURABLE_SPARK_OPTS
    }


class SparkConfBuilder:

    def __init__(self):
        self.spark_srv_conf = dict()
        self.spark_constants = dict()
        self.default_spark_srv_conf = dict()
        self.mandatory_default_spark_srv_conf = dict()
        self.spark_costs = dict()

        try:
            (
                self.spark_srv_conf, self.spark_constants, self.default_spark_srv_conf,
                self.mandatory_default_spark_srv_conf, self.spark_costs,
            ) = utils.load_spark_srv_conf()
        except Exception as e:
            log.error(f'Failed to load Spark srv configs: {e}')
            # should fail because Spark config calculation depends on values in srv-configs
            raise e

    def _append_spark_prometheus_conf(self, spark_opts: Dict[str, str]) -> Dict[str, str]:
        spark_opts['spark.ui.prometheus.enabled'] = 'true'
        spark_opts[
            'spark.metrics.conf.*.sink.prometheusServlet.class'
        ] = 'org.apache.spark.metrics.sink.PrometheusServlet'
        spark_opts['spark.metrics.conf.*.sink.prometheusServlet.path'] = '/metrics/prometheus'
        return spark_opts

    def get_dra_configs(self, spark_opts: Dict[str, str]) -> Dict[str, str]:
        # don't enable DRA if it is explicitly disabled
        if (
                'spark.dynamicAllocation.enabled' in spark_opts and
                str(spark_opts['spark.dynamicAllocation.enabled']) != 'true'
        ):
            return spark_opts

        spark_app_name = spark_opts.get('spark.app.name', '')

        log.warning(
            TextColors.yellow(
                '\nSpark Dynamic Resource Allocation (DRA) enabled for this batch. More info: y/spark-dra.\n',
            ),
        )

        # set defaults if not provided already
        _append_spark_config(spark_opts, 'spark.dynamicAllocation.enabled', 'true')
        _append_spark_config(spark_opts, 'spark.dynamicAllocation.shuffleTracking.enabled', 'true')
        _append_spark_config(
            spark_opts, 'spark.dynamicAllocation.executorAllocationRatio',
            str(self.default_spark_srv_conf['spark.dynamicAllocation.executorAllocationRatio']),
        )
        cached_executor_idle_timeout = self.default_spark_srv_conf['spark.dynamicAllocation.cachedExecutorIdleTimeout']
        if 'spark.dynamicAllocation.cachedExecutorIdleTimeout' not in spark_opts:
            if _is_jupyterhub_job(spark_app_name):
                # increase cachedExecutorIdleTimeout by 15 minutes in case of Jupyterhub
                cached_executor_idle_timeout = str(int(cached_executor_idle_timeout[:-1]) + 900) + 's'
            log.warning(
                f'\nSetting {TextColors.yellow("spark.dynamicAllocation.cachedExecutorIdleTimeout")} as '
                f'{cached_executor_idle_timeout}. Executor with cached data block will be released '
                f'if it has been idle for this duration. If you wish to change the value of '
                f'cachedExecutorIdleTimeout, please provide the exact value of '
                f'spark.dynamicAllocation.cachedExecutorIdleTimeout in your spark args. If your job is performing '
                f'bad because the cached data was lost, please consider increasing this value.\n',
            )
        _append_spark_config(
            spark_opts, 'spark.dynamicAllocation.cachedExecutorIdleTimeout',
            cached_executor_idle_timeout,
        )

        min_ratio_executors = None
        default_dra_min_executor_ratio = self.default_spark_srv_conf['spark.yelp.dra.minExecutorRatio']
        if 'spark.dynamicAllocation.minExecutors' not in spark_opts:
            # the ratio of total executors to be used as minExecutors
            min_executor_ratio = spark_opts.get('spark.yelp.dra.minExecutorRatio', default_dra_min_executor_ratio)
            # set minExecutors default as a ratio of spark.executor.instances
            num_instances = int(
                spark_opts.get(
                    'spark.executor.instances',
                    self.default_spark_srv_conf['spark.executor.instances'],
                ),
            )
            min_executors = int(num_instances * float(min_executor_ratio))
            # minExecutors should not be more than initialExecutors
            if 'spark.dynamicAllocation.initialExecutors' in spark_opts:
                min_executors = min(min_executors, int(spark_opts['spark.dynamicAllocation.initialExecutors']))
            # minExecutors should not be more than maxExecutors
            if 'spark.dynamicAllocation.maxExecutors' in spark_opts:
                min_executors = min(
                    min_executors, int(int(spark_opts['spark.dynamicAllocation.maxExecutors']) *
                                       float(min_executor_ratio)),
                )

            min_ratio_executors = min_executors

            warn_msg = f'\nSetting {TextColors.yellow("spark.dynamicAllocation.minExecutors")} as'

            # set minExecutors equal to 0 for Jupyter Spark sessions
            if _is_jupyterhub_job(spark_app_name):
                min_executors = 0
                warn_msg = (
                    f'Looks like you are launching Spark session from a Jupyter notebook. '
                    f'{warn_msg} {min_executors} to save spark costs when any spark action is not running'
                )
            else:
                warn_msg = f'{warn_msg} {min_executors}'

            spark_opts['spark.dynamicAllocation.minExecutors'] = str(min_executors)
            log.warning(
                f'\n{warn_msg}. If you wish to change the value of minimum executors, please provide '
                f'the exact value of spark.dynamicAllocation.minExecutors in your spark args\n',
            )

            if not _is_jupyterhub_job(spark_app_name) and 'spark.yelp.dra.minExecutorRatio' not in spark_opts:
                log.debug(
                    f'\nspark.yelp.dra.minExecutorRatio not provided. This specifies the ratio of total executors '
                    f'to be used as minimum executors for Dynamic Resource Allocation. More info: y/spark-dra. '
                    f'Using default ratio: {default_dra_min_executor_ratio}. If you wish to change this value, '
                    f'please provide the desired spark.yelp.dra.minExecutorRatio in your spark args\n',
                )

        if 'spark.dynamicAllocation.maxExecutors' not in spark_opts:
            # set maxExecutors default equal to spark.executor.instances
            max_executors = int(
                spark_opts.get(
                    'spark.executor.instances',
                    self.default_spark_srv_conf['spark.executor.instances'],
                ),
            )
            # maxExecutors should not be less than initialExecutors
            if 'spark.dynamicAllocation.initialExecutors' in spark_opts:
                max_executors = max(max_executors, int(spark_opts['spark.dynamicAllocation.initialExecutors']))

            spark_opts['spark.dynamicAllocation.maxExecutors'] = str(max_executors)
            log.warning(
                f'\nSetting {TextColors.yellow("spark.dynamicAllocation.maxExecutors")} as {max_executors}. '
                f'If you wish to change the value of maximum executors, please provide the exact value of '
                f'spark.dynamicAllocation.maxExecutors in your spark args\n',
            )

        # TODO: add regex to better match Jupyterhub Spark session app name
        if 'jupyterhub' in spark_app_name and 'spark.dynamicAllocation.initialExecutors' not in spark_opts:
            if min_ratio_executors is not None:
                # set initialExecutors default equal to minimum executors calculated above using
                # 'spark.yelp.dra.minExecutorRatio' and `default_dra_min_executor_ratio` for Jupyter Spark sessions
                initial_executors = min_ratio_executors
            else:
                # otherwise set initial executors equal to minimum executors
                initial_executors = int(spark_opts['spark.dynamicAllocation.minExecutors'])

            spark_opts['spark.dynamicAllocation.initialExecutors'] = str(initial_executors)
            log.warning(
                f'\nSetting {TextColors.yellow("spark.dynamicAllocation.initialExecutors")} as {initial_executors}. '
                f'If you wish to change the value of initial executors, please provide the exact value of '
                f'spark.dynamicAllocation.initialExecutors in your spark args\n',
            )

        spark_opts['spark.executor.instances'] = spark_opts['spark.dynamicAllocation.minExecutors']
        return spark_opts

    def _cap_executor_resources(
            self,
            executor_cores: int,
            executor_memory: str,
            memory_mb: int,
    ) -> Tuple[int, str]:
        max_cores = self.spark_constants.get('resource_configs', {}).get('max', {})['cpu']
        max_memory_gb = self.spark_constants.get('resource_configs', {}).get('max', {})['mem']

        warning_title = 'Capped Executor Resources based on maximun available aws nodes'
        warning_title_printed = False

        if memory_mb > max_memory_gb * 1024:
            executor_memory = f'{max_memory_gb}g'
            log.warning(warning_title)
            log.warning(
                f'  - spark.executor.memory:    {int(memory_mb / 1024):3}g  → {executor_memory}',
            )
            warning_title_printed = True

        if executor_cores > max_cores:
            if not warning_title_printed:
                log.warning(warning_title)
            log.warning(
                f'  - spark.executor.cores:     {executor_cores:3}c  → {max_cores}c\n',
            )
            executor_cores = max_cores

        return executor_cores, executor_memory

    def compute_executor_instances_k8s(self, user_spark_opts: Dict[str, str]) -> int:
        executor_cores = int(
            user_spark_opts.get(
                'spark.executor.cores',
                self.default_spark_srv_conf['spark.executor.cores'],
            ),
        )

        if 'spark.executor.instances' in user_spark_opts:
            executor_instances = int(user_spark_opts['spark.executor.instances'])
        elif 'spark.cores.max' in user_spark_opts:
            # spark.cores.max provided, calculate based on (max cores // per-executor cores)
            executor_instances = (int(user_spark_opts['spark.cores.max']) // executor_cores)
        else:
            # spark.executor.instances and spark.cores.max not provided, the executor instances should at least
            # be equal to `default_executor_instances`.
            executor_instances = max(
                (self.default_spark_srv_conf['spark.executor.instances'] *
                 self.default_spark_srv_conf['spark.executor.cores']) // executor_cores,
                self.default_spark_srv_conf['spark.executor.instances'],
            )

        # Deprecation message
        if 'spark.cores.max' in user_spark_opts:
            log.warning(
                f'spark.cores.max is DEPRECATED. Replace with '
                f'spark.executor.instances={executor_instances} in --spark-args and in your service code '
                f'as "spark.executor.instances * spark.executor.cores" if used.\n',
            )

        return executor_instances

    def _recalculate_executor_resources(
            self,
            user_spark_opts: Dict[str, str],
            force_spark_resource_configs: bool,
            ratio_adj_thresh: int,
            pool: str,
    ) -> Dict[str, str]:
        executor_cores = int(
            user_spark_opts.get(
                'spark.executor.cores',
                self.default_spark_srv_conf['spark.executor.cores'],
            ),
        )
        executor_memory = user_spark_opts.get(
            'spark.executor.memory',
            f'{self.default_spark_srv_conf["spark.executor.memory"]}g',
        )
        executor_instances = int(
            user_spark_opts.get(
                'spark.executor.instances',
                self.default_spark_srv_conf['spark.executor.instances'],
            ),
        )
        task_cpus = int(
            user_spark_opts.get(
                'spark.task.cpus',
                self.default_spark_srv_conf['spark.task.cpus'],
            ),
        )

        memory_mb = parse_memory_string(executor_memory)
        memory_gb = math.ceil(memory_mb / 1024)

        def _calculate_resources(
                cpu: int,
                memory: int,
                instances: int,
                task_cpus: int,
                target_memory: int,
                ratio_adj_thresh: int,
        ) -> Tuple[int, str, int, int]:
            """
            Calculate resource needed based on memory size and recommended mem:core ratio (7:1).

            Parameters:
            memory: integer values in GB.

            Returns:
            A tuple of (new_cpu, new_memory, new_instances, task_cpus).
            """
            # For multi-step release
            if memory > ratio_adj_thresh:
                return cpu, f'{memory}g', instances, task_cpus

            target_mem_cpu_ratio = self.spark_constants['target_mem_cpu_ratio']

            new_cpu: int
            new_memory: int
            new_instances = (instances * memory) // target_memory
            if new_instances > 0:
                new_cpu = int(target_memory / target_mem_cpu_ratio)
                new_memory = target_memory
            else:
                new_instances = 1
                new_cpu = max(memory // target_mem_cpu_ratio, 1)
                new_memory = new_cpu * target_mem_cpu_ratio

            if cpu != new_cpu or memory != new_memory or instances != new_instances:
                log.warning(
                    f'Adjust Executor Resources based on recommended mem:core:: 7:1 and Bucket: '
                    f'{new_memory}g, {new_cpu}cores to better fit aws nodes\n'
                    f'  - spark.executor.cores:     {cpu:3}c  → {new_cpu}c\n'
                    f'  - spark.executor.memory:    {memory:3}g  → {new_memory}g\n'
                    f'  - spark.executor.instances: {instances:3}x  → {new_instances}x\n'
                    'Check y/spark-metrics to compare how your job performance compared to previous runs.\n'
                    'Feel free to adjust to spark resource configs in yelpsoa-configs with above newly '
                    'adjusted values.\n',
                )

            if new_cpu < task_cpus:
                log.warning(
                    f'Given spark.task.cpus is {task_cpus}, '
                    f'=> adjusted to {new_cpu} to keep it within the limits of adjust spark.executor.cores.\n',
                )
                task_cpus = new_cpu
            return new_cpu, f'{new_memory}g', new_instances, task_cpus

        # Constants
        recommended_memory_gb = self.spark_constants.get('resource_configs', {}).get('recommended', {})['mem']
        medium_cores = self.spark_constants.get('resource_configs', {}).get('medium', {})['cpu']
        medium_memory_mb = self.spark_constants.get('resource_configs', {}).get('medium', {})['mem']
        max_cores = self.spark_constants.get('resource_configs', {}).get('max', {})['cpu']
        max_memory_gb = self.spark_constants.get('resource_configs', {}).get('max', {})['mem']

        if pool not in ['batch', 'stable_batch']:
            log.warning(
                f'We are not internally adjusting any spark resources for given pool {pool}. '
                'Please ensure that given resource requests are optimal.',
            )
        elif memory_gb > max_memory_gb or executor_cores > max_cores:
            executor_cores, executor_memory = self._cap_executor_resources(executor_cores, executor_memory, memory_mb)
        elif force_spark_resource_configs:
            log.warning(
                '--force-spark-resource-configs is set to true: this can result in non-optimal bin-packing '
                'of executors on aws nodes or can lead to wastage the resources. '
                "Please use this flag only if you have tested that standard memory/cpu configs won't work for "
                'your job.\nLet us know at #spark if you think, your use-case needs to be standardized.\n',
            )
        elif memory_gb >= medium_memory_mb or executor_cores > medium_cores:
            (executor_cores, executor_memory, executor_instances, task_cpus) = _calculate_resources(
                executor_cores,
                memory_gb,
                executor_instances,
                task_cpus,
                medium_memory_mb,
                ratio_adj_thresh,
            )
        else:
            (executor_cores, executor_memory, executor_instances, task_cpus) = _calculate_resources(
                executor_cores,
                memory_gb,
                executor_instances,
                task_cpus,
                recommended_memory_gb,
                ratio_adj_thresh,
            )

        user_spark_opts.update({
            'spark.executor.cores': str(executor_cores),
            'spark.kubernetes.executor.limit.cores': str(executor_cores),
            'spark.executor.memory': str(executor_memory),
            'spark.executor.instances': str(executor_instances),
            'spark.task.cpus': str(task_cpus),
        })
        if 'spark.cores.max' in user_spark_opts:
            user_spark_opts['spark.cores.max'] = str(executor_instances * executor_cores)
        return user_spark_opts

    def _append_sql_partitions_conf(self, spark_opts: Dict[str, str]) -> Dict[str, str]:
        if 'spark.sql.shuffle.partitions' not in spark_opts:
            num_partitions = 3 * (
                int(spark_opts.get('spark.cores.max', 0)) or
                (int(spark_opts.get('spark.executor.instances', 0)) *
                 int(spark_opts.get('spark.executor.cores', self.default_spark_srv_conf['spark.executor.cores'])))
            )

            if (
                    'spark.dynamicAllocation.enabled' in spark_opts and
                    str(spark_opts['spark.dynamicAllocation.enabled']) == 'true' and
                    'spark.dynamicAllocation.maxExecutors' in spark_opts and
                    str(spark_opts['spark.dynamicAllocation.maxExecutors']) != 'infinity'
            ):
                num_partitions_dra = 3 * (
                    int(spark_opts.get('spark.dynamicAllocation.maxExecutors', 0)) *
                    int(spark_opts.get('spark.executor.cores', self.default_spark_srv_conf['spark.executor.cores']))
                )
                num_partitions = max(num_partitions, num_partitions_dra)

            num_partitions = num_partitions or self.default_spark_srv_conf['spark.sql.shuffle.partitions']
            _append_spark_config(spark_opts, 'spark.sql.shuffle.partitions', str(num_partitions))
        else:
            num_partitions = int(spark_opts['spark.sql.shuffle.partitions'])
        _append_spark_config(spark_opts, 'spark.sql.files.minPartitionNum', str(num_partitions))
        _append_spark_config(spark_opts, 'spark.default.parallelism', str(num_partitions))

        return spark_opts

    def _adjust_spark_requested_resources(
            self,
            user_spark_opts: Dict[str, str],
            cluster_manager: str,
            pool: str,
            force_spark_resource_configs: bool = False,
            ratio_adj_thresh: Any = None,
    ) -> Dict[str, str]:
        if ratio_adj_thresh is None:
            ratio_adj_thresh = self.spark_constants['adjust_executor_res_ratio_thresh']
        executor_cores = int(
            user_spark_opts.setdefault(
                'spark.executor.cores',
                str(self.default_spark_srv_conf['spark.executor.cores']),
            ),
        )
        if cluster_manager == 'kubernetes':
            executor_instances = self.compute_executor_instances_k8s(user_spark_opts)
            user_spark_opts.setdefault('spark.executor.instances', str(executor_instances))
            max_cores = executor_instances * executor_cores
            if (
                    'spark.mesos.executor.memoryOverhead' in user_spark_opts and
                    'spark.executor.memoryOverhead' not in user_spark_opts
            ):
                user_spark_opts['spark.executor.memoryOverhead'] = user_spark_opts[
                    'spark.mesos.executor.memoryOverhead'
                ]
            user_spark_opts.setdefault('spark.kubernetes.executor.limit.cores', str(executor_cores))
            waiting_time = (
                self.spark_constants['default_clusterman_observed_scaling_time'] +
                executor_instances * self.spark_constants['default_resources_waiting_time_per_executor'] // 60
            )
            user_spark_opts.setdefault(
                'spark.scheduler.maxRegisteredResourcesWaitingTime',
                str(waiting_time) + 'min',
            )
        elif cluster_manager == 'local':
            executor_instances = int(
                user_spark_opts.setdefault(
                    'spark.executor.instances',
                    str(self.default_spark_srv_conf['spark.executor.instances']),
                ),
            )
            max_cores = executor_instances * executor_cores
        else:
            raise UnsupportedClusterManagerException(cluster_manager)

        if max_cores < executor_cores:
            raise ValueError(f'Total number of cores {max_cores} is less than per-executor cores {executor_cores}')

        # TODO: replace this with kubernetes specific config
        num_gpus = int(user_spark_opts.get('spark.mesos.gpus.max', '0'))
        task_cpus = int(user_spark_opts.get('spark.task.cpus', '1'))
        # we can skip this step if user is not using gpu or do not configure
        # task cpus and executor cores
        if num_gpus == 0 or (task_cpus != 1 and executor_cores != 1):
            return self._recalculate_executor_resources(
                user_spark_opts, force_spark_resource_configs,
                ratio_adj_thresh, pool,
            )

        if num_gpus > GPUS_HARD_LIMIT:
            raise ValueError(
                'Requested {num_gpus} GPUs, which exceeds hard limit of {GPUS_HARD_LIMIT}',
            )

        with open(GPU_POOLS_YAML_FILE_PATH) as fp:
            pool_def = yaml.safe_load(fp).get(pool)

        if pool_def is None:
            raise ValueError(
                'Unable to adjust spark.task.cpus and spark.executor.cores because '
                f"pool \"{pool}\" not found in gpu_pools",
            )

        gpus_per_inst = int(pool_def['gpus_per_instance'])
        cpus_per_inst = int(pool_def['cpus_per_instance'])
        if gpus_per_inst == 0 or cpus_per_inst == 0:
            raise ValueError(
                'Unable to adjust spark.task.cpus and spark.executor.cores because '
                f'pool {pool} does not appear to have any GPUs and/or CPUs',
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
        num_cpus = int(executor_instances) * int(executor_cores)
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
        return self._recalculate_executor_resources(
            user_spark_opts, force_spark_resource_configs,
            ratio_adj_thresh, pool,
        )

    def update_spark_srv_configs(self, spark_conf: MutableMapping[str, str]):
        additional_conf = {k: v for k, v in self.mandatory_default_spark_srv_conf.items() if k not in spark_conf}
        spark_conf.update(additional_conf)

    def get_history_url(self, spark_conf: Mapping[str, str]) -> Optional[str]:
        if spark_conf.get('spark.eventLog.enabled') != 'true':
            return None
        event_log_dir = spark_conf.get('spark.eventLog.dir')
        for env, env_conf in self.spark_srv_conf['environments'].items():
            if event_log_dir == env_conf['default_event_log_dir']:
                return env_conf['history_server']
        return None

    def _append_event_log_conf(
            self,
            spark_opts: Dict[str, str],
            access_key: Optional[str] = None,
            secret_key: Optional[str] = None,
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

        if len(self.spark_srv_conf.items()) == 0:
            log.warning('spark_srv_conf is empty, disable event log')
            spark_opts.update({'spark.eventLog.enabled': 'false'})
            return spark_opts

        if access_key:
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

            for conf in self.spark_srv_conf.get('environments', {}).values():
                if account_id == conf['account_id']:
                    spark_opts['spark.eventLog.enabled'] = 'true'
                    spark_opts['spark.eventLog.dir'] = conf['default_event_log_dir']
                    return spark_opts

        else:
            environment_config = self.spark_srv_conf.get('environments', {}).get(
                utils.get_runtime_env(),
            )
            if environment_config:
                spark_opts.update({
                    'spark.eventLog.enabled': 'true',
                    'spark.eventLog.dir': environment_config['default_event_log_dir'],
                })
                return spark_opts

        log.warning('Disable event log because No preset event log dir')
        spark_opts['spark.eventLog.enabled'] = 'false'
        return spark_opts

    def compute_approx_hourly_cost_dollars(
            self,
            spark_conf: Mapping[str, str],
            paasta_cluster: str,
            paasta_pool: str,
    ) -> Tuple[float, float]:
        per_executor_cores = int(spark_conf.get(
            'spark.executor.cores',
            self.default_spark_srv_conf['spark.executor.cores'],
        ))
        max_cores = per_executor_cores * (int(spark_conf.get(
            'spark.executor.instances',
            self.default_spark_srv_conf['spark.executor.instances'],
        )))
        min_cores = max_cores
        if 'spark.dynamicAllocation.enabled' in spark_conf and spark_conf['spark.dynamicAllocation.enabled'] == 'true':
            max_cores = per_executor_cores * (int(
                spark_conf.get('spark.dynamicAllocation.maxExecutors', max_cores),
            ))
            min_cores = per_executor_cores * (int(
                spark_conf.get('spark.dynamicAllocation.minExecutors', min_cores),
            ))

        cost_factor = self.spark_costs.get(paasta_cluster, dict()).get(paasta_pool, 0)

        min_dollars = round(min_cores * cost_factor, 5)
        max_dollars = round(max_cores * cost_factor, 5)
        if max_dollars * 24 > self.spark_constants['high_cost_threshold_daily']:
            log.warning(
                TextColors.red(
                    TextColors.bold(
                        '\n!!!!! HIGH COST ALERT !!!!!',
                    ),
                ),
            )
        log.warning(
            TextColors.magenta(
                TextColors.bold(
                    f'\nExpected {"maximum" if min_dollars != max_dollars else ""} cost based on requested resources: '
                    f'${str(max_dollars)} every hour and ${str(max_dollars * 24)} in a day.'
                    f'\nPlease monitor y/spark-metrics for memory and cpu usage to tune requested executor count'
                    f' config spark.executor.instances.\nFollow y/write-spark-job for optimization tips.\n',
                ),
            ),
        )
        return min_dollars, max_dollars

    def get_spark_conf(
        self,
        cluster_manager: str,
        spark_app_base_name: str,
        user_spark_opts: Mapping[str, Any],
        paasta_cluster: str,
        paasta_pool: str,
        paasta_service: str,
        paasta_instance: str,
        docker_img: str,
        aws_creds: Optional[Tuple[Optional[str], Optional[str], Optional[str]]] = None,
        extra_volumes: Optional[List[Mapping[str, str]]] = None,
        use_eks: bool = False,
        k8s_server_address: Optional[str] = None,
        spark_opts_from_env: Optional[Mapping[str, str]] = None,
        aws_region: Optional[str] = None,
        service_account_name: Optional[str] = None,
        force_spark_resource_configs: bool = True,
        user: Optional[str] = None,
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
        :param use_eks: flag to specify if EKS cluster should be used
        :param k8s_server_address: address of the k8s server to be used
        :param spark_opts_from_env: different from user_spark_opts, configuration in this
            dict will not be filtered. This options is left for people who use `paasta spark-run`
            to launch the batch, and inside the batch use `spark_tools.paasta` to create
            spark session.
        :param aws_region: The default aws region to use
        :param service_account_name: The k8s service account to use for spark k8s authentication.
            If not provided, it uses cert files at {K8S_AUTH_FOLDER} to authenticate.
        :param force_spark_resource_configs: skip the resource/instances recalculation.
            This is strongly not recommended.
        :returns: spark opts in a dict.
        """
        # Mesos deprecation
        if cluster_manager == 'mesos':
            log.warning('Mesos has been deprecated. Please use kubernetes as the cluster manager.\n')
            raise UnsupportedClusterManagerException(cluster_manager)

        # for simplicity, all the following computation are assuming spark opts values
        # is str type.
        user_spark_opts = _convert_user_spark_opts_value_to_str(user_spark_opts)

        app_base_name = (
            user_spark_opts.get('spark.app.name') or
            spark_app_base_name
        )

        # Pick a port from a pre-defined port range, which will then be used by our Jupyter
        # server metric aggregator API. The aggregator API collects Prometheus metrics from multiple
        # Spark sessions and exposes them through a single endpoint.
        try:
            ui_port = int(
                (spark_opts_from_env or {}).get('spark.ui.port') or
                utils.ephemeral_port_reserve_range(
                    self.spark_constants.get('preferred_spark_ui_port_start'),
                    self.spark_constants.get('preferred_spark_ui_port_end'),
                ),
            )
        except Exception as e:
            log.warning(
                f'Could not get an available port using srv-config port range: {e}. '
                'Using default port range to get an available port.',
            )
            ui_port = utils.ephemeral_port_reserve_range()

        spark_conf = {**(spark_opts_from_env or {}), **_filter_user_spark_opts(user_spark_opts)}
        random_postfix = utils.get_random_string(4)

        if aws_creds is not None and aws_creds[2] is not None:
            spark_conf['spark.hadoop.fs.s3a.aws.credentials.provider'] = AWS_ENV_CREDENTIALS_PROVIDER

        # app_name from env is already appended with port and time to make it unique
        app_name = (spark_opts_from_env or {}).get('spark.app.name')
        if not app_name:
            app_name = f'{app_base_name}_{ui_port}_{int(time.time())}_{random_postfix}'
        is_jupyter = _is_jupyterhub_job(app_name)

        # Explicitly setting app id: replace special characters to '_' to make it consistent
        # in all places for metric systems:
        # - since in the Promehteus metrics endpoint those will be converted to '_'
        # - while the 'spark-app-selector' executor pod label will keep the original app id
        if is_jupyter:
            raw_app_id = app_name
        else:
            raw_app_id = f'{paasta_service}__{paasta_instance}__{random_postfix}'
        app_id = re.sub(r'[\.,-]', '_', utils.get_k8s_resource_name_limit_size_with_hash(raw_app_id))

        # Starting Spark 3.4+, spark-app-name label has been added. Limiting to 63 characters
        app_name = utils.get_k8s_resource_name_limit_size_with_hash(app_name)

        spark_conf.update({
            'spark.app.name': app_name,
            'spark.app.id': app_id,
            'spark.ui.port': str(ui_port),
        })

        # adjusted with spark default resources
        spark_conf = self._adjust_spark_requested_resources(
            spark_conf, cluster_manager, paasta_pool, force_spark_resource_configs,
        )

        # Add pod template file
        pod_template_path = utils.generate_pod_template_path()
        try:
            utils.create_pod_template(pod_template_path, app_base_name)
        except Exception as e:
            log.error(f'Failed to generate Spark executor pod template: {e}')
            pod_template_path = ''

        if cluster_manager == 'kubernetes':
            spark_conf.update(_get_k8s_spark_env(
                paasta_cluster=paasta_cluster,
                paasta_service=paasta_service,
                paasta_instance=paasta_instance,
                docker_img=docker_img,
                pod_template_path=pod_template_path,
                volumes=extra_volumes,
                paasta_pool=paasta_pool,
                driver_ui_port=ui_port,
                service_account_name=service_account_name,
                include_self_managed_configs=not use_eks,
                k8s_server_address=k8s_server_address,
                user=user,
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
        spark_conf = self.get_dra_configs(spark_conf)

        # generate cost warnings
        self.compute_approx_hourly_cost_dollars(spark_conf, paasta_cluster, paasta_pool)

        # configure spark prometheus metrics
        spark_conf = self._append_spark_prometheus_conf(spark_conf)

        # configure spark_event_log
        if aws_creds:
            spark_conf = self._append_event_log_conf(spark_conf, *aws_creds)
        else:
            spark_conf = self._append_event_log_conf(spark_conf)

        # configure sql shuffle partitions
        spark_conf = self._append_sql_partitions_conf(spark_conf)

        # add spark srv config defaults if not specified
        self.update_spark_srv_configs(spark_conf)

        # configure spark Console Progress
        if is_jupyter:
            spark_conf = _append_spark_config(spark_conf, 'spark.ui.showConsoleProgress', 'true')

        if aws_creds:
            spark_conf = _append_aws_credentials_conf(spark_conf, *aws_creds, aws_region)

        return spark_conf


def parse_memory_string(memory_string: Optional[str]) -> int:
    """Return memory size in MB."""
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


def get_grafana_url(spark_conf: Mapping[str, str]) -> str:
    return (
        'http://y/spark-monitoring?'
        f"var-paasta_cluster={spark_conf['spark.executorEnv.PAASTA_CLUSTER']}&"
        f"var-service={spark_conf['spark.executorEnv.PAASTA_SERVICE']}&"
        f"var-instance={spark_conf['spark.executorEnv.PAASTA_INSTANCE']}"
    )


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
