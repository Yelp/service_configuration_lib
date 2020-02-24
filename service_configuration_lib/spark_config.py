import logging
from typing import Any
from typing import List
from typing import Mapping
from typing import MutableMapping
from typing import Optional

DEFAULT_SPARK_MESOS_SECRET_FILE = '/nail/etc/paasta_spark_secret'
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
    'spark.hadoop.fs.s3a.access.key',
    'spark.hadoop.fs.s3a.secret.key',
}
log = logging.Logger(__name__)


def get_mesos_spark_env(
    spark_app_name: str,
    spark_ui_port: str,
    mesos_leader: str,
    mesos_secret: str,
    paasta_cluster: str,
    paasta_pool: str,
    paasta_service: str,
    paasta_instance: str,
    docker_img: str,
    volumes: List[str],
    user_spark_opts: Mapping[str, Any],
    event_log_dir: Optional[str] = None,
    needs_docker_cfg: bool = False,
) -> Mapping[str, str]:
    user_non_config_opts = set(user_spark_opts) & NON_CONFIGURABLE_SPARK_OPTS
    if user_non_config_opts:
        raise ValueError(f'The following Spark options are not user-configurable: {user_non_config_opts}')

    spark_env: MutableMapping[str, str] = {
        'spark.master': f'mesos://{mesos_leader}',
        'spark.ui.port': spark_ui_port,
        'spark.executorEnv.PAASTA_SERVICE': paasta_service,
        'spark.executorEnv.PAASTA_INSTANCE': paasta_instance,
        'spark.executorEnv.PAASTA_CLUSTER': paasta_cluster,
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        'spark.mesos.executor.docker.parameters':
            f'label=paasta_service={paasta_service},label=paasta_instance={paasta_instance}',
        'spark.mesos.executor.docker.volumes': ','.join(volumes),
        'spark.mesos.executor.docker.image': docker_img,
        'spark.mesos.principal': 'spark',
        'spark.mesos.secret': mesos_secret,

        # User-configurable defaults here
        'spark.app.name': spark_app_name,
        'spark.cores.max': '4',
        'spark.executor.cores': '2',
        'spark.executor.memory': '4g',
        # Use \; for multiple constraints, e.g. 'instance_type:m4.10xlarge\;pool:default'
        'spark.mesos.constraints': f'pool:{paasta_pool}',
        'spark.mesos.executor.docker.forcePullImage': 'true',
        'spark.eventLog.enabled': 'true',
    }
    if needs_docker_cfg:
        spark_env['spark.mesos.uris'] = 'file:///root/.dockercfg'

    spark_env = {**spark_env, **user_spark_opts}

    # spark_opts could be a mix of string and numbers.
    if int(spark_env['spark.executor.cores']) > int(spark_env['spark.cores.max']):
        raise ValueError(
            'spark.executor.cores={executor_cores} should be not greater than spark.cores.max={max_cores}'.format(
                executor_cores=spark_env['spark.executor.cores'],
                max_cores=spark_env['spark.cores.max'],
            ),
        )

    if not spark_env.get('spark.sql.shuffle.partitions'):
        spark_env['spark.sql.shuffle.partitions'] = str(2 * int(spark_env['spark.cores.max']))
        log.warning(
            'spark.sql.shuffle.partitions has been set to {num_partitions} '
            'to be equal to twice the number of requested cores, but you should '
            'consider setting a higher value if necessary.'
            ' Follow y/spark for help on partition sizing'.format(
                num_partitions=spark_env['spark.sql.shuffle.partitions'],
            ),
        )

    if spark_env['spark.eventLog.enabled'] == 'true':
        if not spark_env.get('spark.eventLog.dir'):
            if not event_log_dir:
                raise ValueError('Asked for event logging but spark.eventLog.dir missing')
            spark_env['spark.eventLog.dir'] = event_log_dir
        log.info(f'Spark event logs available in {event_log_dir}')

    spark_env['spark.mesos.executor.docker.parameters'] = 'cpus={}'.format(spark_env['spark.executor.cores'])
    exec_mem = spark_env['spark.executor.memory']
    if exec_mem[-1] != 'g' or not exec_mem[:-1].isdigit():
        raise ValueError('Executor memory {} not in format dg.'.format(spark_env['spark.executor.memory']))
    if int(exec_mem[:-1]) > 32:
        log.warning(
            f'You have specified a large amount of memory ({exec_mem[:-1]} > 32g); '
            f'please make sure that you actually need this much, or reduce your memory requirements',
        )

    return spark_env


def stringify_spark_env(spark_env: Mapping[str, str]) -> str:
    return ' '.join([f'--conf {k}={v}' for k, v in spark_env.items()])
