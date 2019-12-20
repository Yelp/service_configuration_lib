import pytest

from service_configuration_lib.spark_config import get_mesos_spark_env


@pytest.fixture
def config_args():
    return dict(
        spark_app_name='my-spark-app',
        spark_ui_port='1234',
        mesos_leader='the-mesos-leader:5050',
        mesos_secret='SUPER_SECRET_STRING',
        paasta_cluster='westeros-devc',
        paasta_pool='spark-pool',
        paasta_service='spark-service',
        paasta_instance='batch',
        docker_img='docker-dev.nowhere.com/spark:latest',
        volumes=['/nail/var:/nail/var:ro', '/etc/foo:/etc/foo:ro'],
        user_spark_opts={},
        event_log_dir='/var/log',
    )


def test_non_config_opts(config_args):
    config_args['user_spark_opts']['spark.master'] = 'foo'
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


def test_invalid_cores(config_args):
    config_args['user_spark_opts']['spark.executor.cores'] = 10
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


def test_invalid_mem(config_args):
    config_args['user_spark_opts']['spark.executor.memory'] = '64x'
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


def test_no_event_log_dir(config_args):
    config_args['event_log_dir'] = None
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


@pytest.mark.parametrize('shuffle_partitions', [None, 20])
@pytest.mark.parametrize('needs_docker_cfg', [True, False])
def test_get_mesos_spark_env(shuffle_partitions, needs_docker_cfg, config_args):
    config_args['user_spark_opts']['spark.sql.shuffle.partitions'] = shuffle_partitions
    config_args['needs_docker_cfg'] = needs_docker_cfg
    spark_env = get_mesos_spark_env(**config_args)
    expected = {
        'spark.app.name': 'my-spark-app',
        'spark.cores.max': '4',
        'spark.eventLog.dir': '/var/log',
        'spark.eventLog.enabled': 'true',
        'spark.executor.cores': '2',
        'spark.executor.memory': '4g',
        'spark.executorEnv.PAASTA_CLUSTER': 'westeros-devc',
        'spark.executorEnv.PAASTA_INSTANCE': 'batch',
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.PAASTA_SERVICE': 'spark-service',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        'spark.master': 'mesos://the-mesos-leader:5050',
        'spark.mesos.constraints': 'pool:{paasta_pool}',
        'spark.mesos.executor.docker.forcePullImage': 'true',
        'spark.mesos.executor.docker.image': 'docker-dev.nowhere.com/spark:latest',
        'spark.mesos.executor.docker.parameters': 'cpus=2',
        'spark.mesos.executor.docker.volumes': '/nail/var:/nail/var:ro,/etc/foo:/etc/foo:ro',
        'spark.mesos.principal': 'spark',
        'spark.mesos.secret': 'SUPER_SECRET_STRING',
        'spark.sql.shuffle.partitions': shuffle_partitions or '8',
        'spark.ui.port': '1234',
    }
    if needs_docker_cfg:
        expected['spark.mesos.uris'] = 'file:///root/.dockercfg'
    assert spark_env == expected
