import pytest

from service_configuration_lib.spark_config import get_k8s_spark_env
from service_configuration_lib.spark_config import get_mesos_spark_auth_env
from service_configuration_lib.spark_config import get_mesos_spark_env


@pytest.fixture
def config_args():
    return dict(
        spark_app_name='my-spark-app',
        spark_ui_port='1234',
        mesos_leader='the-mesos-leader:5050',
        paasta_cluster='westeros-devc',
        paasta_pool='spark-pool',
        paasta_service='spark-service',
        paasta_instance='batch',
        docker_img='docker-dev.nowhere.com/spark:latest',
        volumes=['/nail/var:/nail/var:ro', '/etc/foo:/etc/foo:RO'],
        user_spark_opts={},
        event_log_dir='/var/log',
    )


@pytest.fixture
def k8s_config_args():
    return dict(
        spark_app_name='my-spark-app',
        spark_ui_port='1234',
        paasta_cluster='westeros-devc',
        paasta_service='spark-service',
        paasta_instance='batch',
        paasta_pool='spark-pool',
        docker_img='docker-dev.nowhere.com/spark:latest',
        user_spark_opts={},
        event_log_dir='/var/log',
        volumes=[
            {
                'hostPath': '/nail/etc/beep',
                'containerPath': '/nail/etc/beep',
                'mode': 'RO',
            },
            {
                'hostPath': '/nail/etc/bop',
                'containerPath': '/nail/etc/bop',
                'mode': 'RW',
            },
        ],
    )


def test_non_config_opts(config_args):
    config_args['user_spark_opts']['spark.master'] = 'foo'
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


def test_non_config_opts_k8s_volume(config_args):
    config_args['user_spark_opts']['spark.kubernetes.executor.volumes.hostPath.some_path.mount.path'] = 'foo'
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
        'spark.mesos.constraints': 'pool:spark-pool',
        'spark.mesos.executor.docker.forcePullImage': 'true',
        'spark.mesos.executor.docker.image': 'docker-dev.nowhere.com/spark:latest',
        'spark.mesos.executor.docker.parameters': 'cpus=2,label=paasta_service=spark-service,'
        'label=paasta_instance=batch',
        'spark.mesos.executor.docker.volumes': '/nail/var:/nail/var:ro,/etc/foo:/etc/foo:ro',
        'spark.sql.shuffle.partitions': shuffle_partitions or '8',
        'spark.ui.port': '1234',
    }
    if needs_docker_cfg:
        expected['spark.mesos.uris'] = 'file:///root/.dockercfg'
    assert spark_env == expected


@pytest.mark.parametrize('log_dir,expected', [('/var/log', True), (None, False)])
def test_get_mesos_spark_env_event_log_dir(config_args, log_dir, expected):
    config_args['event_log_dir'] = log_dir
    spark_env = get_mesos_spark_env(**config_args)
    assert ('spark.eventLog.dir' in spark_env) == expected


def test_get_mesos_spark_env_incorrect_file_mode(config_args):
    config_args['volumes'] = ['/nail/var:/nail/var:false', '/etc/foo:/etc/foo:false']
    with pytest.raises(ValueError):
        get_mesos_spark_env(**config_args)


def test_get_mesos_spark_auth_env():
    assert get_mesos_spark_auth_env() == {
        'SPARK_MESOS_PRINCIPAL': 'spark',
        'SPARK_MESOS_SECRET': 'SHARED_SECRET(SPARK_MESOS_SECRET)',
    }


@pytest.mark.parametrize('shuffle_partitions', [None, 20])
def test_get_k8s_spark_env(shuffle_partitions, k8s_config_args):
    k8s_config_args['user_spark_opts']['spark.sql.shuffle.partitions'] = shuffle_partitions
    assert get_k8s_spark_env(**k8s_config_args) == {
        'spark.master': 'k8s://https://k8s.paasta-westeros-devc.yelp:16443',
        'spark.ui.port': '1234',
        'spark.executorEnv.PAASTA_SERVICE': 'spark-service',
        'spark.executorEnv.PAASTA_INSTANCE': 'batch',
        'spark.executorEnv.PAASTA_CLUSTER': 'westeros-devc',
        'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
        'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
        'spark.kubernetes.pyspark.pythonVersion': '3',
        'spark.kubernetes.container.image': 'docker-dev.nowhere.com/spark:latest',
        'spark.kubernetes.namespace': 'paasta-spark',
        'spark.kubernetes.authenticate.caCertFile': '/etc/spark_k8s_secrets/westeros-devc-ca.crt',
        'spark.kubernetes.authenticate.clientKeyFile': '/etc/spark_k8s_secrets/westeros-devc-client.key',
        'spark.kubernetes.authenticate.clientCertFile': '/etc/spark_k8s_secrets/westeros-devc-client.crt',
        'spark.kubernetes.container.image.pullPolicy': 'Always',
        'spark.app.name': 'my-spark-app',
        'spark.cores.max': '4',
        'spark.executor.cores': '2',
        'spark.executor.memory': '4g',
        'spark.eventLog.enabled': 'true',
        'spark.sql.shuffle.partitions': shuffle_partitions or '8',
        'spark.eventLog.dir': '/var/log',
        'spark.kubernetes.executor.volumes.hostPath.0.mount.path': '/nail/etc/beep',
        'spark.kubernetes.executor.volumes.hostPath.0.mount.readOnly': 'true',
        'spark.kubernetes.executor.volumes.hostPath.0.options.path': '/nail/etc/beep',
        'spark.kubernetes.executor.volumes.hostPath.1.mount.path': '/nail/etc/bop',
        'spark.kubernetes.executor.volumes.hostPath.1.mount.readOnly': 'false',
        'spark.kubernetes.executor.volumes.hostPath.1.options.path': '/nail/etc/bop',
        'spark.kubernetes.executor.label.yelp.com/paasta_instance': 'batch',
        'spark.kubernetes.executor.label.yelp.com/paasta_service': 'spark-service',
        'spark.kubernetes.executor.label.yelp.com/paasta_cluster': 'westeros-devc',
        'spark.kubernetes.executor.label.paasta.yelp.com/service': 'spark-service',
        'spark.kubernetes.executor.label.paasta.yelp.com/instance': 'batch',
        'spark.kubernetes.executor.label.paasta.yelp.com/cluster': 'westeros-devc',
        'spark.kubernetes.node.selector.yelp.com/pool': 'spark-pool',
        'spark.kubernetes.executor.label.yelp.com/pool': 'spark-pool',
    }


@pytest.mark.parametrize('log_dir,expected', [('/var/log', True), (None, False)])
def test_get_k8s_spark_env_event_log_dir(k8s_config_args, log_dir, expected):
    k8s_config_args['event_log_dir'] = log_dir
    spark_env = get_k8s_spark_env(**k8s_config_args)
    assert ('spark.eventLog.dir' in spark_env) == expected
