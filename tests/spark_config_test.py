import functools
import itertools
import json
import os
import sys
from unittest import mock

import pytest
import requests
import yaml

from service_configuration_lib import spark_config

TEST_ACCOUNT_ID = '123456789'


@pytest.fixture
def mock_log(monkeypatch):
    mock_log = mock.Mock()
    monkeypatch.setattr(spark_config, 'log', mock_log)
    return mock_log


@pytest.fixture
def mock_spark_run_conf(tmpdir, monkeypatch):
    spark_run_conf = {
        'environments': {
            'testing': {
                'account_id': TEST_ACCOUNT_ID,
                'default_event_log_dir': 's3a://test/eventlog',
                'history_server': 'https://spark-history-testing',
            },
        },
    }
    fp = tmpdir.join('spark_run.yaml')
    fp.write(yaml.dump(spark_run_conf))
    monkeypatch.setattr(spark_config, 'DEFAULT_SPARK_RUN_CONFIG', str(fp))
    spark_config._load_spark_srv_conf(spark_run_conf)
    return spark_run_conf


@pytest.fixture
def mock_time():
    with mock.patch.object(spark_config.time, 'time', return_value=123.456):
        yield 123.456


class TestGetAWSCredentials:

    access_key = 'test_key'
    secret_key = 'test_secret'
    session_token = 'test_token'

    temp_creds = {
        'aws_access_key_id': access_key,
        'aws_secret_access_key': secret_key,
        'aws_session_token': session_token,
    }
    expected_temp_creds = (access_key, secret_key, session_token)

    creds = {'aws_access_key_id': access_key, 'aws_secret_access_key': secret_key}
    expected_creds = (access_key, secret_key, None)

    def test_no_aws_creds(self):
        assert spark_config.get_aws_credentials(no_aws_credentials=True) == (None, None, None)

    @pytest.mark.parametrize('aws_creds', [temp_creds, creds])
    def test_aws_credentials_yaml(self, tmpdir, aws_creds):
        fp = tmpdir.join('test.yaml')
        fp.write(yaml.dump(aws_creds))
        expected_output = self.expected_temp_creds if aws_creds == self.temp_creds else self.expected_creds
        assert spark_config.get_aws_credentials(aws_credentials_yaml=str(fp)) == expected_output

    def test_use_service_credentials(self, tmpdir, monkeypatch):
        test_service = 'test_service'
        creds_dir = tmpdir.mkdir('creds')
        creds_file = creds_dir.join('test_service.yaml')
        creds_file.write(yaml.dump(self.creds))
        monkeypatch.setattr(spark_config, 'AWS_CREDENTIALS_DIR', str(creds_dir))
        assert spark_config.get_aws_credentials(service=test_service) == self.expected_creds

    @pytest.fixture
    def mock_session(self):
        mock_session = mock.Mock()
        with mock.patch.object(spark_config, 'Session', return_value=mock_session):
            mock_session.get_credentials.return_value = mock.Mock(
                access_key=self.access_key,
                secret_key=self.secret_key,
                token=self.session_token,
            )
            yield mock_session

    def test_use_service_credentials_missing_file(self, tmpdir, monkeypatch, mock_session, mock_log):
        test_service = 'not_exist'
        creds_dir = tmpdir.mkdir('creds')
        monkeypatch.setattr(spark_config, 'AWS_CREDENTIALS_DIR', str(creds_dir))
        assert spark_config.get_aws_credentials(service=test_service) == self.expected_temp_creds
        (warning_msg,), _ = mock_log.warning.call_args
        expected_message = f"Did not find service AWS credentials at {os.path.join(creds_dir, test_service + '.yaml')}"
        assert expected_message in warning_msg

    def test_use_session(self, mock_session):
        assert spark_config.get_aws_credentials(session=mock_session) == self.expected_temp_creds

    def test_use_aws_credentials_json(self, tmpdir):
        fp = tmpdir.join('test.json')
        fp.write(json.dumps({'accessKeyId': self.access_key, 'secretAccessKey': self.secret_key}))
        assert spark_config.get_aws_credentials(aws_credentials_json=str(fp)) == self.expected_creds

    def test_use_profile(self, mock_session):
        assert spark_config.get_aws_credentials(profile_name='test_profile') == self.expected_temp_creds

    def test_fail(self, tmpdir):
        fp = tmpdir.join('test.yaml')
        fp.write('not yaml file')
        with pytest.raises(ValueError):
            spark_config.get_aws_credentials(aws_credentials_yaml=str(fp))


def test_pick_random_port():
    with mock.patch('ephemeral_port_reserve.reserve') as mock_reserve:
        port = spark_config._pick_random_port('test')
        (host, prefer_port), _ = mock_reserve.call_args
        assert host == '0.0.0.0'
        assert prefer_port >= 33000
        assert port == mock_reserve.return_value


class MockConfigFunction:

    def __init__(self, mock_func, return_value):
        self.return_value = return_value

        def side_effect(*args, **kwargs):
            return {**args[0], **self.return_value}
        self._patch = mock.patch.object(spark_config, mock_func, side_effect=side_effect)

    def __enter__(self):
        self.mocker = self._patch.__enter__()
        return self

    def __exit__(self, *args, **kwargs):
        self._patch.__exit__(*args, **kwargs)


class TestGetSparkConf:
    cluster = 'test-cluster'
    service = 'test-service'
    instance = 'test-instance'
    pool = 'test-pool'
    docker_image = 'docker-dev.yelp.com/test-image'
    executor_cores = '10'
    spark_app_base_name = 'test_app_base_name'
    default_mesos_leader = 'mesos://some-url.yelp.com:5050'
    aws_provider_key = 'spark.hadoop.fs.s3a.aws.credentials.provider'

    @pytest.fixture
    def base_volumes(self):
        return [{'hostPath': '/tmp', 'containerPath': '/tmp', 'mode': 'RO'}]

    @pytest.fixture
    def mock_paasta_volumes(self, monkeypatch, tmpdir):
        files = [f'paasta{i + 1}' for i in range(2)]
        volumes = [
            {'hostPath': f'/host/{f}', 'containerPath': f'/container/{f}', 'mode': 'RO'}
            for i, f in enumerate(files)
        ]
        fp = tmpdir.join('volumes.json')
        fp.write(json.dumps({'volumes': volumes}))
        monkeypatch.setattr(spark_config, 'DEFAULT_PAASTA_VOLUME_PATH', str(fp))
        return [f'/host/{f}:/container/{f}:ro' for f in files]

    @pytest.fixture
    def mock_existed_files(self, mock_paasta_volumes):
        existed_files = [v.split(':')[0] for v in mock_paasta_volumes] + [
            '/host/file1', '/host/file2', '/host/file3', '/etc/pki/spark', '/etc/group', '/etc/passwd',
        ]
        with mock.patch('os.path.exists', side_effect=lambda f: f in existed_files):
            yield existed_files

    @pytest.mark.parametrize(
        'original_volumes', [
            None,
            [
                '/host/file1:/containter/file1:ro',
                '/host/file2:/containter/file2:ro',
                '/host/paasta1:/container/paasta1:ro',
            ],
        ],
    )
    @pytest.mark.parametrize('load_paasta_volumes', [True, False])
    @pytest.mark.parametrize(
        'extra_volumes', [
            None,
            [
                {'hostPath': '/host/file1', 'containerPath': '/container/file1', 'mode': 'RO'},
                {'hostPath': '/host/file3', 'containerPath': '/container/file3', 'mode': 'RW'},
                {'hostPath': '/host/not_exist', 'containerPath': '/container/not_exist', 'mode': 'RW'},
            ],
        ],
    )
    def test_get_mesos_docker_volumes_conf(
        self,
        load_paasta_volumes,
        original_volumes,
        extra_volumes,
        mock_existed_files,
        mock_paasta_volumes,
    ):
        validate_key = 'spark.mesos.executor.docker.volumes'
        expected_volumes = [
            '/etc/passwd:/etc/passwd:ro', '/etc/group:/etc/group:ro',
        ]
        if load_paasta_volumes:
            expected_volumes.extend(mock_paasta_volumes)
        if original_volumes:
            expected_volumes.extend(original_volumes)
        if extra_volumes:
            expected_volumes.extend([
                f"{v['hostPath']}:{v['containerPath']}:{v['mode'].lower()}"
                for v in extra_volumes
                if v['hostPath'] in mock_existed_files
            ])

        spark_conf = {validate_key: ','.join(original_volumes)} if original_volumes else {}

        output = spark_config._get_mesos_docker_volumes_conf(
            spark_conf, extra_volumes, load_paasta_volumes,
        )
        assert sorted(output[validate_key].split(',')) == sorted(set(expected_volumes))

    def test_get_k8s_volume_hostpath_dict(self):
        assert spark_config._get_k8s_volume_hostpath_dict(
            '/host/file1', '/container/file1', 'RO', itertools.count(),
        ) == {
            'spark.kubernetes.executor.volumes.hostPath.0.mount.path': '/container/file1',
            'spark.kubernetes.executor.volumes.hostPath.0.options.path': '/host/file1',
            'spark.kubernetes.executor.volumes.hostPath.0.mount.readOnly': 'true',
        }

    @pytest.mark.parametrize(
        'volumes', [
            None,
            [
                {'hostPath': '/host/file1', 'containerPath': '/containter/file1', 'mode': 'RO'},
                {'hostPath': '/host/file2', 'containerPath': '/containter/file2', 'mode': 'RO'},
                {'hostPath': '/host/paasta1', 'containerPath': '/container/paasta1', 'mode': 'RO'},
            ],
        ],
    )
    @pytest.mark.usefixtures('mock_existed_files')
    def test_get_k8s_docker_volumes_conf(self, volumes):
        expected_volumes = {}

        _get_k8s_volume = functools.partial(spark_config._get_k8s_volume_hostpath_dict, count=itertools.count())
        if volumes:
            for volume in volumes:
                expected_volumes.update(
                    _get_k8s_volume(volume['hostPath'], volume['containerPath'], volume['mode']),
                )

        expected_volumes.update({
            **_get_k8s_volume('/etc/pki/spark', '/etc/pki/spark', 'ro'),
            **_get_k8s_volume('/etc/passwd', '/etc/passwd', 'ro'),
            **_get_k8s_volume('/etc/group', '/etc/group', 'ro'),
        })

        output = spark_config._get_k8s_docker_volumes_conf(volumes)
        assert output == expected_volumes

    @pytest.fixture
    def mock_account_id(self, tmpdir, monkeypatch):
        def get_client(service_name, **kwargs):
            if (
                kwargs.get('aws_access_key_id') != 'prod_key' or
                kwargs.get('aws_secret_access_key') != 'prod_secret'
            ):
                raise Exception('Unknown key')

            mock_client = mock.Mock()
            mock_get = mock_client.get_caller_identity().get
            mock_get.side_effect = lambda x: TEST_ACCOUNT_ID if x == 'Account' else None
            return mock_client

        with mock.patch.object(spark_config.boto3, 'client', side_effect=get_client):
            yield

    @pytest.fixture
    def gpu_pool(self, tmpdir, monkeypatch):
        pools_def = {
            'test_gpu_pool': {
                'gpus_per_instance': 1,
                'cpus_per_instance': 8,
                'memory_per_instance': 1024,
            },
        }
        fp = tmpdir.join('gpu_pools.yaml')
        fp.write(yaml.dump(pools_def))
        monkeypatch.setattr(spark_config, 'GPU_POOLS_YAML_FILE_PATH', str(fp))
        return pools_def

    @pytest.mark.parametrize(
        'test_name,cluster_manager,user_spark_opts,expected_output,force_spark_resource_configs', [
            (
                'k8s allocation batch size not specified',
                'kubernetes',
                {
                    'spark.executor.cores': '4',
                    'spark.cores.max': '128',
                },
                {
                    'spark.kubernetes.allocation.batch.size': '512',
                },
                False,
            ),
            (
                'k8s allocation batch size specified',
                'kubernetes',
                {
                    'spark.executor.cores': '4',
                    'spark.cores.max': '128',
                    'spark.kubernetes.allocation.batch.size': '151',
                },
                {
                    'spark.kubernetes.allocation.batch.size': '151',
                },
                False,
            ),
            (
                'use default k8s settings',
                'kubernetes',
                {},
                {
                    'spark.executor.memory': '28g',
                    'spark.executor.cores': '4',
                    'spark.kubernetes.executor.limit.cores': '4',
                    'spark.executor.instances': '2',
                    'spark.kubernetes.allocation.batch.size': '512',
                    'spark.scheduler.maxRegisteredResourcesWaitingTime': '15min',
                },
                False,
            ),
            (
                'user defined resources with k8s',
                'kubernetes',
                {
                    'spark.executor.cores': '2',
                    'spark.executor.instances': '600',
                },
                {
                    'spark.executor.memory': '28g',
                    'spark.executor.cores': '4',  # adjusted
                    'spark.kubernetes.executor.limit.cores': '4',
                    'spark.executor.instances': '600',
                    'spark.kubernetes.allocation.batch.size': '512',
                    'spark.scheduler.maxRegisteredResourcesWaitingTime': '35min',
                },
                False,
            ),
            (
                'kubernetes migration',
                'kubernetes',
                {
                    'spark.executor.memory': '2g',
                    'spark.executor.cores': '4',
                    'spark.cores.max': '12',
                    'spark.mesos.executor.memoryOverhead': '4096',
                },
                {
                    'spark.executor.memory': '7g',
                    'spark.executor.cores': '1',  # adjusted
                    'spark.kubernetes.executor.limit.cores': '1',
                    'spark.executor.instances': '1',
                    'spark.cores.max': '12',
                    'spark.kubernetes.allocation.batch.size': '512',
                    'spark.scheduler.maxRegisteredResourcesWaitingTime': '15min',
                    'spark.executor.memoryOverhead': '4096',
                    'spark.mesos.executor.memoryOverhead': '4096',
                },
                False,
            ),
            (
                'use default mesos settings',
                'mesos',
                {},
                {
                    'spark.executor.memory': '28g',
                    'spark.executor.cores': '4',
                    'spark.cores.max': '8',
                },
                False,
            ),
            (
                'user defined resources',
                'mesos',
                {
                    'spark.executor.memory': '2g',
                    'spark.executor.cores': '4',
                    'spark.cores.max': '12',
                },
                {
                    'spark.executor.memory': '7g',
                    'spark.executor.cores': '1',
                    'spark.cores.max': '12',
                },
                False,
            ),
            (
                'user defined resources - capped cpu & memory',
                'mesos',
                {
                    'spark.executor.cores': '13',
                    'spark.executor.memory': '112g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '12',
                    'spark.executor.memory': '110g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',
                },
                False,
            ),
            (
                'user defined resources - recalculated - medium memory',
                'mesos',
                {
                    'spark.executor.cores': '10',
                    'spark.executor.memory': '60g',
                    'spark.executor.instances': '1',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '8',
                    'spark.executor.memory': '56g',
                    'spark.executor.instances': '1',
                    'spark.task.cpus': '1',
                    'spark.cores.max': '32',
                },
                False,
            ),
            (
                'user defined resources - recalculated - medium memory',
                'mesos',
                {
                    'spark.executor.cores': '6',
                    'spark.executor.memory': '60g',
                    'spark.executor.instances': '1',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '8',
                    'spark.executor.memory': '56g',
                    'spark.executor.instances': '1',
                    'spark.task.cpus': '1',
                    'spark.cores.max': '32',
                },
                False,
            ),
            (
                'user defined resources - recalculated - recommended memory',
                'mesos',
                {
                    'spark.executor.cores': '4',
                    'spark.executor.memory': '32g',
                    'spark.executor.instances': '8',
                    'spark.cores.max': '32',
                },
                {
                    'spark.executor.cores': '4',
                    'spark.kubernetes.executor.limit.cores': '4',
                    'spark.executor.memory': '28g',
                    'spark.executor.instances': '9',
                    'spark.task.cpus': '1',
                    'spark.cores.max': '36',
                },
                False,
            ),
            (
                'user defined resources - recalculated - non standard memory',
                'mesos',
                {
                    'spark.executor.cores': '6',
                    'spark.executor.memory': '13g',
                    'spark.executor.instances': '1',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '1',
                    'spark.kubernetes.executor.limit.cores': '1',
                    'spark.executor.memory': '7g',
                    'spark.executor.instances': '1',
                    'spark.task.cpus': '1',
                    'spark.cores.max': '32',
                },
                False,
            ),
            (
                'user defined resources - recalculated - non standard memory - task cpus capped',
                'mesos',
                {
                    'spark.executor.cores': '6',
                    'spark.executor.memory': '13g',
                    'spark.executor.instances': '1',
                    'spark.task.cpus': '4',
                    'spark.cores.max': '32',
                },
                {
                    'spark.executor.cores': '1',
                    'spark.kubernetes.executor.limit.cores': '1',
                    'spark.executor.memory': '7g',
                    'spark.executor.instances': '1',
                    'spark.task.cpus': '1',
                    'spark.cores.max': '32',
                },
                False,
            ),
            (
                'user defined resources - force-spark-resource-configs - capped',
                'mesos',
                {
                    'spark.executor.cores': '13',
                    'spark.executor.memory': '112g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '12',
                    'spark.kubernetes.executor.limit.cores': '12',
                    'spark.executor.memory': '110g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',
                },
                True,
            ),
            (
                'user defined resources - force-spark-resource-configs - not capped',
                'mesos',
                {
                    'spark.executor.cores': '10',
                    'spark.executor.memory': '100g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',

                },
                {
                    'spark.executor.cores': '10',
                    'spark.kubernetes.executor.limit.cores': '10',
                    'spark.executor.memory': '100g',
                    'spark.executor.instances': '2',
                    'spark.cores.max': '32',
                },
                True,
            ),
            (
                'gpu with default settings',
                'mesos',
                {'spark.mesos.gpus.max': '2'},
                {
                    'spark.mesos.gpus.max': '2',
                    'spark.mesos.containerizer': 'mesos',
                    'spark.default.parallelism': '2',
                    'spark.task.cpus': '4',
                    'spark.executor.cores': '4',
                    'spark.kubernetes.executor.limit.cores': '4',
                    'spark.executor.memory': '28g',
                    'spark.cores.max': '16',
                },
                False,
            ),
            (
                'Gpu with user defined resources',
                'mesos',
                {
                    'spark.mesos.gpus.max': '2',
                    'spark.task.cpus': '2',
                    'spark.executor.cores': '4',
                },
                {
                    'spark.mesos.gpus.max': '2',
                    'spark.task.cpus': '2',
                    'spark.executor.cores': '4',
                    'spark.kubernetes.executor.limit.cores': '4',
                    'spark.cores.max': '8',
                },
                False,
            ),
        ],
    )
    def test_adjust_spark_requested_resources(
        self,
        test_name,
        cluster_manager,
        user_spark_opts,
        expected_output,
        force_spark_resource_configs,
        gpu_pool,
    ):
        ratio_adj_thresh = sys.maxsize
        pool = (
            'test-batch-pool'
            if user_spark_opts.get('spark.mesos.gpus.max', '0') == '0'
            else next(iter(gpu_pool.keys()))
        )

        output = spark_config._adjust_spark_requested_resources(
            user_spark_opts, cluster_manager, pool, force_spark_resource_configs, ratio_adj_thresh,
        )
        for key in expected_output.keys():
            err_msg = f'[{test_name}] wrong value for {key}, expected_output={expected_output}'
            assert output[key] == expected_output[key], err_msg

    @pytest.mark.parametrize(
        'cluster_manager,spark_opts,pool', [
            # max_cores < executor_core
            ('mesos', {'spark.cores.max': '10', 'spark.executor.cores': '20'}, pool),
            # use gpu with kubernetes
            ('kubernetes', {'spark.mesos.gpus.max': '10'}, pool),
            # gpu over limit
            ('mesos', {'spark.mesos.gpus.max': str(spark_config.GPUS_HARD_LIMIT + 1)}, pool),
            # pool not found
            ('mesos', {'spark.mesos.gpus.max': '2'}, 'not_exist_pool'),
        ],
    )
    def test_adjust_spark_requested_resources_error(
        self,
        cluster_manager,
        spark_opts,
        pool,
        gpu_pool,
    ):
        with pytest.raises(ValueError):
            spark_config._adjust_spark_requested_resources(spark_opts, cluster_manager, pool)

    @pytest.mark.parametrize(
        'user_spark_opts,expected_output', [
            # dynamic resource allocation enabled
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                },
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
                    'spark.dynamicAllocation.executorAllocationRatio': '0.8',
                    'spark.dynamicAllocation.cachedExecutorIdleTimeout': '900s',
                    'spark.dynamicAllocation.minExecutors': '0',
                    'spark.dynamicAllocation.maxExecutors': '2',
                    'spark.executor.instances': '0',
                },
            ),
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': '512',
                    'spark.dynamicAllocation.minExecutors': '128',
                    'spark.dynamicAllocation.initialExecutors': '128',
                },
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': '512',
                    'spark.dynamicAllocation.minExecutors': '128',
                    'spark.dynamicAllocation.initialExecutors': '128',
                    'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
                    'spark.dynamicAllocation.executorAllocationRatio': '0.8',
                    'spark.dynamicAllocation.cachedExecutorIdleTimeout': '900s',
                    'spark.executor.instances': '128',
                },
            ),
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.executor.instances': '821',
                },
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': '821',
                    'spark.dynamicAllocation.minExecutors': '205',
                    'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
                    'spark.dynamicAllocation.executorAllocationRatio': '0.8',
                    'spark.dynamicAllocation.cachedExecutorIdleTimeout': '900s',
                    'spark.executor.instances': '205',
                },
            ),
            # dynamic resource allocation disabled explicitly
            (
                {
                    'spark.dynamicAllocation.enabled': 'false',
                    'spark.executor.instances': '600',
                },
                {
                    'spark.dynamicAllocation.enabled': 'false',
                    'spark.executor.instances': '600',
                },
            ),
            # dynamic resource allocation not specified
            (
                {
                    'spark.executor.instances': '606',
                },
                {
                    'spark.executor.instances': '151',  # enabled by default, 606/4
                },
            ),
        ],
    )
    def test_get_dra_configs(
            self,
            user_spark_opts,
            expected_output,
    ):
        output = spark_config.get_dra_configs(user_spark_opts)
        for key in expected_output.keys():
            assert output[key] == expected_output[key], f'wrong value for {key}'

    @pytest.mark.parametrize(
        'user_spark_opts,aws_creds,expected_output', [
            # user specified to disable
            (
                {'spark.eventLog.enabled': 'false'},
                (None, None, None),
                {'spark.eventLog.enabled': 'false'},
            ),
            # user specified their own bucket
            (
                {
                    'spark.eventLog.enabled': 'true',
                    'spark.eventLog.dir': 's3a://other/bucket',
                },
                (None, None, None),
                {
                    'spark.eventLog.enabled': 'true',
                    'spark.eventLog.dir': 's3a://other/bucket',
                },
            ),
            # use predefined bucket
            (
                {},
                ('prod_key', 'prod_secret', None),
                {
                    'spark.eventLog.enabled': 'true',
                    'spark.eventLog.dir': 's3a://test/eventlog',
                },
            ),
            # no predefined bucket available
            (
                {},
                ('different_key', 'different_secret', 'different'),
                {'spark.eventLog.enabled': 'false'},
            ),
        ],
    )
    def test_append_event_log_conf(
        self,
        mock_spark_run_conf,
        mock_account_id,
        user_spark_opts,
        aws_creds,
        expected_output,
    ):
        output = spark_config._append_event_log_conf(user_spark_opts, *aws_creds)
        for key in expected_output:
            assert output[key] == expected_output[key]

    @pytest.mark.parametrize(
        'user_spark_opts,expected_output', [
            # mesos
            ({'spark.cores.max': '10'}, '30'),
            # k8s
            ({'spark.executor.instances': '10', 'spark.executor.cores': '3'}, '90'),
            # user defined
            ({'spark.sql.shuffle.partitions': '300'}, ['300', '128', '128']),
            # dynamic resource allocation enabled, both maxExecutors and max cores defined
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': '128',
                    'spark.executor.cores': '3',
                    'spark.cores.max': '10',
                },
                '384',  # max (3 * (max cores), (maxExecutors * executor cores))
            ),
            # dynamic resource allocation enabled maxExecutors not defined, max cores defined
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.executor.cores': '3',
                    'spark.cores.max': '10',
                },
                '30',  # 2 * max cores
            ),
            # dynamic resource allocation enabled maxExecutors not defined, max cores not defined
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.executor.cores': '3',
                },
                '128',  # DEFAULT_SQL_SHUFFLE_PARTITIONS
            ),
            # dynamic resource allocation enabled maxExecutors infinity
            (
                {
                    'spark.dynamicAllocation.enabled': 'true',
                    'spark.dynamicAllocation.maxExecutors': 'infinity',
                    'spark.executor.cores': '3',
                    'spark.cores.max': '10',
                },
                '30',  # 2 * max cores
            ),
        ],
    )
    def test_append_sql_partitions_conf(
        self, user_spark_opts, expected_output,
    ):
        output = spark_config._append_sql_partitions_conf(user_spark_opts)
        keys = [
            'spark.sql.shuffle.partitions',
            'spark.sql.files.minPartitionNum',
            'spark.default.parallelism',
        ]
        if isinstance(expected_output, str):
            expected_output = [expected_output] * 3
        for key, expected in zip(keys, expected_output):
            assert output[key] == expected

    @pytest.mark.parametrize(
        'user_spark_opts,expected_output', [
            # not configured by user
            ({}, 'true'),
            # configured by user
            ({'spark.logConf': 'false'}, 'false'),
        ],
    )
    def test_append_spark_conf_log(
            self, user_spark_opts, expected_output,
    ):
        key = 'spark.logConf'
        output = spark_config._append_spark_config(user_spark_opts, key, 'true')

        assert output[key] == expected_output

    @pytest.mark.parametrize(
        'user_spark_opts,expected_output', [
            # not configured by user
            ({}, 'true'),
            # configured by user
            ({'spark.ui.showConsoleProgress': 'false'}, 'false'),
        ],
    )
    def test_append_console_progress_conf(
            self, user_spark_opts, expected_output,
    ):
        key = 'spark.ui.showConsoleProgress'
        output = spark_config._append_spark_config(user_spark_opts, key, 'true')

        assert output[key] == expected_output

    def test_append_aws_credentials_conf(self):
        output = spark_config._append_aws_credentials_conf(
            {},
            mock.sentinel.access,
            mock.sentinel.secret,
            mock.sentinel.token,
        )
        assert output['spark.executorEnv.AWS_ACCESS_KEY_ID'] == mock.sentinel.access
        assert output['spark.executorEnv.AWS_SECRET_ACCESS_KEY'] == mock.sentinel.secret
        assert output['spark.executorEnv.AWS_SESSION_TOKEN'] == mock.sentinel.token

    @pytest.fixture
    def mock_append_spark_conf_log(self):
        return_value = {'spark.logConf': 'true'}
        with MockConfigFunction(
                '_append_spark_config', return_value,
        ) as m:
            yield m

    @pytest.fixture
    def mock_append_console_progress_conf(self):
        return_value = {'spark.ui.showConsoleProgress': 'true'}
        with MockConfigFunction(
                '_append_spark_config', return_value,
        ) as m:
            yield m

    @pytest.fixture
    def mock_get_mesos_docker_volumes_conf(self):
        return_value = {'spark.mesos.executor.docker.volumes': '/tmp:/tmp:ro'}
        with MockConfigFunction('_get_mesos_docker_volumes_conf', return_value) as m:
            yield m

    @pytest.fixture
    def mock_append_sql_partitions_conf(self):
        keys = [
            'spark.sql.shuffle.partitions',
            'spark.sql.files.minPartitionNum',
            'spark.default.parallelism',
        ]
        return_value = {k: '10' for k in keys}
        with MockConfigFunction(
            '_append_sql_partitions_conf', return_value,
        ) as m:
            yield m

    @pytest.fixture
    def mock_append_event_log_conf(self):
        return_value = {
            'spark.eventLog.enabled': 'true',
            'spark.eventLog.dir': 's3a://test/bucket/',
        }
        with MockConfigFunction('_append_event_log_conf', return_value) as m:
            yield m

    @pytest.fixture
    def mock_append_aws_credentials_conf(self):
        return_value = {
            'spark.executorEnv.AWS_ACCESS_KEY_ID': 'my_key',
            'spark.executorEnv.AWS_SECRET_ACCESS_KEY': 'your_key',
            'spark.executorEnv.AWS_SESSION_TOKEN': 'we_all_key',
            'spark.executorEnv.AWS_DEFAULT_REGION': 'ice_cream',
        }
        with MockConfigFunction('_append_aws_credentials_conf', return_value) as m:
            yield m

    @pytest.fixture
    def mock_adjust_spark_requested_resources_mesos(self):
        return_value = {
            'spark.cores.max': '10',
            'spark.executor.cores': self.executor_cores,
            'spark.executor.memory': '2g',
        }
        with MockConfigFunction('_adjust_spark_requested_resources', return_value) as m:
            yield m

    @pytest.fixture
    def mock_adjust_spark_requested_resources_kubernetes(self):
        return_value = {
            'spark.cores.instances': '2',
            'spark.executor.cores': self.executor_cores,
            'spark.executor.memory': '2g',
        }
        with MockConfigFunction('_adjust_spark_requested_resources', return_value) as m:
            yield m

    @pytest.fixture
    def mock_get_dra_configs(self):
        return_value = {
            'spark.dynamicAllocation.enabled': 'true',
            'spark.dynamicAllocation.maxExecutors': '2',
            'spark.dynamicAllocation.shuffleTracking.enabled': 'true',
            'spark.dynamicAllocation.executorAllocationRatio': '0.8',
            'spark.executor.instances': '2',
            'spark.dynamicAllocation.minExecutors': '0',
            'spark.dynamicAllocation.cachedExecutorIdleTimeout': '900s',
        }
        with MockConfigFunction('get_dra_configs', return_value) as m:
            yield m

    @pytest.fixture
    def mock_secret(self, tmpdir, monkeypatch):
        secret = 'secret'
        fp = tmpdir.join('mesos_secret')
        fp.write(secret)
        monkeypatch.setattr(spark_config, 'DEFAULT_SPARK_MESOS_SECRET_FILE', str(fp))
        return secret

    @pytest.fixture(params=[False, True])
    def with_secret(self, request):
        return request.param

    @pytest.fixture
    def assert_mesos_secret(self, with_secret, mock_secret):
        expected_output = mock_secret if with_secret else None

        def verify(output):
            if expected_output:
                key = 'spark.mesos.secret'
                assert output[key] == mock_secret
                return [key]
            return []
        return verify

    @pytest.fixture
    def mock_request_mesos_leader(self):
        return_value = self.default_mesos_leader.replace('mesos://', 'http://') + '/#/'
        with mock.patch.object(spark_config.requests, 'get') as m:
            m.return_value = mock.Mock(url=return_value)
            yield m

    def test_find_spark_master(self, mock_request_mesos_leader):
        assert spark_config.find_spark_master('test-cluster') == 'mesos://some-url.yelp.com:5050'

    def test_find_spark_master_error(self, mock_request_mesos_leader):
        mock_request_mesos_leader.side_effect = requests.RequestException()
        with pytest.raises(ValueError):
            spark_config.find_spark_master('test-cluster')

    def test_convert_user_spark_opts_value_str(self):
        spark_conf = {
            'spark.executor.memory': '4g',
            'spark.executor.cores': 2,
            'spark.eventLog.enabled': False,
        }
        assert spark_config._convert_user_spark_opts_value_to_str(spark_conf) == {
            'spark.executor.memory': '4g',
            'spark.executor.cores': '2',
            'spark.eventLog.enabled': 'false',
        }

    @pytest.fixture(params=[None, 'test-mesos:5050'])
    def mesos_leader(self, request):
        return request.param

    @pytest.fixture
    def assert_mesos_leader(self, mesos_leader, mock_request_mesos_leader):
        expected_output = f'mesos://{mesos_leader}' if mesos_leader else self.default_mesos_leader

        def validate(output):
            key = 'spark.master'
            assert output[key] == expected_output
            return [key]

        return validate

    @pytest.fixture(params=[None, {'workdir': '/tmp'}])
    def extra_docker_params(self, request):
        return request.param

    @pytest.fixture
    def assert_docker_parameters(self, extra_docker_params):
        def verify(output):
            expected_output = (
                f'cpus={self.executor_cores},'
                f'label=paasta_service={self.service},'
                f'label=paasta_instance={self.instance}'
            )
            if extra_docker_params:
                for key, value in extra_docker_params.items():
                    expected_output += f',{key}={value}'

            key = 'spark.mesos.executor.docker.parameters'
            assert output[key] == expected_output
            return [key]
        return verify

    @pytest.fixture(params=[False, True])
    def needs_docker_cfg(self, request):
        return request.param

    @pytest.fixture
    def assert_docker_cfg(self, needs_docker_cfg):
        def verify(output):
            if needs_docker_cfg:
                key = 'spark.mesos.uris'
                assert output[key] == 'file:///root/.dockercfg'
                return [key]
            return []
        return verify

    @pytest.fixture
    def mock_pick_random_port(self):
        port = '12345'
        with mock.patch.object(spark_config, '_pick_random_port', return_value=port):
            yield port

    @pytest.fixture(params=[None, '23456'])
    def ui_port(self, request, mock_pick_random_port):
        return request.param

    @pytest.fixture(params=[None, 'test_app_name_from_env'])
    def spark_opts_from_env(self, request, ui_port):
        spark_opts = {}
        if ui_port:
            spark_opts['spark.ui.port'] = ui_port
        if request.param:
            spark_opts['spark.app.name'] = request.param
        return spark_opts or None

    @pytest.fixture
    def assert_ui_port(self, spark_opts_from_env, ui_port, mock_pick_random_port):
        expected_output = ui_port if ui_port else mock_pick_random_port

        def verify(output):
            key = 'spark.ui.port'
            assert output[key] == expected_output
            return [key]
        return verify

    @pytest.fixture(params=[None, {'spark.app.name': 'app_base_name_from_spark_opts'}])
    def user_spark_opts(self, request):
        return request.param

    @pytest.fixture
    def assert_app_name(self, spark_opts_from_env, user_spark_opts, ui_port, mock_pick_random_port):
        expected_output = (spark_opts_from_env or {}).get('spark.app.name')
        if not expected_output:
            expected_output = (
                (user_spark_opts or {}).get('spark.app.name') or
                self.spark_app_base_name
            ) + '_' + (ui_port or mock_pick_random_port) + '_123'

        def verify(output):
            key = 'spark.app.name'
            assert output[key] == expected_output
            return [key]
        return verify

    @pytest.fixture
    def assert_mesos_conf(self):
        def verify(output):
            expected_output = {
                'spark.executorEnv.PAASTA_SERVICE': self.service,
                'spark.executorEnv.PAASTA_INSTANCE': self.instance,
                'spark.executorEnv.PAASTA_CLUSTER': self.cluster,
                'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
                'spark.executorEnv.SPARK_USER': 'root',
                'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
                'spark.mesos.executor.docker.image': self.docker_image,
                'spark.mesos.executor.docker.forcePullImage': 'true',
                'spark.mesos.constraints': f'pool:{self.pool}',
                'spark.mesos.principal': 'spark',
                'spark.shuffle.useOldFetchProtocol': 'true',
            }
            for key, value in expected_output.items():
                assert output[key] == value
            return list(expected_output.keys())

        return verify

    def test_get_spark_conf_mesos(
        self,
        user_spark_opts,
        spark_opts_from_env,
        base_volumes,
        ui_port,
        with_secret,
        mesos_leader,
        needs_docker_cfg,
        extra_docker_params,
        mock_get_mesos_docker_volumes_conf,
        mock_append_event_log_conf,
        mock_append_aws_credentials_conf,
        mock_append_sql_partitions_conf,
        mock_adjust_spark_requested_resources_mesos,
        mock_time,
        assert_mesos_leader,
        assert_docker_parameters,
        assert_mesos_secret,
        assert_docker_cfg,
        assert_mesos_conf,
        assert_ui_port,
        assert_app_name,
        mock_log,
        mock_append_spark_conf_log,
        mock_append_console_progress_conf,
    ):
        other_spark_opts = {'spark.driver.memory': '2g', 'spark.executor.memoryOverhead': '1024'}
        not_allowed_opts = {'spark.executorEnv.PAASTA_SERVICE': 'random-service'}
        user_spark_opts = {
            **(user_spark_opts or {}),
            **not_allowed_opts,
            **other_spark_opts,
        }

        aws_creds = (None, None, None)
        aws_region = 'ice_cream'

        output = spark_config.get_spark_conf(
            cluster_manager='mesos',
            spark_app_base_name=self.spark_app_base_name,
            user_spark_opts=user_spark_opts,
            paasta_cluster=self.cluster,
            paasta_pool=self.pool,
            paasta_service=self.service,
            paasta_instance=self.instance,
            docker_img=self.docker_image,
            extra_volumes=base_volumes,
            aws_creds=aws_creds,
            extra_docker_params=extra_docker_params,
            with_secret=with_secret,
            needs_docker_cfg=needs_docker_cfg,
            mesos_leader=mesos_leader,
            spark_opts_from_env=spark_opts_from_env,
            load_paasta_default_volumes=True,
            aws_region=aws_region,
            force_spark_resource_configs=False,
        )

        verified_keys = set(
            assert_mesos_leader(output) +
            assert_docker_parameters(output) +
            assert_mesos_secret(output) +
            assert_docker_cfg(output) +
            assert_mesos_conf(output) +
            assert_ui_port(output) +
            assert_app_name(output) +
            list(other_spark_opts.keys()) +
            list(mock_get_mesos_docker_volumes_conf.return_value.keys()) +
            list(mock_adjust_spark_requested_resources_mesos.return_value.keys()) +
            list(mock_append_event_log_conf.return_value.keys()) +
            list(mock_append_aws_credentials_conf.return_value.keys()) +
            list(mock_append_sql_partitions_conf.return_value.keys()) +
            list(mock_append_spark_conf_log.return_value.keys()) +
            list(mock_append_console_progress_conf.return_value.keys()),
        )
        assert len(set(output.keys()) - verified_keys) == 0
        mock_get_mesos_docker_volumes_conf.mocker.assert_called_once_with(
            mock.ANY, base_volumes, True,
        )
        mock_adjust_spark_requested_resources_mesos.mocker.assert_called_once_with(
            mock.ANY, 'mesos', self.pool, False,
        )
        mock_append_event_log_conf.mocker.assert_called_once_with(
            mock.ANY, *aws_creds,
        )
        mock_append_aws_credentials_conf.mocker.assert_called_once_with(mock.ANY, *aws_creds, aws_region)
        mock_append_sql_partitions_conf.mocker.assert_called_once_with(
            mock.ANY,
        )
        (warning_msg,), _ = mock_log.warning.call_args
        assert next(iter(not_allowed_opts.keys())) in warning_msg

    def _get_k8s_base_volumes(self):
        """Helper needed to allow tests to pass in github CI checks."""
        return [
            volume for volume in spark_config.K8S_BASE_VOLUMES
            if os.path.exists(volume['containerPath'])
        ]

    @pytest.fixture
    def assert_kubernetes_conf(self, base_volumes):
        expected_output = {
            'spark.master': f'k8s://https://k8s.{self.cluster}.paasta:6443',
            'spark.executorEnv.PAASTA_SERVICE': self.service,
            'spark.executorEnv.PAASTA_INSTANCE': self.instance,
            'spark.executorEnv.PAASTA_CLUSTER': self.cluster,
            'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
            'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
            'spark.kubernetes.pyspark.pythonVersion': '3',
            'spark.kubernetes.container.image': self.docker_image,
            'spark.kubernetes.namespace': 'paasta-spark',
            'spark.kubernetes.authenticate.caCertFile': f'{spark_config.K8S_AUTH_FOLDER}/{self.cluster}-ca.crt',
            'spark.kubernetes.authenticate.clientKeyFile': f'{spark_config.K8S_AUTH_FOLDER}/{self.cluster}-client.key',
            'spark.kubernetes.authenticate.clientCertFile': (
                f'{spark_config.K8S_AUTH_FOLDER}/{self.cluster}-client.crt'
            ),
            'spark.kubernetes.container.image.pullPolicy': 'Always',
            'spark.kubernetes.executor.label.yelp.com/paasta_service': self.service,
            'spark.kubernetes.executor.label.yelp.com/paasta_instance': self.instance,
            'spark.kubernetes.executor.label.yelp.com/paasta_cluster': self.cluster,
            'spark.kubernetes.executor.label.paasta.yelp.com/service': self.service,
            'spark.kubernetes.executor.label.paasta.yelp.com/instance': self.instance,
            'spark.kubernetes.executor.label.paasta.yelp.com/cluster': self.cluster,
            'spark.kubernetes.node.selector.yelp.com/pool': self.pool,
            'spark.kubernetes.executor.label.yelp.com/pool': self.pool,
            'spark.kubernetes.executor.label.paasta.yelp.com/pool': self.pool,
            'spark.kubernetes.executor.label.yelp.com/owner': 'core_ml',
            'spark.logConf': 'true',
            'spark.ui.showConsoleProgress': 'true',
        }
        for i, volume in enumerate(base_volumes + self._get_k8s_base_volumes()):
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.mount.path'] = volume['containerPath']
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.mount.readOnly'] = str(
                volume['mode'] == 'RO',
            ).lower()
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.options.path'] = volume['hostPath']

        def verify(output):
            for key, value in expected_output.items():
                assert output[key] == value
            return list(expected_output.keys())
        return verify

    def test_leaders_get_spark_conf_kubernetes(
        self,
        user_spark_opts,
        spark_opts_from_env,
        ui_port,
        base_volumes,
        mock_append_event_log_conf,
        mock_append_aws_credentials_conf,
        mock_append_sql_partitions_conf,
        mock_adjust_spark_requested_resources_kubernetes,
        mock_get_dra_configs,
        mock_time,
        assert_ui_port,
        assert_app_name,
        assert_kubernetes_conf,
        mock_log,
    ):
        other_spark_opts = {'spark.driver.memory': '2g', 'spark.executor.memoryOverhead': '1024'}
        user_spark_opts = {
            **(user_spark_opts or {}),
            **other_spark_opts,
        }

        aws_creds = (None, None, None)
        aws_region = 'ice_cream'

        output = spark_config.get_spark_conf(
            cluster_manager='kubernetes',
            spark_app_base_name=self.spark_app_base_name,
            user_spark_opts=user_spark_opts,
            paasta_cluster=self.cluster,
            paasta_pool=self.pool,
            paasta_service=self.service,
            paasta_instance=self.instance,
            docker_img=self.docker_image,
            extra_volumes=base_volumes,
            aws_creds=aws_creds,
            spark_opts_from_env=spark_opts_from_env,
            aws_region=aws_region,
            force_spark_resource_configs=False,
        )

        verified_keys = set(
            assert_ui_port(output) +
            assert_app_name(output) +
            assert_kubernetes_conf(output) +
            list(other_spark_opts.keys()) +
            list(mock_adjust_spark_requested_resources_kubernetes.return_value.keys()) +
            list(mock_get_dra_configs.return_value.keys()) +
            list(mock_append_event_log_conf.return_value.keys()) +
            list(mock_append_aws_credentials_conf.return_value.keys()) +
            list(mock_append_sql_partitions_conf.return_value.keys()),
        )
        assert set(output.keys()) == verified_keys
        mock_adjust_spark_requested_resources_kubernetes.mocker.assert_called_once_with(
            mock.ANY, 'kubernetes', self.pool, False,
        )
        mock_append_event_log_conf.mocker.assert_called_once_with(
            mock.ANY, *aws_creds,
        )
        mock_append_aws_credentials_conf.mocker.assert_called_once_with(mock.ANY, *aws_creds, aws_region)
        mock_append_sql_partitions_conf.mocker.assert_called_once_with(
            mock.ANY,
        )

    @pytest.fixture
    def assert_local_conf(self, base_volumes):
        expected_output = {
            'spark.master': 'local[4]',
            'spark.executorEnv.PAASTA_SERVICE': self.service,
            'spark.executorEnv.PAASTA_INSTANCE': self.instance,
            'spark.executorEnv.PAASTA_CLUSTER': self.cluster,
            'spark.executorEnv.PAASTA_INSTANCE_TYPE': 'spark',
            'spark.executorEnv.SPARK_EXECUTOR_DIRS': '/tmp',
            'spark.logConf': 'true',
            'spark.ui.showConsoleProgress': 'true',
        }
        for i, volume in enumerate(base_volumes + self._get_k8s_base_volumes()):
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.mount.path'] = volume['containerPath']
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.mount.readOnly'] = str(
                volume['mode'] == 'RO',
            ).lower()
            expected_output[f'spark.kubernetes.executor.volumes.hostPath.{i}.options.path'] = volume['hostPath']

        def verify(output):
            for key, value in expected_output.items():
                assert output[key] == value
            return list(expected_output.keys())
        return verify

    def test_local_spark(
        self,
        user_spark_opts,
        spark_opts_from_env,
        ui_port,
        base_volumes,
        mock_append_event_log_conf,
        mock_append_aws_credentials_conf,
        mock_append_sql_partitions_conf,
        mock_adjust_spark_requested_resources_kubernetes,
        mock_get_dra_configs,
        mock_time,
        assert_ui_port,
        assert_app_name,
        assert_local_conf,
        mock_log,
    ):
        aws_creds = (None, None, None)
        aws_region = 'ice_cream'
        output = spark_config.get_spark_conf(
            cluster_manager='local',
            spark_app_base_name=self.spark_app_base_name,
            user_spark_opts=user_spark_opts or {},
            paasta_cluster=self.cluster,
            paasta_pool=self.pool,
            paasta_service=self.service,
            paasta_instance=self.instance,
            docker_img=self.docker_image,
            extra_volumes=base_volumes,
            aws_creds=aws_creds,
            spark_opts_from_env=spark_opts_from_env,
            aws_region=aws_region,
            force_spark_resource_configs=False,
        )
        verified_keys = set(
            assert_ui_port(output) +
            assert_app_name(output) +
            assert_local_conf(output) +
            list(mock_append_event_log_conf.return_value.keys()) +
            list(mock_adjust_spark_requested_resources_kubernetes.return_value.keys()) +
            list(mock_get_dra_configs.return_value.keys()) +
            list(mock_append_aws_credentials_conf.return_value.keys()) +
            list(mock_append_sql_partitions_conf.return_value.keys()),
        )
        assert set(output.keys()) == verified_keys
        mock_append_event_log_conf.mocker.assert_called_once_with(
            mock.ANY, *aws_creds,
        )
        mock_adjust_spark_requested_resources_kubernetes.mocker.assert_called_once_with(
            mock.ANY, 'local', self.pool, False,
        )
        mock_append_aws_credentials_conf.mocker.assert_called_once_with(mock.ANY, *aws_creds, aws_region)
        mock_append_sql_partitions_conf.mocker.assert_called_once_with(
            mock.ANY,
        )

    @pytest.mark.parametrize('reason', ['mesos_leader', 'mesos_secret'])
    def test_get_spark_conf_mesos_error(self, reason, monkeypatch, mock_request_mesos_leader):
        if reason == 'mesos_leader':
            mock_request_mesos_leader.side_effect = spark_config.requests.RequestException()
        else:
            monkeypatch.setattr(spark_config, 'DEFAULT_SPARK_MESOS_SECRET_FILE', '/not_exist')
        with pytest.raises(ValueError):
            spark_config.get_spark_conf(
                cluster_manager='mesos',
                spark_app_base_name=self.spark_app_base_name,
                user_spark_opts={},
                paasta_cluster=self.cluster,
                paasta_pool=self.pool,
                paasta_service=self.service,
                paasta_instance=self.instance,
                docker_img=self.docker_image,
                aws_creds=(None, None, None),
                extra_volumes=[],
                force_spark_resource_configs=False,
            )


def test_stringify_spark_env():
    conf = {'spark.mesos.leader': '1234', 'spark.mesos.principal': 'spark'}
    assert spark_config.stringify_spark_env(conf) == (
        '--conf spark.mesos.leader=1234 --conf spark.mesos.principal=spark'
    )


@pytest.mark.parametrize(
    'spark_conf,expected_output', [
        ({'spark.eventLog.enabled': 'false'}, None),
        (
            {'spark.eventLog.enabled': 'true', 'spark.eventLog.dir': 's3a://test/eventlog'},
            'https://spark-history-testing',
        ),
        (
            {'spark.eventLog.enabled': 'true', 'spark.eventLog.dir': 's3a://test/different/eventlog'},
            None,
        ),
    ],
)
def test_get_history_url(mock_spark_run_conf, spark_conf, expected_output):
    assert spark_config.get_history_url(spark_conf) == expected_output


@pytest.mark.parametrize(
    'memory_string,expected_output', [
        ('1g', 1024),
        ('2048', 2048),
    ],
)
def test_parse_memory_string(memory_string, expected_output):
    assert spark_config.parse_memory_string(memory_string) == expected_output


def test_get_signalfx_url():
    spark_conf = {
        'spark.executorEnv.PAASTA_CLUSTER': 'test-cluster',
        'spark.executorEnv.PAASTA_SERVICE': 'test-service',
        'spark.executorEnv.PAASTA_INSTANCE': 'test-instance',
    }
    assert spark_config.get_signalfx_url(spark_conf) == (
        'https://app.signalfx.com/#/dashboard/FOjL2yRAcAA?density=4'
        '&variables%5B%5D=Instance%3Dinstance_name:'
        '&variables%5B%5D=Service%3Dservice_name:%5B%22spark%22%5D'
        '&variables%5B%5D=PaaSTA%20Cluster%3Dpaasta_cluster:test-cluster'
        '&variables%5B%5D=PaaSTA%20Service%3Dpaasta_service:test-service'
        '&variables%5B%5D=PaaSTA%20Instance%3Dpaasta_instance:test-instance'
        '&startTime=-1h&endTime=Now'
    )


@pytest.mark.parametrize(
    'adj_thresh,cpu,memory,expected_cpu,expected_memory', [
        (999, 10, '60g', 8, '56g'),
        (60, 10, '60g', 8, '56g'),
        (7, 10, '60g', 10, '60g'),
        (999, 4, '32g', 4, '28g'),
        (32, 4, '32g', 4, '28g'),
        (8, 4, '32g', 4, '32g'),
        (999, 2, '8g', 1, '7g'),
        (8, 2, '8g', 1, '7g'),
        (7, 2, '8g', 2, '8g'),
    ],
)
def test_adjust_cpu_mem_ratio_thresh(adj_thresh, cpu, memory, expected_cpu, expected_memory):
    spark_opts = dict()
    spark_opts['spark.executor.cores'] = cpu
    spark_opts['spark.executor.memory'] = memory
    spark_opts['spark.executor.instances'] = 1
    spark_opts['spark.task.cpus'] = 1

    result_dict = spark_config._recalculate_executor_resources(spark_opts, False, adj_thresh)
    assert int(result_dict['spark.executor.cores']) == expected_cpu
    assert result_dict['spark.executor.memory'] == expected_memory
    assert int(result_dict['spark.executor.instances']) == 1
    assert int(result_dict['spark.task.cpus']) == 1


@pytest.mark.parametrize(
    'spark_opts,expected_output', [
        # mesos ( 2 instances, not configure memory overhead, default: 384m )
        (
            {
                'spark.cores.max': '10',
                'spark.executor.cores': '5',
                'spark.executor.memory': '2g',
            },
            {
                'cpus': 10,
                'mem': (384 + 2048) * 2,
                'disk': (384 + 2048) * 2,
                'gpus': 0,
            },
        ),
        # mesos ( 2 instances, not configure memory overhead, default: 409m )
        (
            {
                'spark.cores.max': '10',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
            },
            {
                'cpus': 10,
                'mem': (409 + 4096) * 2,
                'disk': (409 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # mesos ( 2 instances, configure memory overhead)
        (
            {
                'spark.cores.max': '10',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.executor.memoryOverhead': '3072',
            },
            {
                'cpus': 10,
                'mem': (3072 + 4096) * 2,
                'disk': (3072 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # mesos ( 2 instances, Duplicate config, choose the higher memory overhead)
        (
            {
                'spark.cores.max': '10',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.executor.memoryOverhead': '3072',
                'spark.mesos.executor.memoryOverhead': '4096',
            },
            {
                'cpus': 10,
                'mem': (4096 + 4096) * 2,
                'disk': (4096 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # mesos ( 2 instances, configure memory overhead)
        (
            {
                'spark.cores.max': '10',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.mesos.executor.memoryOverhead': '3072',
            },
            {
                'cpus': 10,
                'mem': (3072 + 4096) * 2,
                'disk': (3072 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # k8s
        (
            {
                'spark.executor.instances': '2',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.executor.memoryOverhead': '3072',
            },
            {
                'cpus': 10,
                'mem': (3072 + 4096) * 2,
                'disk': (3072 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # k8s
        (
            {
                'spark.executor.instances': '2',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.kubernetes.memoryOverheadFactor': '0.5',
            },
            {
                'cpus': 10,
                'mem': (4096 * 0.5 + 4096) * 2,
                'disk': (4096 * 0.5 + 4096) * 2,
                'gpus': 0,
            },
        ),
        # gpu
        (
            {
                'spark.cores.max': '10',
                'spark.mesos.gpus.max': '2',
                'spark.executor.cores': '5',
                'spark.executor.memory': '4g',
                'spark.executor.memoryOverhead': '3072',
            },
            {
                'cpus': 10,
                'mem': (3072 + 4096) * 2,
                'disk': (3072 + 4096) * 2,
                'gpus': 2,
            },

        ),
    ],
)
def test_get_resources_requested(spark_opts, expected_output):
    assert spark_config.get_resources_requested(spark_opts) == expected_output


@pytest.fixture
def mock_clusterman_metrics(tmpdir, monkeypatch):
    fp = tmpdir.join('clusterman.yaml')
    fp.write(yaml.dump({
        'clusters': {'test-cluster': {'aws_region': 'test-region'}},
    }))
    monkeypatch.setattr(spark_config, 'CLUSTERMAN_YAML_FILE_PATH', str(fp))
    mock_clusterman_metrics = mock.MagicMock()
    yield mock_clusterman_metrics


@pytest.fixture
def mock_get_resources_requested():
    with mock.patch.object(
        spark_config,
        'get_resources_requested',
        return_value={'cpus': 10, 'mem': 2048},
    ) as m:
        yield m


def test_send_and_calculate_resources_cost(
    mock_clusterman_metrics,
    mock_get_resources_requested,
    mock_time,
):
    mock_clusterman_metrics.generate_key_with_dimensions.side_effect = lambda x, _: x
    app_name = 'test-app'
    spark_opts = {
        'spark.executorEnv.PAASTA_CLUSTER': 'test-cluster',
        'spark.app.name': app_name,
    }
    web_url = 'https://spark-monitor-url.com/'
    cost, resources = spark_config.send_and_calculate_resources_cost(
        mock_clusterman_metrics, spark_opts, web_url, 'test-pool',
    )

    expected_dimension = {'framework_name': app_name, 'webui_url': web_url}

    mock_clusterman_metrics.generate_key_with_dimensions.assert_has_calls([
        mock.call('requested_cpus', expected_dimension),
        mock.call('requested_mem', expected_dimension),
    ])

    mock_writer = (
        mock_clusterman_metrics.ClustermanMetricsBotoClient.return_value
        .get_writer.return_value.__enter__.return_value
    )
    mock_writer.send.assert_has_calls([
        mock.call(('requested_cpus', int(mock_time), 10)),
        mock.call(('requested_mem', int(mock_time), 2048)),
    ])

    mock_clusterman_metrics.util.costs.estimate_cost_per_hour.assert_called_once_with(
        cluster='test-cluster', pool='test-pool', cpus=10, mem=2048,
    )


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
    assert expected_instance_label == spark_config._get_k8s_resource_name_limit_size_with_hash(instance_name)
