import pytest
import uuid
from unittest import mock
from service_configuration_lib import utils


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
        'ml_compute_test_string',
        'random_string',
    ],
)
def test_generate_pod_template_path(hex_value):
    with mock.patch.object(uuid, 'uuid4', return_value=uuid.UUID(hex=hex_value)):
        assert utils.generate_pod_template_path() == f'/nail/tmp/spark-pt-{hex_value}.yaml'
