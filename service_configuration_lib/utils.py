import logging
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
