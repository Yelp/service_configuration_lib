#!/usr/bin/env python
# Copyright 2015 Yelp Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
from setuptools import find_packages
from setuptools import setup

setup(
    name='service-configuration-lib',
    version='2.18.16',
    provides=['service_configuration_lib'],
    description='Start, stop, and inspect Yelp SOA services',
    url='https://github.com/Yelp/service_configuration_lib',
    author='Yelp Compute Infrastructure Team',
    author_email='opensource+scl@yelp.com',
    packages=find_packages(exclude=['tests', 'scripts']),
    install_requires=[
        'ephemeral-port-reserve >= 1.1.0',
        'PyYAML >= 5.1',
        'pyinotify',
        'requests>=2.18.4',
        'boto3',
    ],
    license='Copyright Yelp 2013, All Rights Reserved',
    scripts=[
        'scripts/all_nodes_that_receive',
        'scripts/all_nodes_that_run',
        'scripts/dump_service_configuration_yaml',
        'scripts/services_deployed_here',
        'scripts/services_needing_puppet_help',
        'scripts/services_that_run_here',
    ],
)
