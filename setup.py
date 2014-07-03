#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='service-configuration-lib',
    version='0.5.0',
    provides=['service_configuration_lib'],
    description='Start, stop, and inspect Yelp SOA services',
    url='https://github.com/Yelp/service_configuration_lib',
    author='Yelp Operations Team',
    author_email='opensource+scl@yelp.com',
    packages=find_packages(exclude=['tests', 'scripts']),
    install_requires=['PyYAML >= 3.0', 'pycurl'],
    license='Copyright Yelp 2013, All Rights Reserved',
    scripts=[
        'scripts/all_nodes_that_receive',
        'scripts/all_nodes_that_run',
        'scripts/dump_service_configuration_yaml',
        'scripts/services_deployed_here',
        'scripts/services_needing_puppet_help',
        'scripts/services_that_run_here',
        'scripts/services_using_ssl'
    ],
)
