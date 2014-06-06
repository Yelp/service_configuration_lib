#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='service_configuration_lib',
    version='0.1.1',
    provides=['service_configuration_lib'],
    description='Start, stop, and inspect Yelp SOA services',
    url='https://github.com/Yelp/service_configuration_lib',
    author='Yelp Operations Team',
    author_email='opensource+scl@yelp.com',
    packages=find_packages(exclude=['tests']),
    setup_requires=['setuptools'],
    install_requires=['PyYAML >= 3.0'],
    license='Copyright Yelp 2013, All Rights Reserved'
)
