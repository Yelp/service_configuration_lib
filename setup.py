#!/usr/bin/env python

from setuptools import setup, find_packages

setup(
    name='service_configuration_lib',
    version='0.1.1',
    provides=['service_configuration_lib'],
    description='Start, stop, and inspect Yelp SOA services',
    url='https://gitweb.yelpcorp.com/?p=service_configuration_lib.git',
    author='Yelp Operations Team',
    author_email='operations@yelp.com',
    packages=find_packages(exclude=['tests']),
    install_requires=['PyYAML >= 3.0'],
    license='Copyright Yelp 2013, All Rights Reserved'
)
