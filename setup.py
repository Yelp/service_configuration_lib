from setuptools import setup, find_packages

setup(
    name='service_configuration_lib',
    version='0.1',
    provides=['service_configuration_lib'],
    description='Start, stop, and inspect Yelp SOA services',
    author='Yelp Operations Team',
    author_email='opensource+scl@yelp.com',
    packages=find_packages(exclude=['tests']),
    install_requires=[],
)
