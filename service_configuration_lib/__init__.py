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
from __future__ import print_function

import copy
import hashlib
import io
import logging
import os
import socket
import sys
from typing import Mapping

import ephemeral_port_reserve
import yaml

try:
    from yaml.cyaml import CSafeLoader as Loader
except ImportError:  # pragma: no cover (no libyaml-dev / pypy)
    Loader = yaml.SafeLoader  # type: ignore

DEFAULT_SOA_DIR = '/nail/etc/services'
log = logging.getLogger(__name__)
_yaml_cache: Mapping[str, Mapping] = {}
_use_yaml_cache = True


def enable_yaml_cache():
    global _use_yaml_cache
    _use_yaml_cache = True


def disable_yaml_cache():
    global _use_yaml_cache
    _use_yaml_cache = False


def read_port(port_file):
    # Try to read port information
    try:
        with io.open(port_file, 'r', encoding='UTF-8') as port_file_fd:
            port = int(port_file_fd.read().strip())
    except IOError:
        port = None
    except ValueError:
        port = None
    return port


def load_yaml(fd):
    return yaml.load(fd, Loader=Loader)


def read_monitoring(monitoring_file):
    return read_yaml_file(monitoring_file)


def read_deploy(deploy_file):
    return read_yaml_file(deploy_file)


def read_data(data_file):
    return read_yaml_file(data_file)


def read_smartstack(smartstack_file):
    return read_yaml_file(smartstack_file)


def read_service_information(service_file):
    return read_yaml_file(service_file)


def read_dependencies(dependencies_file):
    return read_yaml_file(dependencies_file)


def read_yaml_file(file_name, deepcopy=True):
    if _use_yaml_cache and file_name in _yaml_cache:
        original = _yaml_cache[file_name]
        if deepcopy:
            return copy.deepcopy(original)
        else:
            return original
    data = {}
    try:
        with io.open(file_name, 'r', encoding='UTF-8') as fd:
            data = load_yaml(fd.read())
            data = data or {}
            if _use_yaml_cache:
                _yaml_cache[file_name] = copy.deepcopy(data)
    except IOError:
        pass
    except FileNotFoundError:
        if _use_yaml_cache:
            _yaml_cache[file_name] = {}
    except Exception:
        print('Failed to parse YAML from %s' % file_name, file=sys.stderr)
        raise
    return data


def generate_service_info(service_information, **kwargs):
    service_info = kwargs
    service_info.update(service_information)
    return service_info


def read_extra_service_information(service_name, extra_info, soa_dir=DEFAULT_SOA_DIR, deepcopy=True):
    return read_yaml_file(
        os.path.join(
            os.path.abspath(soa_dir), service_name, extra_info + '.yaml',
        ),
        deepcopy=deepcopy,
    )


def read_service_configuration_from_dir(rootdir, service_dirname):
    port_file = os.path.join(rootdir, service_dirname, 'port')
    monitoring_file = os.path.join(rootdir, service_dirname, 'monitoring.yaml')
    deploy_file = os.path.join(rootdir, service_dirname, 'deploy.yaml')
    data_file = os.path.join(rootdir, service_dirname, 'data.yaml')
    smartstack_file = os.path.join(rootdir, service_dirname, 'smartstack.yaml')
    service_file = os.path.join(rootdir, service_dirname, 'service.yaml')
    dependencies_file = os.path.join(rootdir, service_dirname, 'dependencies.yaml')

    smartstack = read_smartstack(smartstack_file)
    port = smartstack.get('port', read_port(port_file))
    monitoring = read_monitoring(monitoring_file)
    deploy = read_deploy(deploy_file)
    data = read_data(data_file)
    service_information = read_service_information(service_file)
    dependencies = read_dependencies(dependencies_file)

    return generate_service_info(
        service_information,
        port=port,
        monitoring=monitoring,
        deploy=deploy,
        data=data,
        smartstack=smartstack,
        dependencies=dependencies,
    )


def read_service_configuration(service_name, soa_dir=DEFAULT_SOA_DIR):
    return read_service_configuration_from_dir(os.path.abspath(soa_dir), service_name)


def read_services_configuration(soa_dir=DEFAULT_SOA_DIR):
    # Returns a dict of service information, keys are the service name
    # Not all services have all fields. Who knows what might be in there
    all_services = {}
    rootdir = os.path.abspath(soa_dir)
    for service_dirname in os.listdir(rootdir):
        service_name = service_dirname
        service_info = read_service_configuration_from_dir(rootdir, service_dirname)
        all_services.update({service_name: service_info})
    return all_services


def list_services(soa_dir=DEFAULT_SOA_DIR):
    rootdir = os.path.abspath(soa_dir)
    return os.listdir(rootdir)


def read_soa_metadata(soa_dir=DEFAULT_SOA_DIR):
    """Reads the metadata file in the SOA directory"""
    return read_yaml_file(os.path.join(os.path.abspath(soa_dir), '.metadata.json'))


def get_service_from_port(port, all_services=None):
    """Gets the name of the service from the port
    all_services allows you to feed in the services to look through, pass
    in a dict of service names to service information eg.
    {
        'service_name': {
            'port': port_number
        }
    }

    Returns the name of the service
    """
    if port is None or not isinstance(port, int):
        return None

    if all_services is None:
        all_services = read_services_configuration()

    for name, info in all_services.items():
        srv_port = info.get('port')
        if srv_port is not None and port == int(srv_port):
            return name

        for elem in info.get('smartstack', {}).values():
            elem_port = elem.get('proxy_port')
            if elem_port is not None and port == int(elem_port):
                return name


def services_that_run_here():
    hostname = socket.getfqdn()
    return set(services_that_run_on(hostname))


def services_that_run_on(hostname, service_configuration=None):
    running_services = []
    if service_configuration is None:
        service_configuration = read_services_configuration()
    for service in service_configuration:
        if 'runs_on' in service_configuration[service] and \
                service_configuration[service]['runs_on'] and \
                hostname in service_configuration[service]['runs_on']:
            running_services.append(service)
    return running_services


def services_deployed_here():
    hostname = socket.getfqdn()
    return set(services_deployed_on(hostname))


def services_deployed_on(hostname, service_configuration=None):
    if service_configuration is None:
        service_configuration = read_services_configuration()
    running_services = services_that_run_on(hostname, service_configuration)
    # Deployed services are a superset of running ones
    deployed_services = running_services

    for service in service_configuration:
        if (
            'deployed_to' in service_configuration[service] and
            service_configuration[service]['deployed_to'] and (
                service_configuration[service]['deployed_to'] is True or
                hostname in service_configuration[service]['deployed_to']
            ) and
            service not in running_services
        ):
            deployed_services.append(service)

    return deployed_services


def services_needing_puppet_help_here():
    hostname = socket.getfqdn()
    return services_needing_puppet_help_on(hostname)


def services_needing_puppet_help_on(hostname, service_configuration=None):
    if service_configuration is None:
        service_configuration = read_services_configuration()
    deployed_services = services_deployed_on(hostname, service_configuration)
    return [s for s in deployed_services if service_configuration[s].get('needs_puppet_help')]


def all_nodes_that_run(service, service_configuration=None):
    return all_nodes_that_receive(service, service_configuration=service_configuration, run_only=True)


def all_nodes_that_receive(service, service_configuration=None, run_only=False, deploy_to_only=False):
    """If run_only, returns only the services that are in the runs_on list.
    If deploy_to_only, returns only the services in the deployed_to list.
    If neither, both are returned, duplicates stripped.
    Results are always sorted.
    """
    assert not (run_only and deploy_to_only)

    if service_configuration is None:
        service_configuration = read_services_configuration()
    runs_on = service_configuration[service]['runs_on']
    deployed_to = service_configuration[service].get('deployed_to')
    if deployed_to is None:
        deployed_to = []

    if run_only:
        result = runs_on
    elif deploy_to_only:
        result = deployed_to
    else:
        result = set(runs_on) | set(deployed_to)

    return list(sorted(result))


def all_nodes_that_run_in_env(service, env, service_configuration=None):
    """ Returns all nodes that run in an environment. This needs
    to be specified in field named 'env_runs_on' one level under services
    in the configuration, and needs to contain an object which maps strings
    to lists (environments to nodes).

    :param service: A string specifying which service to look up nodes for
    :param env: A string specifying which environment's nodes should be returned
    :param service_configuration: A service_configuration dict to look in or None to
                                  use the default dict.

    :returns: list of all nodes running in a certain environment
    """

    if service_configuration is None:
        service_configuration = read_services_configuration()
    env_runs_on = service_configuration[service]['env_runs_on']
    if env in env_runs_on:
        return list(sorted(env_runs_on[env]))
    else:
        return []


def pick_random_port(hash_key):
    """Return a random port.
    Tries to return the same port for the same service each time, when
    possible.
    """
    hash_number = int(hashlib.sha1(hash_key).hexdigest(), 16)
    preferred_port = 33000 + (hash_number % 25000)
    return ephemeral_port_reserve.reserve('0.0.0.0', preferred_port)


# vim: et ts=4 sw=4
