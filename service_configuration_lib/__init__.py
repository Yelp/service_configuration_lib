#!/usr/bin/env python

import os
import sys
import yaml
import socket

DEFAULT_SOA_DIR = "/nail/etc/services"

def read_port(port_file):
    # Try to read port information
    try:
        with open(port_file, 'r') as port_file_fd:
            port = int(port_file_fd.read().strip())
    except IOError:
        port = None
    except ValueError:
        port = None
    return port

def read_vip(vip_file):
    try:
        with open(vip_file, 'r') as vip_file_fd:
            vip = vip_file_fd.read().strip()
    except IOError:
        vip = None
    return vip

def load_yaml(fd):
    if yaml.__with_libyaml__:
        return yaml.load(fd, Loader=yaml.CLoader)
    else:
        return yaml.load(fd)

def read_lb_extras(lb_extras_file):
    try:
        with open(lb_extras_file, 'r') as lbe_file_fd:
            lb_extras = load_yaml(lbe_file_fd.read())
            if not lb_extras:
                lb_extras = {}
    except IOError:
        lb_extras = {}
    except:
        print >>sys.stderr, "Failed to parse YAML from %s" % lb_extras_file
        raise
    return lb_extras

def read_service_information(service_file):
    try:
        with open(service_file, 'r') as service_file_fd:
            service_information = load_yaml(service_file_fd.read())
            if not service_information:
                service_information = {}
    except IOError:
        service_information = {}
    except:
        print >>sys.stderr, "Failed to parse YAML from %s" % service_file
        raise
    return service_information

def generate_service_info(port, vip, lb_extras, service_information):
    service_info = { 'port': port, 'vip': vip, 'lb_extras': lb_extras }
    service_info.update( service_information )
    return service_info

def read_services_configuration(soa_dir=DEFAULT_SOA_DIR):
    # Returns a dict of service information, keys are the service name
    # Not all services have all fields. Who knows what might be in there
    # You can't depend on every service having a vip, for example
    all_services = {}
    for rootdir, dirs, _ in os.walk(soa_dir):
        for service_dirname in dirs:
            service_name = service_dirname

            port_file = os.path.join(rootdir, service_dirname, "port")
            vip_file = os.path.join(rootdir, service_dirname, "vip")
            lb_extras_file = os.path.join(rootdir, service_dirname, "lb.yaml")
            service_file = os.path.join(rootdir, service_dirname, "service.yaml")

            port = read_port(port_file)
            vip = read_vip(vip_file)
            lb_extras = read_lb_extras(lb_extras_file)
            service_information = read_service_information(service_file)

            service_info = generate_service_info(port, vip, lb_extras, service_information) 
            all_services.update( { service_name: service_info } )
    return all_services

def services_that_run_here():
    hostname = socket.getfqdn()
    return services_that_run_on(hostname)

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
    return services_deployed_on(hostname)

def services_deployed_on(hostname, service_configuration=None):
    if service_configuration is None:
        service_configuration = read_services_configuration()
    running_services = services_that_run_on(hostname, service_configuration)
    # Deployed services are a superset of running ones
    deployed_services = running_services
    for service in service_configuration:
        if 'deployed_to' in service_configuration[service] and \
           service_configuration[service]['deployed_to'] and \
           hostname in service_configuration[service]['deployed_to'] and \
           service not in running_services :
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
    """ If run_only, returns only the services that are in the runs_on list.
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

def services_using_ssl_on(hostname, service_configuration=None):
    if service_configuration is None:
        service_configuration = read_services_configuration()
    deployed_services = services_deployed_on(hostname,service_configuration)
    return [s for s in deployed_services if service_configuration[s].get('ssl')]

def services_using_ssl_here():
    hostname = socket.getfqdn()
    return services_using_ssl_on(hostname)

# vim: et ts=4 sw=4

