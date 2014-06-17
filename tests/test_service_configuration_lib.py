#!/usr/bin/env python

import service_configuration_lib
import testify as T
import mock

class ServiceConfigurationLibTestCase(T.TestCase):
    fake_service_configuration = {
         'fake_service1': {'deployed_to': None,
                           'lb_extras': {},
                           'port': 11111,
                           'runs_on': ['fake_hostname3',
                                       'fake_hostname2',
                                       'fake_hostname1'],
                           'vip': 'fakevip1'},
         'fake_service2': {'deployed_to': [ 'fake_deployed_hostname1',
                                            'fake_deployed_hostname2',
                                            'fake_hostname4'],
                     'lb_extras': {'exclude_forwardfor': True},
                     'port': 22222,
                     'runs_on': ['fake_hostname2',
                                 'fake_hostname3',
                                 'fake_hostname4'],
                     'vip': 'fakevip2'},
         'fake_service3': {'deployed_to': None,
                          'lb_extras': {},
                          'port': 33333,
                          'runs_on': ['fake_hostname3',
                                      'fake_hostname4',
                                      'fake_hostname5'],
                          'needs_puppet_help': True,
                          'ssl': True,
                          'vip': 'fakevip3'}
    }

    def test_generate_service_info_should_have_all_keys(self):
        fake_port = 9999
        fake_vip  = 'fakevip9'
        fake_lb_extras = { 'fakekey': 'fakevalue' }
        fake_service_information = { 'fakekey2': 'fakevalue2' }
        actual = service_configuration_lib.generate_service_info(
            fake_port,
            fake_vip,
            fake_lb_extras,
            fake_service_information,
        )
        expected = {
            'lb_extras': fake_lb_extras,
            'vip': fake_vip,
            # Can't use the fake_service_information because it's an 
            # un-nested hash at this point
            'fakekey2': 'fakevalue2',
            'port': fake_port
        }
        T.assert_equal(expected, actual)
        print actual

    def test_read_vip_should_return_none_when_file_doesnt_exist(self):
        expected = None
        fake_vip_file = 'fake_vip_file'
        # TODO: Mock open?
        actual = service_configuration_lib.read_vip(fake_vip_file)
        T.assert_equal(expected, actual)

    def test_services_that_run_on_should_properly_read_configuration(self):
        expected = [ 'fake_service1', 'fake_service2' ]
        fake_hostname = 'fake_hostname2'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_that_run_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_services_that_run_on_should_return_an_empty_array_when_the_hostname_isnt_anywhere(self):
        expected = []
        fake_hostname = 'non_existent_fake_hostname2'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_that_run_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_services_deployed_to_should_return_deployed_and_running_services(self):
        expected = ['fake_service1', 'fake_service2', 'fake_service3']
        fake_hostname = 'fake_hostname3'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_deployed_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_services_needing_puppet_help_on_should_properly_read_configuration(self):
        expected = [ 'fake_service3' ]
        fake_hostname = 'fake_hostname4'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_needing_puppet_help_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_all_nodes_that_run_should_properly_return_the_right_nodes(self):
        expected = [ 'fake_hostname3', 'fake_hostname4', 'fake_hostname5']
        fake_service = 'fake_service3'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.all_nodes_that_run(fake_service, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_services_using_ssl_on_should_return_a_service(self):
        expected = [ 'fake_service3' ]
        fake_hostname = 'fake_hostname4'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_using_ssl_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_all_nodes_that_receive_removes_duplicates(self):
        expected = [ 'fake_deployed_hostname1', 'fake_deployed_hostname2', 'fake_hostname2', 'fake_hostname3', 'fake_hostname4']
        fake_service = 'fake_service2'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.all_nodes_that_receive(fake_service, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_all_nodes_that_receive_with_no_deploys_to(self):
        expected = [ 'fake_hostname3', 'fake_hostname4', 'fake_hostname5']
        fake_service = 'fake_service3'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.all_nodes_that_receive(fake_service, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_all_nodes_that_receive_is_sorted(self):
        expected = [ 'fake_hostname1', 'fake_hostname2', 'fake_hostname3']
        fake_service = 'fake_service1'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.all_nodes_that_receive(fake_service, fake_service_configuration)
        T.assert_equal(expected, actual)


if __name__ == '__main__':
    T.run()

