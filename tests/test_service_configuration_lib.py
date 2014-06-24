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

    @mock.patch('os.path.abspath', return_value='nodir')
    @mock.patch('os.listdir', return_value=["1","2","3"])
    @mock.patch('service_configuration_lib.read_service_configuration_from_dir', return_value='hello')
    def test_read_services_configuration(self, read_patch, listdir_patch, abs_patch):
        expected = {'1': 'hello', '2': 'hello', '3': 'hello'}
        actual = service_configuration_lib.read_services_configuration(soa_dir='testdir')
        abs_patch.assert_called_once_with('testdir')
        listdir_patch.assert_called_once_with('nodir')
        read_patch.assert_has_calls(
            [mock.call('nodir', '1'), mock.call('nodir', '2'), mock.call('nodir', '3')])
        T.assert_equal(expected, actual)

    @mock.patch('service_configuration_lib.read_service_configuration_from_dir', return_value='bye')
    @mock.patch('os.path.abspath', return_value='cafe')
    def test_read_service_configuration(self, abs_patch, read_patch):
        expected = 'bye'
        actual = service_configuration_lib.read_service_configuration('boba', soa_dir='tea')
        abs_patch.assert_called_once_with('tea')
        read_patch.assert_called_once_with('cafe', 'boba')
        T.assert_equal(expected, actual)

    @mock.patch('os.path.join', return_value='forever_joined')
    @mock.patch('service_configuration_lib.read_port', return_value='1111')
    @mock.patch('service_configuration_lib.read_vip', return_value='ULTRA_VIP')
    @mock.patch('service_configuration_lib.read_lb_extras', return_value='no_extras')
    @mock.patch('service_configuration_lib.read_service_information', return_value='no_info')
    @mock.patch('service_configuration_lib.generate_service_info', return_value={'oof': 'ouch'})
    def test_read_service_configuration_from_dir(self, gen_patch, info_patch, lb_patch, vip_patch,
                                                 port_patch, join_patch):
        expected = {'oof' : 'ouch'}
        actual = service_configuration_lib.read_service_configuration_from_dir('never', 'die')
        join_patch.assert_has_calls([mock.call('never','die','port'), mock.call('never','die','vip'),
                mock.call('never','die','lb.yaml'), mock.call('never','die','service.yaml')])
        port_patch.assert_called_once_with('forever_joined')
        vip_patch.assert_called_once_with('forever_joined')
        lb_patch.assert_called_once_with('forever_joined')
        info_patch.assert_called_once_with('forever_joined')
        gen_patch.assert_called_once_with('1111', 'ULTRA_VIP', 'no_extras', 'no_info')
        T.assert_equal(expected, actual)

    @mock.patch('os.path.join', return_value='together_forever')
    @mock.patch('os.path.abspath', return_value='real_soa_dir')
    @mock.patch('service_configuration_lib.read_service_information', return_value={'what': 'info'})
    def test_read_extra_service_information(self, info_patch, abs_patch, join_patch):
        expected = {'what': 'info'}
        actual = service_configuration_lib.read_extra_service_information('noname',
                'noinfo', soa_dir='whatsadir')
        abs_patch.assert_called_once_with('whatsadir')
        join_patch.assert_called_once_with('real_soa_dir', 'noname', 'noinfo', '.yaml')
        info_patch.assert_called_once_with('together_forever')
        T.assert_equal(expected, actual)

if __name__ == '__main__':
    T.run()

