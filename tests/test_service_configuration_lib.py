#!/usr/bin/env python

import service_configuration_lib
import testify as T
import mock

class ServiceConfigurationLibTestCase(T.TestCase):
    fake_service_configuration = {
         'fake_service1': {'deployed_to': None,
                           'lb_extras': {},
                           'monitoring': {
                               'fake_monitoring_key': 'fake_monitoring_value'
                            },
                           'deploy': {},
                           'port': 11111,
                           'runs_on': ['fake_hostname3',
                                       'fake_hostname2',
                                       'fake_hostname1'],
                           'vip': 'fakevip1'},
         'fake_service2': {'deployed_to': [ 'fake_deployed_hostname1',
                                            'fake_deployed_hostname2',
                                            'fake_hostname4'],
                     'lb_extras': {'exclude_forwardfor': True},
                     'monitoring': {},
                     'port': 22222,
                     'runs_on': ['fake_hostname2',
                                 'fake_hostname3',
                                 'fake_hostname4'],
                     'vip': 'fakevip2'},
         'fake_service3': {'deployed_to': None,
                          'lb_extras': {},
                          'monitoring': {},
                          'port': 33333,
                          'runs_on': ['fake_hostname3',
                                      'fake_hostname4',
                                      'fake_hostname5'],
                          'env_runs_on': {
                            'fake_env1': ['fake_hostname3'],
                            'fake_env2': ['fake_hostname4', 'fake_hostname5']
                            },
                          'needs_puppet_help': True,
                          'ssl': True,
                          'vip': 'fakevip3',
         },
        'fake_service4': { 'deployed_to': True,
                           'runs_on': [],
                           'needs_puppet_help': True,
        },
        'fake_service5': { 'deployed_to': [],
                           'runs_on': [],
                           'needs_puppet_help': True,
        },
    }

    def test_generate_service_info_should_have_all_keys(self):
        """I'm not entirely sure what this test is testing since I can add a
        new value or remove an old value and the test passes without changing
        any code. I simplified it to make it less misleading and focus on the
        one thing it does to, which is test that the arg service_information is
        updated.
        """
        fake_service_information = { 'fakekey2': 'fakevalue2' }
        fake_port = 9999
        actual = service_configuration_lib.generate_service_info(
            fake_service_information,
            port=fake_port,
        )
        expected = {
            # Can't use the fake_service_information because it's an
            # un-nested hash at this point
            'fakekey2': 'fakevalue2',
            'port': fake_port,
        }
        T.assert_equal(expected, actual)

    def test_read_vip_should_return_none_when_file_doesnt_exist(self):
        expected = None
        fake_vip_file = 'fake_vip_file'
        # TODO: Mock open?
        actual = service_configuration_lib.read_vip(fake_vip_file)
        T.assert_equal(expected, actual)

    def test_read_monitoring_should_return_empty_when_file_doesnt_exist(self):
        expected = {}
        fake_monitoring_file = 'fake_monitoring_file'
        # TODO: Mock open?
        actual = service_configuration_lib.read_monitoring(
            fake_monitoring_file
        )
        T.assert_equal(expected, actual)

    def test_read_deploy_should_return_empty_when_file_doesnt_exist(self):
        expected = {}
        fake_deploy_file = 'fake_deploy_file'
        # TODO: Mock open?
        actual = service_configuration_lib.read_deploy(
            fake_deploy_file
        )
        T.assert_equal(expected, actual)

    def test_read_smartstack_should_return_empty_when_file_doesnt_exist(self):
        expected = {}
        fake_smartstack_file = 'fake_smartstack_file'
        # TODO: Mock open?
        actual = service_configuration_lib.read_smartstack(
            fake_smartstack_file
        )
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

    @mock.patch('service_configuration_lib.os.path.isdir', autospec=True)
    @mock.patch('service_configuration_lib.os.listdir', autospec=True)
    def test_list_extra_deployed_here(self, mock_listdir, mock_isdir):
        expected = ['fake_service1', 'fake_service2']
        mock_listdir.return_value = expected
        mock_isdir.return_value = True
        actual = service_configuration_lib.list_extra_deployed_here('tests/fake_extra')
        T.assert_sorted_equal(expected, actual)

    @mock.patch('service_configuration_lib.os.path.isdir', autospec=True)
    def test_list_extra_deployed_here_no_directory(self, mock_isdir):
        expected = []
        mock_isdir.return_value = False
        actual = service_configuration_lib.list_extra_deployed_here('missing/directory')
        T.assert_equal(expected, actual)

    @mock.patch('service_configuration_lib.os.path.isdir', autospec=True)
    @mock.patch('service_configuration_lib.os.listdir', autospec=True)
    def test_list_extra_run_here(self, mock_listdir, mock_isdir):
        expected = ['fake_service1']
        mock_isdir.return_value = True
        mock_listdir.return_value = expected
        actual = service_configuration_lib.list_extra_run_here('tests/fake_extra')
        T.assert_sorted_equal(expected, actual)

    @mock.patch('service_configuration_lib.os.path.isdir', autospec=True)
    def test_list_extra_run_here_no_directory(self, mock_isdir):
        expected = []
        mock_isdir.return_value = False
        actual = service_configuration_lib.list_extra_run_here('missing/directory')
        T.assert_sorted_equal(expected, actual)

    def test_services_deployed_to_should_return_deployed_and_running_services(self):
        expected = ['fake_service1', 'fake_service2', 'fake_service3', 'fake_service4']
        fake_hostname = 'fake_hostname3'
        fake_service_configuration = self.fake_service_configuration
        actual = service_configuration_lib.services_deployed_on(fake_hostname, fake_service_configuration)
        T.assert_equal(expected, actual)

    def test_services_needing_puppet_help_on_should_properly_read_configuration(self):
        expected = [ 'fake_service3', 'fake_service4' ]
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
    @mock.patch('service_configuration_lib.read_monitoring', return_value='no_monitoring')
    @mock.patch('service_configuration_lib.read_deploy', return_value='no_deploy')
    @mock.patch('service_configuration_lib.read_data', return_value='no_data')
    @mock.patch('service_configuration_lib.read_service_information', return_value='no_info')
    @mock.patch('service_configuration_lib.generate_service_info', return_value={'oof': 'ouch'})
    def test_read_service_configuration_from_dir(
        self,
        gen_patch,
        info_patch,
        data_patch,
        deploy_patch,
        monitoring_patch,
        lb_patch,
        vip_patch,
        port_patch,
        join_patch,
    ):
        expected = {'oof' : 'ouch'}
        actual = service_configuration_lib.read_service_configuration_from_dir('never', 'die')
        join_patch.assert_has_calls([
            mock.call('never','die','port'),
            mock.call('never','die','vip'),
            mock.call('never','die','lb.yaml'),
            mock.call('never','die','monitoring.yaml'),
            mock.call('never','die','deploy.yaml'),
            mock.call('never','die','data.yaml'),
            mock.call('never','die','service.yaml'),
        ])
        port_patch.assert_called_once_with('forever_joined')
        vip_patch.assert_called_once_with('forever_joined')
        lb_patch.assert_called_once_with('forever_joined')
        monitoring_patch.assert_called_once_with('forever_joined')
        deploy_patch.assert_called_once_with('forever_joined')
        data_patch.assert_called_once_with('forever_joined')
        info_patch.assert_called_once_with('forever_joined')
        gen_patch.assert_called_once_with('no_info', port='1111',
                                          vip='ULTRA_VIP',
                                          lb_extras='no_extras',
                                          monitoring='no_monitoring',
                                          deploy='no_deploy',
                                          data='no_data')
        T.assert_equal(expected, actual)

    @mock.patch('os.path.join', return_value='together_forever')
    @mock.patch('os.path.abspath', return_value='real_soa_dir')
    @mock.patch('service_configuration_lib._read_yaml_file', return_value={'what': 'info'})
    def test_read_extra_service_information(self, info_patch, abs_patch, join_patch):
        expected = {'what': 'info'}
        actual = service_configuration_lib.read_extra_service_information('noname',
                'noinfo', soa_dir='whatsadir')
        abs_patch.assert_called_once_with('whatsadir')
        join_patch.assert_called_once_with('real_soa_dir', 'noname', 'noinfo.yaml')
        info_patch.assert_called_once_with('together_forever')
        T.assert_equal(expected, actual)

    @mock.patch('service_configuration_lib.open', create=True, return_value=mock.MagicMock(spec=file))
    @mock.patch('service_configuration_lib.load_yaml', return_value={'data': 'mock'})
    def test_read_yaml_file_single(self, load_patch, open_patch):
        expected = {'data': 'mock'}
        filename = 'fake_fname_uno'
        actual = service_configuration_lib._read_yaml_file(filename)
        open_patch.assert_called_once_with(filename, 'r')
        load_patch.assert_called_once_with(open_patch.return_value.__enter__().read())
        T.assert_equal(expected, actual)

    @mock.patch('service_configuration_lib.open', create=True, return_value=mock.MagicMock(spec=file))
    @mock.patch('service_configuration_lib.load_yaml', return_value={'mmmm': 'tests'})
    def test_read_yaml_file_with_cache(self, load_patch, open_patch):
        expected = {'mmmm': 'tests'}
        filename = 'fake_fname_dos'
        service_configuration_lib.enable_yaml_cache()
        actual = service_configuration_lib._read_yaml_file(filename)
        actual_two = service_configuration_lib._read_yaml_file(filename)
        open_patch.assert_called_once_with(filename, 'r')
        load_patch.assert_called_once_with(open_patch.return_value.__enter__().read())
        T.assert_equal(expected, actual)
        T.assert_equal(expected, actual_two)
        # When we cache, we can NOT return a pointer to the original object
        # because the caller might mutate it. We need to ensure that
        # the returned object is a copy.
        T.assert_is_not(actual, actual_two)

    @mock.patch('service_configuration_lib.open', create=True, return_value=mock.MagicMock(spec=file))
    @mock.patch('service_configuration_lib.load_yaml', return_value={'water': 'slide'})
    def test_read_yaml_file_no_cache(self, load_patch, open_patch):
        expected = {'water': 'slide'}
        filename = 'fake_fname_tres'
        service_configuration_lib.disable_yaml_cache()
        actual = service_configuration_lib._read_yaml_file(filename)
        actual_two = service_configuration_lib._read_yaml_file(filename)
        open_patch.assert_any_call(filename, 'r')
        assert open_patch.call_count == 2
        load_patch.assert_any_call(open_patch.return_value.__enter__().read())
        assert load_patch.call_count == 2
        T.assert_equal(expected, actual)
        T.assert_equal(expected, actual_two)

    def test_env_runs_on(self):
        expected = ['fake_hostname3']
        actual = service_configuration_lib.all_nodes_that_run_in_env('fake_service3','fake_env1', service_configuration=self.fake_service_configuration)
        T.assert_equal(expected, actual)

        expected = ['fake_hostname4', 'fake_hostname5']
        actual = service_configuration_lib.all_nodes_that_run_in_env('fake_service3','fake_env2', service_configuration=self.fake_service_configuration)
        T.assert_equal(expected, actual)

if __name__ == '__main__':
    T.run()

