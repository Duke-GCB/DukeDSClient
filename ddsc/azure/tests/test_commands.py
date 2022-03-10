from unittest import TestCase
from mock import patch, Mock, call, ANY
from ddsc.azure.commands import BaseAzureCommand, DDSUserException, AzureListCommand, AzureDeleteCommand


class TestBaseAzureCommand(TestCase):
    def setUp(self):
        self.config = Mock()
        self.base_cmd = BaseAzureCommand(self.config)

    @patch('ddsc.azure.commands.create_azure_api')
    def test_azure_api(self, mock_create_azure_api):
        mock_azure_api = mock_create_azure_api.return_value
        self.assertEqual(self.base_cmd._azure_api, None)
        self.assertEqual(self.base_cmd.azure_api, mock_azure_api)
        self.assertEqual(self.base_cmd._azure_api, mock_azure_api)

    def test_get_netid(self):
        # prevent email since it's not reliable with azure storage
        with self.assertRaises(DDSUserException):
            BaseAzureCommand.get_netid(Mock(email="someemail"))
        self.assertEqual(BaseAzureCommand.get_netid(Mock(username="someuser", email=None)), "someuser")

    def test_get_project(self):
        self.base_cmd._azure_api = Mock()
        self.base_cmd._azure_api.get_project_by_name.return_value = ['somedata']
        # project ids are not compatible with azure
        with self.assertRaises(DDSUserException):
            self.base_cmd.get_project(Mock(project_id='123'))
        projects = self.base_cmd.get_project(Mock(project_id=None, project_name="mouse"))
        self.assertEqual(projects, ['somedata'])
        self.base_cmd._azure_api.get_project_by_name.return_value = ['somedata']
        # raises when project not found
        self.base_cmd._azure_api.get_project_by_name.return_value = []
        with self.assertRaises(DDSUserException):
            self.base_cmd.get_project(Mock(project_id=None, project_name="mouse"))


class TestAzureListCommand(TestCase):
    @patch('ddsc.azure.commands.print')
    def test_run_list_projects(self, mock_print):
        list_cmd = AzureListCommand(config=Mock())
        list_cmd._azure_api = Mock()
        mock_project = Mock()
        mock_project.name = 'mouse'
        mock_project.get_url.return_value = 'someurl'
        list_cmd._azure_api.list_projects.return_value = [mock_project]
        list_cmd.run(args=Mock(project_id=None, project_name=None, auth_role=None, long_format=False))
        mock_print.assert_has_calls([
            call('mouse')
        ])

    @patch('ddsc.azure.commands.print')
    def test_run_list_projects_long(self, mock_print):
        list_cmd = AzureListCommand(config=Mock())
        list_cmd._azure_api = Mock()
        mock_project = Mock()
        mock_project.name = 'mouse'
        mock_project.get_url.return_value = 'someurl'
        list_cmd._azure_api.list_projects.return_value = [mock_project]
        list_cmd.run(args=Mock(project_id=None, project_name=None, auth_role=None, long_format=True))
        mock_print.assert_has_calls([
            call('mouse\tsomeurl')
        ])

    @patch('ddsc.azure.commands.print')
    def test_run_list_project(self, mock_print):
        list_cmd = AzureListCommand(config=Mock())
        list_cmd._azure_api = Mock()
        mock_project = Mock()
        mock_project.name = 'mouse'
        mock_file = Mock(project_path='data/file1.txt')
        mock_project.get_file_paths.return_value = [mock_file]
        list_cmd._azure_api.get_project_by_name.return_value = mock_project
        list_cmd.run(args=Mock(project_id=None, project_name='mouse', auth_role=None, long_format=False))
        mock_print.assert_has_calls([
            call('Project mouse Contents:'),
            call('data/file1.txt')
        ])

    @patch('ddsc.azure.commands.print')
    def test_run_list_project_long(self, mock_print):
        list_cmd = AzureListCommand(config=Mock())
        list_cmd._azure_api = Mock()
        mock_project = Mock()
        mock_project.name = 'mouse'
        mock_file = Mock(project_path='data/file1.txt')
        mock_file.get_md5.return_value = 'abc123'
        mock_project.get_file_paths.return_value = [mock_file]
        list_cmd._azure_api.get_project_by_name.return_value = mock_project
        list_cmd.run(args=Mock(project_id=None, project_name='mouse', auth_role=None, long_format=True))
        mock_print.assert_has_calls([
            call('Project mouse Contents:'),
            call('data/file1.txt (md5:abc123)')
        ])


class TestAzureDeleteCommand(TestCase):
    @patch('ddsc.azure.commands.boolean_input_prompt')
    def test_delete_prompts(self, mock_boolean_input_prompt):
        mock_boolean_input_prompt.return_value = True
        cmd = AzureDeleteCommand(config=Mock())
        cmd._azure_api = Mock()
        cmd.run(args=Mock(remote_path=None, force=False, project_id=None, project_name="mouse"))
        mock_boolean_input_prompt.assert_called_with(ANY)

    @patch('ddsc.azure.commands.boolean_input_prompt')
    def test_delete_force(self, mock_boolean_input_prompt):
        mock_boolean_input_prompt.return_value = True
        cmd = AzureDeleteCommand(config=Mock())
        cmd._azure_api = Mock()
        cmd.run(args=Mock(remote_path=None, force=True, project_id=None, project_name="mouse"))
        mock_boolean_input_prompt.assert_not_called()
