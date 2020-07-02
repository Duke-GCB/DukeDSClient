from __future__ import absolute_import
from unittest import TestCase
from ddsc.ddsclient import BaseCommand, UploadCommand, ListCommand, DownloadCommand, ClientCommand, MoveCommand
from ddsc.ddsclient import ShareCommand, DeliverCommand, InfoCommand, read_argument_file_contents, \
    INVALID_DELIVERY_RECIPIENT_MSG
from mock import patch, MagicMock, Mock, call, ANY


class TestBaseCommand(TestCase):
    def setUp(self):
        self.args_with_project_id = Mock(project_name=None, project_id='123')
        self.args_with_project_name = Mock(project_name='mouse', project_id=None)

    def test_project_name_or_id_from_args(self):
        project_name_or_id = BaseCommand.create_project_name_or_id_from_args(self.args_with_project_id)
        self.assertEqual('123', project_name_or_id.value)
        self.assertEqual(False, project_name_or_id.is_name)

        project_name_or_id = BaseCommand.create_project_name_or_id_from_args(self.args_with_project_name)
        self.assertEqual('mouse', project_name_or_id.value)
        self.assertEqual(True, project_name_or_id.is_name)

    @patch('ddsc.ddsclient.RemoteStore')
    def test_fetch_project(self, mock_remote_store):
        mock_config = MagicMock()
        base_cmd = BaseCommand(mock_config)
        base_cmd.fetch_project(self.args_with_project_id, must_exist=True, include_children=False)
        mock_remote_store.return_value.fetch_remote_project.assert_called()
        args, kwargs = mock_remote_store.return_value.fetch_remote_project.call_args
        project_name_or_id = args[0]
        self.assertEqual('123', project_name_or_id.value)
        self.assertEqual(False, project_name_or_id.is_name)
        self.assertEqual(True, kwargs['must_exist'])
        self.assertEqual(False, kwargs['include_children'])

    @patch('ddsc.ddsclient.RemoteStore', autospec=True)
    def test_make_user_list(self, mock_remote_store):
        mock_config = MagicMock()
        base_cmd = BaseCommand(mock_config)
        mock_remote_store.return_value.get_or_register_user_by_username.side_effect = [
            Mock(username='joe', email='joe@joe.joe'),
            Mock(username='bob', email='bob@bob.bob'),
        ]
        mock_remote_store.return_value.get_or_register_user_by_email.side_effect = [
            Mock(username='joe', email='joe@joe.joe'),
            ValueError("Invalid")
        ]

        # Find users by username
        results = base_cmd.make_user_list(emails=None, usernames=[
            'joe',
            'bob'
        ])
        self.assertEqual([user.email for user in results], ['joe@joe.joe', 'bob@bob.bob'])
        mock_remote_store.return_value.get_or_register_user_by_username.assert_has_calls([
            call("joe"),
            call("bob")
        ])

        # Find users by email
        results = base_cmd.make_user_list(emails=['joe@joe.joe'], usernames=None)
        self.assertEqual([user.username for user in results], ['joe'])
        mock_remote_store.return_value.get_or_register_user_by_email.assert_has_calls([
            call("joe@joe.joe")
        ])

        # Should get an error for invalid users
        with self.assertRaises(ValueError) as raisedError:
            base_cmd.make_user_list(emails=['no@no.no'], usernames=[])
        self.assertEqual('Invalid',
                         str(raisedError.exception))


class TestUploadCommand(TestCase):
    @patch("ddsc.ddsclient.ProjectUpload")
    @patch("ddsc.ddsclient.ProjectNameOrId")
    @patch("ddsc.ddsclient.LocalProject")
    @patch("ddsc.ddsclient.ProjectUploadDryRun")
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.print')
    def test_without_dry_run(self, mock_print, mock_remote_store, mock_project_upload_dry_run, mock_local_project,
                             mock_project_name_or_id, mock_project_upload):
        mock_config = MagicMock()
        cmd = UploadCommand(mock_config)
        args = Mock()
        args.project_name = "test"
        args.project_id = None
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = False
        cmd.run(args)

        mock_project_name_or_id.create_from_name.assert_called_with("test")

        # create local project with folders
        mock_local_project.return_value.add_paths.assert_called_with(["data", "scripts"])

        # update local project with details about remote files/folders
        remote_project = mock_remote_store.return_value.fetch_remote_project
        mock_local_project.return_value.update_remote_ids(remote_project)

        # uploads with local project
        items_to_send = mock_local_project.return_value.count_items_to_send.return_value
        mock_project_upload.assert_called_with(mock_config, ANY, mock_local_project.return_value, items_to_send)
        mock_project_upload.return_value.run.assert_called_with()
        mock_print.assert_has_calls([
            call(mock_local_project.return_value.count_local_items.return_value.to_str.return_value),
            call(mock_local_project.return_value.count_items_to_send.return_value.to_str.return_value),
            call(mock_project_upload.return_value.get_upload_report.return_value.summary.return_value),
            call(),
            call('\n'),
            call(mock_project_upload.return_value.get_upload_report.return_value.get_content.return_value),
            call('\n'),
            call(mock_project_upload.return_value.get_url_msg.return_value),
        ])

    @patch("ddsc.ddsclient.ProjectUpload")
    @patch("ddsc.ddsclient.ProjectNameOrId")
    @patch("ddsc.ddsclient.LocalProject")
    @patch("ddsc.ddsclient.ProjectUploadDryRun")
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.print')
    def test_without_dry_run_project_id(self, mock_print, mock_remote_store, mock_project_upload_dry_run,
                                        mock_local_project, mock_project_name_or_id, mock_project_upload):
        mock_config = MagicMock()
        cmd = UploadCommand(mock_config)
        args = Mock()
        args.project_name = None
        args.project_id = '123'
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = False
        cmd.run(args)

        mock_project_name_or_id.create_from_project_id.assert_called_with("123")

        # create local project with folders
        mock_local_project.return_value.add_paths.assert_called_with(["data", "scripts"])

        # update local project with details about remote files/folders
        remote_project = mock_remote_store.return_value.fetch_remote_project
        mock_local_project.return_value.update_remote_ids(remote_project)

        # uploads with local project
        items_to_send = mock_local_project.return_value.count_items_to_send.return_value
        mock_project_upload.assert_called_with(mock_config, ANY, mock_local_project.return_value, items_to_send)
        mock_project_upload.return_value.run.assert_called_with()

    @patch("ddsc.ddsclient.LocalProject")
    @patch("ddsc.ddsclient.ProjectUploadDryRun")
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.print')
    def test_with_dry_run(self, mock_print, mock_remote_store, mock_project_upload_dry_run, mock_local_project):
        mock_config = MagicMock()
        cmd = UploadCommand(mock_config)
        args = Mock()
        args.project_name = "test"
        args.project_id = None
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = True
        cmd.run(args)

        mock_local_project.assert_called_with(followsymlinks=False, file_exclude_regex=mock_config.file_exclude_regex)
        mock_project_upload_dry_run.assert_called_with(mock_local_project.return_value)
        mock_print.assert_called_with(mock_project_upload_dry_run.return_value.get_report.return_value)


class TestDownloadCommand(TestCase):
    @patch('ddsc.ddsclient.Client')
    @patch("ddsc.ddsclient.ProjectFileDownloader")
    def test_run_project_name(self, mock_project_file_downloader, mock_client):
        cmd = DownloadCommand(MagicMock())
        args = Mock()
        args.project_name = 'mouse'
        args.project_id = None
        args.include_paths = None
        args.exclude_paths = None
        args.folder = '/tmp/data'
        cmd.run(args)

        mock_client.return_value.get_project_by_name.assert_called_with('mouse')
        mock_project_file_downloader.assert_called_with(
            cmd.config,
            '/tmp/data',
            mock_client.return_value.get_project_by_name.return_value,
            path_filter=None
        )
        mock_project_file_downloader.return_value.run.assert_called()

    @patch('ddsc.ddsclient.Client')
    @patch("ddsc.ddsclient.ProjectFileDownloader")
    def test_run_project_id(self, mock_project_file_downloader, mock_client):
        cmd = DownloadCommand(MagicMock())
        args = Mock()
        args.project_name = None
        args.project_id = '123'
        args.include_paths = None
        args.exclude_paths = None
        args.folder = '/tmp/stuff'
        cmd.run(args)

        mock_client.return_value.get_project_by_id.assert_called_with('123')
        mock_project_file_downloader.assert_called_with(
            cmd.config,
            '/tmp/stuff',
            mock_client.return_value.get_project_by_id.return_value,
            path_filter=None
        )
        mock_project_file_downloader.return_value.run.assert_called()


class TestShareCommand(TestCase):
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_no_message(self, mock_d4s2_project, mock_remote_store):
        cmd = ShareCommand(MagicMock())
        myargs = Mock(project_name='mouse', email=None, username='joe123', force_send=False,
                      auth_role='project_viewer', msg_file=None)
        cmd.run(myargs)
        args, kwargs = mock_d4s2_project().share.call_args
        project, to_user, force_send, auth_role, message = args
        self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
        self.assertEqual('project_viewer', auth_role)
        self.assertEqual('', message)
        args, kwargs = mock_remote_store.return_value.fetch_remote_project.call_args
        self.assertEqual('mouse', args[0].get_name_or_raise())

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_message(self, mock_d4s2_project, mock_remote_store):
        with open('setup.py') as message_infile:
            cmd = ShareCommand(MagicMock())
            myargs = Mock(project_name=None, project_id='123', email=None, username='joe123', force_send=False,
                          auth_role='project_viewer', msg_file=message_infile)
            cmd.run(myargs)
            args, kwargs = mock_d4s2_project().share.call_args
            project, to_user, force_send, auth_role, message = args
            self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
            self.assertEqual('project_viewer', auth_role)
            self.assertIn('setup(', message)
            args, kwargs = mock_remote_store.return_value.fetch_remote_project.call_args
            self.assertEqual('123', args[0].get_id_or_raise())


class TestDeliverCommand(TestCase):
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_no_message_and_copy(self, mock_d4s2_project, mock_remote_store):
        cmd = DeliverCommand(MagicMock())
        cmd.get_new_project_name = Mock()
        cmd.get_new_project_name.return_value = 'NewProjectName'
        myargs = Mock(project_name='mouse',
                      project_id=None,
                      email=None,
                      resend=False,
                      username='joe123',
                      share_usernames=[],
                      share_emails=[],
                      copy_project=True,
                      include_paths=None,
                      exclude_paths=None,
                      msg_file=None)
        cmd.run(myargs)
        args, kwargs = mock_d4s2_project().deliver.call_args
        project, new_project_name, to_user, share_users, force_send, path_filter, message = args
        self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
        self.assertEqual(False, force_send)
        self.assertEqual('', message)
        self.assertEqual('NewProjectName', new_project_name)
        args, kwargs = mock_remote_store.return_value.fetch_remote_project.call_args
        self.assertEqual('mouse', args[0].get_name_or_raise())

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_message(self, mock_d4s2_project, mock_remote_store):
        with open('setup.py') as message_infile:
            cmd = DeliverCommand(MagicMock())
            myargs = Mock(project_name=None,
                          project_id='456',
                          resend=False,
                          email=None,
                          username='joe123',
                          share_emails=[],
                          share_usernames=[],
                          copy_project=False,
                          include_paths=None,
                          exclude_paths=None,
                          msg_file=message_infile)
            cmd.run(myargs)
            args, kwargs = mock_d4s2_project().deliver.call_args
            project, new_project_name, to_user, share_users, force_send, path_filter, message = args
            self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
            self.assertEqual(False, force_send)
            self.assertIn('setup(', message)
            self.assertEqual(new_project_name, None)
            args, kwargs = mock_remote_store.return_value.fetch_remote_project.call_args
            self.assertEqual('456', args[0].get_id_or_raise())

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_share_users_good(self, mock_d4s2_project, mock_remote_store):
        mock_to_user = Mock()
        mock_share_user = Mock()
        mock_remote_store.return_value.lookup_or_register_user_by_email_or_username.return_value = mock_to_user
        mock_remote_store.return_value.get_or_register_user_by_username.return_value = mock_share_user
        cmd = DeliverCommand(MagicMock())
        cmd.get_new_project_name = Mock()
        cmd.get_new_project_name.return_value = 'NewProjectName'
        myargs = Mock(project_name='mouse',
                      project_id=None,
                      email=None,
                      resend=False,
                      username='joe123',
                      share_usernames=['joe456'],
                      share_emails=[],
                      copy_project=True,
                      include_paths=None,
                      exclude_paths=None,
                      msg_file=None)
        cmd.run(myargs)
        mock_d4s2_project.return_value.deliver.assert_called_with(
            mock_remote_store.return_value.fetch_remote_project.return_value,
            'NewProjectName',
            mock_to_user,
            [mock_share_user],
            False,
            ANY,
            ''
        )

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_share_users_invalid(self, mock_d4s2_project, mock_remote_store):
        mock_remote_user = Mock()
        mock_remote_store.return_value.lookup_or_register_user_by_email_or_username.return_value = mock_remote_user
        mock_remote_store.return_value.get_or_register_user_by_username.return_value = mock_remote_user
        cmd = DeliverCommand(MagicMock())
        cmd.get_new_project_name = Mock()
        cmd.get_new_project_name.return_value = 'NewProjectName'
        myargs = Mock(project_name='mouse',
                      project_id=None,
                      email=None,
                      resend=False,
                      username='joe123',
                      share_usernames=['joe123'],
                      share_emails=[],
                      copy_project=True,
                      include_paths=None,
                      exclude_paths=None,
                      msg_file=None)
        with self.assertRaises(ValueError) as raised_exception:
            cmd.run(myargs)
        self.assertEqual(str(raised_exception.exception), INVALID_DELIVERY_RECIPIENT_MSG)


class TestDDSClient(TestCase):
    def test_read_argument_file_contents(self):
        self.assertEqual('', read_argument_file_contents(None))
        with open("setup.py") as infile:
            self.assertIn("setup(", read_argument_file_contents(infile))


class TestListCommand(TestCase):
    @patch('sys.stdout.write')
    @patch('ddsc.ddsclient.RemoteStore')
    def test_print_project_list_details(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_projects_details.return_value = [
            {'name': 'one', 'id': '123'},
            {'name': 'two', 'id': '456'},
            {'name': 'three', 'id': '456'},
        ]
        cmd = ListCommand(MagicMock())
        cmd.print_project_list_details(filter_auth_role=None, long_format=False)
        expected_calls = [
            call("one"),
            call("\n"),
            call("two"),
            call("\n"),
            call("three"),
            call("\n")
        ]
        self.assertEqual(expected_calls, mock_print.call_args_list)

    @patch('sys.stdout.write')
    @patch('ddsc.ddsclient.RemoteStore')
    def test_print_project_list_details_long_format(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_projects_details.return_value = [
            {'name': 'one', 'id': '123'},
            {'name': 'two', 'id': '456'},
            {'name': 'three', 'id': '789'},
        ]
        cmd = ListCommand(MagicMock())
        cmd.print_project_list_details(filter_auth_role=None, long_format=True)
        expected_calls = [
            call("123\tone"),
            call("\n"),
            call("456\ttwo"),
            call("\n"),
            call("789\tthree"),
            call("\n")
        ]
        self.assertEqual(expected_calls, mock_print.call_args_list)

    @patch('sys.stdout.write')
    @patch('ddsc.ddsclient.RemoteStore')
    def test_pprint_project_list_details_with_auth_role(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_projects_with_auth_role.return_value = [
            {'name': 'mouse', 'id': '123'},
            {'name': 'ant', 'id': '456'},
        ]
        cmd = ListCommand(MagicMock())
        cmd.print_project_list_details(filter_auth_role='project_admin', long_format=False)
        expected_calls = [
            call("mouse"),
            call("\n"),
            call("ant"),
            call("\n")
        ]
        self.assertEqual(expected_calls, mock_print.call_args_list)

    @patch('sys.stdout.write')
    @patch('ddsc.ddsclient.RemoteStore')
    def test_print_project_list_details_with_auth_role_long_format(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_projects_with_auth_role.return_value = [
            {'name': 'mouse', 'id': '123'},
            {'name': 'ant', 'id': '456'},
        ]
        cmd = ListCommand(MagicMock())
        cmd.print_project_list_details(filter_auth_role='project_admin', long_format=True)
        expected_calls = [
            call("123\tmouse"),
            call("\n"),
            call("456\tant"),
            call("\n")
        ]
        self.assertEqual(expected_calls, mock_print.call_args_list)

    def test_get_project_info_line(self):
        project_dict = {
            'id': '123',
            'name': 'mouse'
        }
        self.assertEqual('mouse', ListCommand.get_project_info_line(project_dict, False))
        self.assertEqual('123\tmouse', ListCommand.get_project_info_line(project_dict, True))


class TestClientCommand(TestCase):
    @patch('ddsc.ddsclient.Client')
    def test_get_project_by_name_or_id__with_name(self, mock_client):
        client_command = ClientCommand(config=Mock())
        project = client_command.get_project_by_name_or_id(Mock(project_name='mouse', project_id=None))
        self.assertEqual(client_command.client, mock_client.return_value)
        self.assertEqual(project, mock_client.return_value.get_project_by_name.return_value)
        mock_client.return_value.get_project_by_name.assert_called_with('mouse')

    @patch('ddsc.ddsclient.Client')
    def test_get_project_by_name_or_id__with_id(self, mock_client):
        client_command = ClientCommand(config=Mock())
        project = client_command.get_project_by_name_or_id(Mock(project_name=None, project_id='123abc'))
        self.assertEqual(client_command.client, mock_client.return_value)
        self.assertEqual(project, mock_client.return_value.get_project_by_id.return_value)
        mock_client.return_value.get_project_by_id.assert_called_with('123abc')


class TestMoveCommand(TestCase):
    @patch('ddsc.ddsclient.Client')
    def test_run(self, mock_client):
        move_command = MoveCommand(config=Mock())
        move_command.run(Mock(project_name='mouse', project_id=None,
                              source_remote_path='/data/file1.txt',
                              target_remote_path='/data/file1.sv.txt'))
        mock_project = mock_client.return_value.get_project_by_name.return_value
        mock_project.move_file_or_folder.assert_called_with('/data/file1.txt', '/data/file1.sv.txt')


class TestInfoCommand(TestCase):
    @patch('ddsc.ddsclient.Client')
    @patch('ddsc.ddsclient.print')
    def test_run(self, mock_print, mock_client):
        mock_project = mock_client.return_value.get_project_by_name.return_value
        mock_project.name = "Mouse"
        mock_project.id = "1234"
        mock_project.portal_url.return_value = "someurl"
        mock_project.get_summary.return_value = '3 top level folders, 0 subfolders, 1 file (12 KiB)'
        size_command = InfoCommand(config=Mock())
        size_command.run(Mock(project_name='mouse'))
        mock_print.assert_has_calls([
            call(),
            call("Name: Mouse"),
            call("ID: 1234"),
            call("URL: someurl"),
            call("Size: 3 top level folders, 0 subfolders, 1 file (12 KiB)"),
            call(),
        ])
