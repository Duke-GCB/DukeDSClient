from __future__ import absolute_import
from unittest import TestCase
from ddsc.ddsclient import UploadCommand, ListCommand
from ddsc.ddsclient import ShareCommand, DeliverCommand, read_argument_file_contents
from mock import patch, MagicMock, Mock, call


class TestUploadCommand(TestCase):
    @patch("ddsc.ddsclient.ProjectUpload")
    def test_without_dry_run(self, FakeProjectUpload):
        cmd = UploadCommand(MagicMock())
        args = Mock()
        args.project_name = "test"
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = False
        cmd.run(args)
        self.assertFalse(FakeProjectUpload().dry_run_report.called)
        self.assertTrue(FakeProjectUpload().get_upload_report.called)

    @patch("ddsc.ddsclient.ProjectUpload")
    def test_with_dry_run(self, FakeProjectUpload):
        cmd = UploadCommand(MagicMock())
        args = Mock()
        args.project_name = "test"
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = True
        cmd.run(args)
        self.assertTrue(FakeProjectUpload().dry_run_report.called)
        self.assertFalse(FakeProjectUpload().get_upload_report.called)


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

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_message(self, mock_d4s2_project, mock_remote_store):
        with open('setup.py') as message_infile:
            cmd = ShareCommand(MagicMock())
            myargs = Mock(project_name='mouse', email=None, username='joe123', force_send=False,
                          auth_role='project_viewer', msg_file=message_infile)
            cmd.run(myargs)
            args, kwargs = mock_d4s2_project().share.call_args
            project, to_user, force_send, auth_role, message = args
            self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
            self.assertEqual('project_viewer', auth_role)
            self.assertIn('setup(', message)


class TestDeliverCommand(TestCase):
    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_no_message(self, mock_d4s2_project, mock_remote_store):
        cmd = DeliverCommand(MagicMock())
        myargs = Mock(project_name='mouse',
                      email=None,
                      resend=False,
                      username='joe123',
                      skip_copy_project=True,
                      include_paths=None,
                      exclude_paths=None,
                      msg_file=None)
        cmd.run(myargs)
        args, kwargs = mock_d4s2_project().deliver.call_args
        project, new_project_name, to_user, force_send, path_filter, message = args
        self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
        self.assertEqual(False, force_send)
        self.assertEqual('', message)

    @patch('ddsc.ddsclient.RemoteStore')
    @patch('ddsc.ddsclient.D4S2Project')
    def test_run_message(self, mock_d4s2_project, mock_remote_store):
        with open('setup.py') as message_infile:
            cmd = DeliverCommand(MagicMock())
            myargs = Mock(project_name='mouse',
                          resend=False,
                          email=None,
                          username='joe123',
                          skip_copy_project=True,
                          include_paths=None,
                          exclude_paths=None,
                          msg_file=message_infile)
            cmd.run(myargs)
            args, kwargs = mock_d4s2_project().deliver.call_args
            project, new_project_name, to_user, force_send, path_filter, message = args
            self.assertEqual(project, mock_remote_store.return_value.fetch_remote_project.return_value)
            self.assertEqual(False, force_send)
            self.assertIn('setup(', message)


class TestDDSClient(TestCase):
    def test_read_argument_file_contents(self):
        self.assertEqual('', read_argument_file_contents(None))
        with open("setup.py") as infile:
            self.assertIn("setup(", read_argument_file_contents(infile))


class TestListCommand(TestCase):
    @patch('sys.stdout.write')
    @patch('ddsc.ddsclient.RemoteStore')
    def test_print_project_names_no_auth_role(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_project_names.return_value = ['one', 'two', 'three']
        cmd = ListCommand(MagicMock())
        cmd.print_project_names(filter_auth_role=None)
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
    def test_print_project_names_with_auth_role(self, mock_remote_store, mock_print):
        mock_remote_store.return_value.get_projects_with_auth_role.return_value = [
            {'name': 'mouse'},
            {'name': 'ant'},
        ]
        cmd = ListCommand(MagicMock())
        cmd.print_project_names(filter_auth_role='project_admin')
        expected_calls = [
            call("mouse"),
            call("\n"),
            call("ant"),
            call("\n")
        ]
        self.assertEqual(expected_calls, mock_print.call_args_list)
