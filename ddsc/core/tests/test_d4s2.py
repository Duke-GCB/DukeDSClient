from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.d4s2 import D4S2Project, CopyActivity, DownloadedFileRelations, UploadedFileRelations, \
    ShareWithSelfError, SHARE_WITH_SELF_MESSAGE, UserMissingEmailError, USER_WITHOUT_EMAIL_MESSAGE
from ddsc.core.util import KindType
from ddsc.core.pathfilter import PathFilter
from mock import patch, MagicMock, Mock


class TestD4S2Project(TestCase):
    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_share(self, mock_requests, mock_d4s2api):
        mock_d4s2api().get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        project.share(project=Mock(name='mouserna'),
                      to_user=MagicMock(id='123'),
                      force_send=False,
                      auth_role='project_viewer',
                      user_message='This is a test.')
        args, kwargs = mock_d4s2api().create_item.call_args
        item = args[0]
        self.assertEqual(mock_d4s2api.SHARE_DESTINATION, item.destination)
        self.assertEqual('123', item.to_user_id)
        self.assertEqual('project_viewer', item.auth_role)
        self.assertEqual('This is a test.', item.user_message)
        mock_d4s2api().send_item.assert_called()

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_share_with_self(self, mock_requests, mock_d4s2api):
        mock_d4s2api.return_value.get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        mock_requests.get.return_value = None
        mock_remote_store = MagicMock()
        mock_current_user = MagicMock()
        mock_current_user.id = '123'
        mock_to_user = MagicMock()
        mock_to_user.id = '123'
        mock_remote_store.get_current_user.return_value = mock_current_user
        project = D4S2Project(config=MagicMock(), remote_store=mock_remote_store, print_func=MagicMock())
        with self.assertRaises(ShareWithSelfError) as raised_error:
            project.share(project=Mock(name='mouserna'),
                          to_user=mock_to_user,
                          force_send=False,
                          auth_role='project_viewer',
                          user_message='This is a test.')
        self.assertEqual(str(raised_error.exception), SHARE_WITH_SELF_MESSAGE.format('share'))

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_share_to_bad_email(self, mock_requests, mock_d4s2api):
        mock_d4s2api().get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        with self.assertRaises(UserMissingEmailError) as raised_error:
            project.share(project=Mock(name='mouserna'),
                          to_user=MagicMock(id='123', email=None),
                          force_send=False,
                          auth_role='project_viewer',
                          user_message='This is a test.')
        self.assertEqual(str(raised_error.exception), USER_WITHOUT_EMAIL_MESSAGE.format('share', 'share'))

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_deliver(self, mock_requests, mock_d4s2api):
        mock_d4s2api().get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        project.deliver(project=Mock(name='mouserna'),
                        new_project_name=None,
                        to_user=MagicMock(id='456'),
                        share_users=[Mock(id='777'), Mock(id='888')],
                        force_send=False,
                        path_filter='',
                        user_message='Yet Another Message.')
        args, kwargs = mock_d4s2api().create_item.call_args
        item = args[0]
        self.assertEqual(mock_d4s2api.DELIVER_DESTINATION, item.destination)
        self.assertEqual('456', item.to_user_id)
        self.assertEqual('Yet Another Message.', item.user_message)
        self.assertEqual(['777', '888'], item.share_user_ids)
        mock_d4s2api().send_item.assert_called()

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_deliver_with_self(self, mock_requests, mock_d4s2api):
        mock_requests.get.return_value = None
        mock_remote_store = MagicMock()
        mock_current_user = MagicMock()
        mock_current_user.id = '123'
        mock_to_user = MagicMock()
        mock_to_user.id = '123'
        mock_remote_store.get_current_user.return_value = mock_current_user
        project = D4S2Project(config=MagicMock(), remote_store=mock_remote_store, print_func=MagicMock())
        with self.assertRaises(ShareWithSelfError) as raised_error:
            project.deliver(project=Mock(name='mouserna'),
                            new_project_name=None,
                            to_user=mock_to_user,
                            share_users=[Mock(id='777'), Mock(id='888')],
                            force_send=False,
                            path_filter='',
                            user_message='Yet Another Message.')
        self.assertEqual(str(raised_error.exception), SHARE_WITH_SELF_MESSAGE.format('deliver'))

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_deliver_to_bad_email(self, mock_requests, mock_d4s2api):
        mock_d4s2api.return_value.get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        with self.assertRaises(UserMissingEmailError) as raised_error:
            project.deliver(project=Mock(name='mouserna'),
                            new_project_name=None,
                            to_user=MagicMock(id='456', email=None),
                            share_users=[Mock(id='777'), Mock(id='888')],
                            force_send=False,
                            path_filter='',
                            user_message='Yet Another Message.')
        self.assertEqual(str(raised_error.exception), USER_WITHOUT_EMAIL_MESSAGE.format('deliver', 'deliver'))

    @patch('ddsc.core.d4s2.ProjectFileDownloader')
    @patch('ddsc.core.d4s2.Client')
    @patch('ddsc.core.d4s2.ProjectUpload')
    def test_copy_project(self, mock_project_upload, mock_client, mock_project_file_downloader):
        data_service = MagicMock()
        remote_store = MagicMock(data_service=data_service)
        remote_store.fetch_remote_project.return_value = None
        project = D4S2Project(config=MagicMock(), remote_store=remote_store, print_func=MagicMock())
        path_filter = PathFilter(include_paths=[], exclude_paths=[])
        project._copy_project(Mock(name='mouse'), 'new_mouse', path_filter)
        data_service.create_activity.assert_called()
        mock_project_file_downloader.return_value.run.assert_called_with()

        mock_project_upload.create_for_paths.assert_called()
        args, kwargs = mock_project_upload.create_for_paths.call_args
        file_upload_post_processor = kwargs['file_upload_post_processor']
        self.assertEqual(UploadedFileRelations, file_upload_post_processor.__class__)

    @patch('ddsc.core.d4s2.ProjectFileDownloader')
    @patch('ddsc.core.d4s2.Client')
    def test_download_project(self, mock_client, mock_project_file_downloader):
        data_service = MagicMock()
        remote_store = MagicMock(data_service=data_service)
        remote_store.fetch_remote_project.return_value = None
        project = D4S2Project(config=MagicMock(), remote_store=remote_store, print_func=MagicMock())
        project.client.get_project_by_id.return_value.get_path_to_files.return_value = {
            '/data/file.txt': Mock(current_version={'id': '123'})
        }
        activity = Mock(remote_path_to_file_version_id={})
        path_filter = PathFilter(include_paths=[], exclude_paths=[])
        project._download_project(activity, '123abc', '/tmp/data', path_filter)

        mock_project_file_downloader.return_value.run.assert_called_with()
        data_service.create_used_relation.assert_called_with(activity.id, 'dds-file', '123')
        self.assertEqual(activity.remote_path_to_file_version_id, {'/data/file.txt': '123'})


class TestCopyActivity(TestCase):
    def test_constructor_and_finished(self):
        data_service = MagicMock()
        data_service.create_activity().json().__getitem__.return_value = '1'
        new_project_name = "mouse_copy"

        # Constructor should create activity
        project = Mock()
        project.name = 'mouse'
        activity = CopyActivity(data_service, project, new_project_name)
        self.assertEqual('1', activity.id)
        data_service.create_activity.assert_called()
        args, kwargs = data_service.create_activity.call_args
        self.assertEqual("DukeDSClient copying project: mouse", args[0])
        self.assertIn("Copying mouse to project mouse_copy", args[1])
        self.assertIsNotNone(kwargs['started_on'])

        # finished method should update and fill in the ended_on date
        activity.finished()
        data_service.update_activity.assert_called()
        args, kwargs = data_service.update_activity.call_args
        self.assertEqual('1', args[0])
        self.assertEqual("DukeDSClient copying project: mouse", args[1])
        self.assertIn("Copying mouse to project mouse_copy", args[2])
        self.assertIsNotNone(kwargs['started_on'])
        self.assertIsNotNone(kwargs['ended_on'])


class TestDownloadedFileRelations(TestCase):
    def test_run(self):
        new_activity_id = '2'
        file_version_id = '32'
        file_remote_path = 'data/results/seq1.fasta'

        data_service = MagicMock()
        data_service.create_activity().json().__getitem__.return_value = new_activity_id
        data_service.get_file.return_value.json.return_value = {'current_version': {'id': file_version_id}}
        activity = CopyActivity(data_service, Mock(name='mouse'), "mouse_copy")

        downloaded_file_relations = DownloadedFileRelations(activity)
        downloaded_file_relations.add(data_service, file_remote_path, file_version_id)

        # run should create a used relationship
        data_service.create_used_relation.assert_called()
        args, kwargs = data_service.create_used_relation.call_args
        self.assertEqual(new_activity_id, args[0])
        self.assertEqual(KindType.file_str, args[1])
        self.assertEqual(file_version_id, args[2])

        # run should save the file_version_id under the remote path
        self.assertEqual(file_version_id, activity.remote_path_to_file_version_id[file_remote_path])


class TestUploadedFileRelations(TestCase):
    def test_run(self):
        new_activity_id = '2'
        download_file_version_id = '32'
        upload_file_version_id = '33'
        file_remote_path = '/data/results/seq1.fasta'
        file_details = {
            'name': 'seq1.fasta',
            'current_version': {
                'id': upload_file_version_id
            },
            'ancestors': [
                {
                    'name': 'data',
                    'kind': KindType.folder_str
                },
                {
                    'name': 'results',
                    'kind': KindType.folder_str
                },
            ]
        }
        data_service = MagicMock()
        data_service.create_activity().json().__getitem__.return_value = new_activity_id
        activity = CopyActivity(data_service, Mock(name="mouse"), "mouse_copy")
        activity.remote_path_to_file_version_id[file_remote_path] = download_file_version_id

        uploaded_file_relations = UploadedFileRelations(activity)
        uploaded_file_relations.run(data_service, file_details)

        # run should create was generated by and was derived from relations
        data_service.create_was_generated_by_relation.assert_called_with(new_activity_id, KindType.file_str,
                                                                         upload_file_version_id)
        data_service.create_was_derived_from_relation.assert_called_with(download_file_version_id, KindType.file_str,
                                                                         upload_file_version_id, KindType.file_str)
