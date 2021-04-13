from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.upload import ProjectUpload, UploadReport
from mock import patch, Mock


class TestProjectUpload(TestCase):
    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.LocalProject")
    def test_get_url_msg(self, mock_local_project, mock_remote_store):
        project_upload = ProjectUpload(config=Mock(), project_name_or_id=Mock(), local_project=Mock(),
                                       items_to_send_count=None, file_upload_post_processor=None, upload_workers=1)
        mock_remote_store.return_value.data_service.portal_url.return_value = 'https://127.0.0.1/#/project/123'
        self.assertEqual(project_upload.get_url_msg(), 'URL to view project: https://127.0.0.1/#/project/123')

    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.LocalProject")
    def test_create_for_paths(self, mock_local_project, mock_remote_store):
        config, remote_store, project_name_or_id, file_upload_post_processor = Mock(), Mock(), Mock(), Mock()
        paths = ['/tmp/data']
        project_upload = ProjectUpload.create_for_paths(config, remote_store, project_name_or_id, paths,
                                                        follow_symlinks=False,
                                                        file_upload_post_processor=file_upload_post_processor)
        self.assertEqual(project_upload.config, config)
        self.assertEqual(project_upload.local_project, mock_local_project.return_value)
        self.assertEqual(project_upload.items_to_send_count,
                         mock_local_project.return_value.count_items_to_send.return_value)

        remote_store.fetch_remote_project.assert_called_with(project_name_or_id)
        project_upload.local_project.update_remote_ids.assert_called_with(
            remote_store.fetch_remote_project.return_value
        )
        mock_local_project.return_value.add_paths.assert_called_with(['/tmp/data'])


class TestUploadReport(TestCase):
    def setUp(self):
        self.upload_report = UploadReport('mouse')
        self.upload_report.visit_project(Mock(remote_id='project1'))
        self.upload_report.visit_folder(Mock(path='data', remote_id='folder1', sent_to_remote=True), None)
        self.upload_report.visit_folder(Mock(path='code', remote_id='folder2', sent_to_remote=False), None)
        self.upload_report.visit_folder(Mock(path='results', remote_id='folder3', sent_to_remote=False), None)
        self.upload_report.visit_file(Mock(path='data/file1.txt', remote_id='file1', remote_file_hash='abc',
                                           size=1201, sent_to_remote=True), None)
        self.upload_report.visit_file(Mock(path='code/run.sh', remote_id='file2', sent_to_remote=False), None)
        self.upload_report.visit_file(Mock(path='code/clean.sh', remote_id='file3', sent_to_remote=False), None)

    def test_summary(self):
        self.assertEqual(self.upload_report.summary(),
                         'Uploaded 1 file and 1 folder. 2 files and 2 folders are already up to date.')

    @patch('ddsc.core.upload.datetime')
    def test_get_content(self, mock_datetime):
        mock_datetime.datetime.utcnow.return_value = '2019-10-08 13:56'
        expected_content = """
Upload Report for Project: 'mouse' 2019-10-08 13:56

SENT FILENAME     ID          SIZE    HASH
Project           project1
data              folder1
data/file1.txt    file1       1201    abc
        """.strip()
        self.assertEqual(self.upload_report.get_content(), expected_content)
