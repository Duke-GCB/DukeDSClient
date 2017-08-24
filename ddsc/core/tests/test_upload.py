from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.upload import ProjectUpload, LocalOnlyCounter
from ddsc.core.localstore import LocalFile
from ddsc.core.remotestore import ProjectNameOrId
from mock import MagicMock, patch


class TestUploadCommand(TestCase):
    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.ProjectUpload")
    @patch("ddsc.core.upload.ProjectUploadDryRun")
    def test_nothing_to_do(self, MockProjectUploadDryRun, MockProjectUpload, MockRemoteStore):
        MockProjectUploadDryRun().upload_items = []
        name_or_id = ProjectNameOrId.create_from_name("someProject")
        project_upload = ProjectUpload(MagicMock(), name_or_id, ["data"])
        dry_run_report = project_upload.dry_run_report()
        self.assertIn("No changes found. Nothing needs to be uploaded.", dry_run_report)

    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.ProjectUpload")
    @patch("ddsc.core.upload.ProjectUploadDryRun")
    def test_two_files_to_upload(self, MockProjectUploadDryRun, MockProjectUpload, MockRemoteStore):
        MockProjectUploadDryRun().upload_items = ['data.txt', 'data2.txt']
        name_or_id = ProjectNameOrId.create_from_name("someProject")
        project_upload = ProjectUpload(MagicMock(), name_or_id, ["data"])
        dry_run_report = project_upload.dry_run_report()
        self.assertIn("Files/Folders that need to be uploaded:", dry_run_report)
        self.assertIn("data.txt", dry_run_report)
        self.assertIn("data2.txt", dry_run_report)


class TestLocalOnlyCounter(TestCase):
    @patch('ddsc.core.localstore.os')
    @patch('ddsc.core.localstore.PathData')
    def test_total_items(self, mock_path_data, mock_os):
        counter = LocalOnlyCounter(bytes_per_chunk=100)
        self.assertEqual(0, counter.total_items())
        f = LocalFile('fakefile.txt')
        f.size = 0
        counter.visit_file(f, None)
        self.assertEqual(1, counter.total_items())
        f = LocalFile('fakefile2.txt')
        f.size = 200
        counter.visit_file(f, None)
        self.assertEqual(3, counter.total_items())
