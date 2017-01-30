from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.upload import ProjectUpload
from mock import MagicMock, patch


class TestUploadCommand(TestCase):
    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.ProjectUpload")
    @patch("ddsc.core.upload.ProjectUploadDryRun")
    def test_nothing_to_do(self, MockProjectUploadDryRun, MockProjectUpload, MockRemoteStore):
        MockProjectUploadDryRun().upload_items = []
        project_upload = ProjectUpload(MagicMock(), "someProject", ["data"])
        dry_run_report = project_upload.dry_run_report()
        self.assertEqual("No changes found. Nothing needs to be uploaded.", dry_run_report)

    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.ProjectUpload")
    @patch("ddsc.core.upload.ProjectUploadDryRun")
    def test_two_files_to_upload(self, MockProjectUploadDryRun, MockProjectUpload, MockRemoteStore):
        MockProjectUploadDryRun().upload_items = ['data.txt','data2.txt']
        project_upload = ProjectUpload(MagicMock(), "someProject", ["data"])
        dry_run_report = project_upload.dry_run_report()
        self.assertIn("Files/Folders that need to be uploaded:", dry_run_report)
        self.assertIn("data.txt", dry_run_report)
        self.assertIn("data2.txt", dry_run_report)
