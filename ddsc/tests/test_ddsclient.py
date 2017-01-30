from __future__ import absolute_import
from unittest import TestCase
from ddsc.ddsclient import UploadCommand
from mock import MagicMock, Mock, patch


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
    def test_without_dry_run(self, FakeProjectUpload):
        cmd = UploadCommand(MagicMock())
        args = Mock()
        args.project_name = "test"
        args.folders = ["data", "scripts"]
        args.follow_symlinks = False
        args.dry_run = True
        cmd.run(args)
        self.assertTrue(FakeProjectUpload().dry_run_report.called)
        self.assertFalse(FakeProjectUpload().get_upload_report.called)

