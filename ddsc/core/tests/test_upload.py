from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.upload import ProjectUpload
from mock import patch, Mock


class TestProjectUpload(TestCase):
    @patch("ddsc.core.upload.RemoteStore")
    @patch("ddsc.core.upload.LocalProject")
    def test_get_url_msg(self, mock_local_project, mock_remote_store):
        project_upload = ProjectUpload(config=Mock(), project_name_or_id=Mock(), local_project=Mock(),
                                       items_to_send_count=None, file_upload_post_processor=None)
        mock_remote_store.return_value.data_service.portal_url.return_value = 'https://127.0.0.1/#/project/123'
        self.assertEqual(project_upload.get_url_msg(), 'URL to view project: https://127.0.0.1/#/project/123')
