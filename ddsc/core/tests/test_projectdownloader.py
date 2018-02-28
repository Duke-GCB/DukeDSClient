from unittest import TestCase
import os
from ddsc.core.projectdownloader import FileUrls
from mock import MagicMock, patch, call

#
# class TestFileUrls(TestCase):
#     @staticmethod
#     def mock_file_url(parent_path):
#         file_url = MagicMock()
#         file_url.get_remote_parent_path.return_value = parent_path
#         return file_url
#
#     @patch('ddsc.core.projectdownloader.os')
#     def test_make_directories(self, mock_os):
#         mock_os.path.join = os.path.join
#         mock_os.path.exists.return_value = False
#         project = MagicMock()
#         file_urls = [
#             self.mock_file_url(parent_path='data/results/first_pass'),
#             self.mock_file_url(parent_path='docs'),
#             self.mock_file_url(parent_path='docs'),
#             self.mock_file_url(parent_path='docs/images'),
#         ]
#
#         pd = FileUrls(file_urls)
#         pd.make_local_directories()
#
#         mock_os.path.makedirs.assert_has_calls([
#             call('/tmp/myproject/data/results/first_pass'),
#             call('/tmp/myproject/docs'),
#             call('/tmp/myproject/docs/images')
#         ], any_order=True)

#    def test_stuff(self):
#        self.assertEqual(1, 2)
