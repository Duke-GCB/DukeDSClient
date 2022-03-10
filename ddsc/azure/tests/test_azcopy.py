from unittest import TestCase
from mock import patch, Mock
from ddsc.azure.azcopy import create_azcopy_executable_path, find_azcopy_executable_path, group_by_dirname, AzCopy,\
    DDSAzureSetupException
import os.path


class TestAzcopyFunctions(TestCase):
    @patch('ddsc.azure.azcopy.platform')
    @patch('ddsc.azure.azcopy.os')
    def test_create_azcopy_executable_path_windows(self, mock_os, mock_platform):
        mock_os.path.join = os.path.join
        mock_os.environ = {"USERPROFILE": r"C:\Users\user1"}
        mock_platform.system.return_value = 'Windows'
        path = create_azcopy_executable_path()
        self.assertTrue(path.startswith(r"C:\Users\user1"))
        self.assertTrue(path.endswith("azcopy.exe"))

    @patch('ddsc.azure.azcopy.platform')
    def test_create_azcopy_executable_path_linux(self, mock_platform):
        mock_platform.system.return_value = 'Linux'
        path = create_azcopy_executable_path()
        self.assertTrue(path.endswith("bin/azcopy"))

    @patch('ddsc.azure.azcopy.shutil')
    @patch('ddsc.azure.azcopy.create_azcopy_executable_path')
    def test_find_azcopy_executable_path(self, mock_create_azcopy_executable_path, mock_shutil):
        mock_shutil.which.return_value = "/tmp/azcopy"
        self.assertEqual(find_azcopy_executable_path(), "/tmp/azcopy")
        mock_shutil.which.return_value = None
        self.assertEqual(find_azcopy_executable_path(), mock_create_azcopy_executable_path.return_value)

    def test_group_by_dirname(self):
        dirname_to_files = group_by_dirname([
            "/tmp/file1.txt",
            "/tmp/file2.txt",
            "/tmp/data/file3.txt",
            "/tmp/data/file4.txt",
            "/data/file5.txt",
        ])
        expected_result = {
            '/tmp': ['/tmp/file1.txt', '/tmp/file2.txt'],
            '/tmp/data': ['/tmp/data/file3.txt', '/tmp/data/file4.txt'],
            '/data': ['/data/file5.txt'],
        }
        self.assertEqual(dirname_to_files, expected_result)


class TestAzCopy(TestCase):
    def test_azcopy_not_found(self):
        az_copy = AzCopy(azcopy_executable=None, print_cmds=False)
        with self.assertRaises(DDSAzureSetupException):
            az_copy.download_directory(source="somebucket", destination="somedir", include_paths=[], exclude_paths=[])

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_upload_files(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.upload_files(source_parent_dir="/tmp/data",
                             source_filenames=["file1.txt", "file2.txt"],
                             destination="mybucket")
        expected = ['azcopy', 'sync', '/tmp/data', 'mybucket', '--put-md5', '--recursive=false', '--include-pattern',
                    'file1.txt;file2.txt']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_upload_files_dry_run(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)

        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.upload_files(source_parent_dir="/tmp/data",
                             source_filenames=["file1.txt", "file2.txt"],
                             destination="mybucket",
                             dry_run=True)
        expected = ['azcopy', 'sync', '/tmp/data', 'mybucket', '--put-md5', '--recursive=false',
                    '--include-pattern', 'file1.txt;file2.txt', '--dry-run']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_upload_directory(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.upload_directory(source="/tmp/data", destination="mybucket")
        expected = ['azcopy', 'sync', '/tmp/data', 'mybucket', '--put-md5']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_upload_directory_dry_run(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.upload_directory(source="/tmp/data", destination="mybucket", dry_run=True)
        expected = ['azcopy', 'sync', '/tmp/data', 'mybucket', '--put-md5', '--dry-run']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_download_directory(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.download_directory(source="mybucket", destination="/tmp/data", include_paths=[], exclude_paths=[])
        expected = ['azcopy', 'sync', 'mybucket', '/tmp/data', '--check-md5', 'FailIfDifferentOrMissing']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_download_directory_dry_run(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.download_directory(source="mybucket", destination="/tmp/data", dry_run=True,
                                   include_paths=[], exclude_paths=[])
        expected = ['azcopy', 'sync', 'mybucket', '/tmp/data', '--check-md5', 'FailIfDifferentOrMissing',
                    '--dry-run']
        mock_subprocess.run.assert_called_with(expected)

    @patch('ddsc.azure.azcopy.subprocess')
    def test_azcopy_download_directory_include_exclude_paths(self, mock_subprocess):
        mock_subprocess.run.return_value = Mock(returncode=0)
        az_copy = AzCopy(azcopy_executable="azcopy", print_cmds=False)
        az_copy.download_directory(source="mybucket", destination="/tmp/data", dry_run=False,
                                   include_paths=['data/good', 'data/good2'], exclude_paths=['data/bad', 'data/bad2'])
        expected = ['azcopy', 'sync', 'mybucket', '/tmp/data', '--check-md5', 'FailIfDifferentOrMissing',
                    '--include-regex', 'data/good;data/good2', '--exclude-regex', 'data/bad;data/bad2']
        mock_subprocess.run.assert_called_with(expected)
