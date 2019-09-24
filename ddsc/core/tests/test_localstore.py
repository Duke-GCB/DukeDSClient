import shutil
import tarfile
from unittest import TestCase
from ddsc.core.localstore import LocalFile, LocalFolder, LocalProject, KindType, FileResendChecker
from ddsc.core.util import ChecksumSizeLimit, parse_checksum_size_limit
from mock import patch, Mock


INCLUDE_ALL = ''


def get_file_or_folder_paths(item, prefix=""):
    results = []
    item_path = ""
    if item.kind != KindType.project_str:
        item_path = "{}{}".format(prefix, item.name)
        results.append(item_path)
    if item.kind != KindType.file_str:
        for child in item.children:
            results.extend(get_file_or_folder_paths(child, item_path + "/"))
    return sorted(results)


class TestProjectFolderFile(TestCase):
    def test_file_str(self):
        f = LocalFile('setup.py')
        self.assertEqual(get_file_or_folder_paths(f), ['setup.py'])

    def test_empty_folder_str(self):
        f = LocalFolder('stuff')
        self.assertEqual(get_file_or_folder_paths(f), ['stuff'])

    def test_folder_one_child_str(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('setup.py'))
        self.assertEqual(get_file_or_folder_paths(folder), ['stuff', 'stuff/setup.py'])

    def test_folder_two_children_str(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('setup.py'))
        folder.add_child(LocalFile('requirements.txt'))
        self.assertEqual(get_file_or_folder_paths(folder), [
            'stuff',
            'stuff/requirements.txt',
            'stuff/setup.py',

        ])

    def test_nested_folder_str(self):
        grand = LocalFolder('grand')
        parent = LocalFolder('parent')
        parent.add_child(LocalFile('setup.py'))
        parent.add_child(LocalFile('requirements.txt'))
        otherparent = LocalFolder('otherparent')
        grand.add_child(parent)
        grand.add_child(otherparent)
        self.assertEqual(get_file_or_folder_paths(grand), [
            'grand',
            'grand/otherparent',
            'grand/parent',
            'grand/parent/requirements.txt',
            'grand/parent/setup.py',
        ])


class TestProjectContent(TestCase):
    """
    These tests exercise code that interacts with the file system.
    We extract ddsc/core/tests/testfolder.tar to temp and tests are specific to that tar file.
    """
    @classmethod
    def setUpClass(cls):
        test_folder = tarfile.TarFile('ddsc/core/tests/testfolder.tar')
        test_folder.extractall('/tmp')
        test_folder.close()

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('/tmp/DukeDsClientTestFolder')

    def test_folder_dot_name(self):
        content = LocalFolder('.')
        self.assertEqual('DukeDSClient', content.name)

    def test_folder_name_removes_slash(self):
        content = LocalFolder('/tmp/DukeDsClientTestFolder/')
        self.assertEqual('DukeDsClientTestFolder', content.name)
        self.assertEqual('/tmp/DukeDsClientTestFolder', content.path)

    def test_folder_name_no_slash(self):
        content = LocalFolder('/tmp/DukeDsClientTestFolder')
        self.assertEqual('DukeDsClientTestFolder', content.name)
        self.assertEqual('/tmp/DukeDsClientTestFolder', content.path)

    def test_folder_up_and_back(self):
        content = LocalFolder('../DukeDSClient')
        self.assertEqual('DukeDSClient', content.name)

    def test_empty_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        self.assertEqual(get_file_or_folder_paths(content), [])

    def test_top_level_file_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEqual(get_file_or_folder_paths(content), [
            '/note.txt'
        ])

    def test_empty_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        self.assertEqual(get_file_or_folder_paths(content), [
            '/emptyfolder'
        ])

    def test_empty_folder_and_file_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEqual(get_file_or_folder_paths(content), [
            '/emptyfolder',
            '/note.txt'
        ])

    def test_one_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/scripts')
        self.assertEqual(get_file_or_folder_paths(content), [
            '/scripts',
            '/scripts/makemoney.sh',
        ])

    def test_nested_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/results')
        self.assertEqual(get_file_or_folder_paths(content), [
            '/results',
            '/results/result1929.txt',
            '/results/result2929.txt',
            '/results/subresults',
            '/results/subresults/result1002.txt',
            '/results/subresults/result13.txt',
            '/results/subresults/result15.txt',
            '/results/subresults2',
        ])

    def test_big_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder')
        child_names = [child.name for child in content.children[0].children]
        self.assertEqual(set(['note.txt', 'emptyfolder', 'results', 'scripts']), set(child_names))
        self.assertEqual(get_file_or_folder_paths(content), [
            '/DukeDsClientTestFolder',
            '/DukeDsClientTestFolder/emptyfolder',
            '/DukeDsClientTestFolder/note.txt',
            '/DukeDsClientTestFolder/results',
            '/DukeDsClientTestFolder/results/result1929.txt',
            '/DukeDsClientTestFolder/results/result2929.txt',
            '/DukeDsClientTestFolder/results/subresults',
            '/DukeDsClientTestFolder/results/subresults/result1002.txt',
            '/DukeDsClientTestFolder/results/subresults/result13.txt',
            '/DukeDsClientTestFolder/results/subresults/result15.txt',
            '/DukeDsClientTestFolder/results/subresults2',
            '/DukeDsClientTestFolder/scripts',
            '/DukeDsClientTestFolder/scripts/makemoney.sh'
        ])

    def test_include_dot_files(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('test_scripts')
        self.assertIn('/test_scripts/.hidden_file', get_file_or_folder_paths(content))

    def test_exclude_dot_files(self):
        content = LocalProject(False, file_exclude_regex='^\.')
        content.add_path('test_scripts')
        self.assertNotIn('/test_scripts/.hidden_file', get_file_or_folder_paths(content))

    def test_ignore_one_dir(self):
        with open("/tmp/DukeDsClientTestFolder/.ddsignore", "w") as text_file:
            text_file.write("emptyfolder")
        content = LocalProject(False, file_exclude_regex='^\.')
        content.add_path('test_scripts')
        self.assertNotIn('.hidden_file', str(content))
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder')
        child_names = [child.name for child in content.children[0].children]
        self.assertEqual(set(['.ddsignore', 'note.txt', 'results', 'scripts']), set(child_names))


class TestLocalFile(TestCase):
    @patch('ddsc.core.localstore.os')
    @patch('ddsc.core.localstore.PathData')
    def test_count_chunks_values(self, mock_path_data, mock_os):
        values = [
            # file_size, bytes_per_chunk, expected
            (200, 10, 20),
            (200, 150, 2),
            (3, 150, 1),
            (0, 10, 1),  # Empty files must send 1 empty chunk to DukeDS
        ]
        f = LocalFile('fakefile.txt')
        for file_size, bytes_per_chunk, expected in values:
            f.size = file_size
            self.assertEqual(expected, f.count_chunks(bytes_per_chunk))


class TestLocalProject(TestCase):
    def setUp(self):
        self.remote_file1 = Mock()
        self.remote_file1.name = 'data.txt'
        self.remote_file1.id = 'abc123'
        self.remote_file1.is_file = True
        self.remote_project = Mock()
        self.remote_project.children = []
        self.local_file1 = Mock()
        self.local_file1.name = 'data.txt'
        self.local_file1.remote_id = None
        self.local_file1.need_to_send = True

    @patch('ddsc.core.localstore.FileResendChecker')
    def test_update_with_remote_project__not_found(self, mock_file_resend_checker):
        mock_file_resend_checker.return_value.need_to_send.return_value = True
        project = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        self.local_file1.name = 'data2.txt'
        project.children = [self.local_file1]
        self.remote_project.children.append(self.remote_file1)

        project.update_with_remote_project(self.remote_project, parse_checksum_size_limit('10MB'))

        self.assertEqual(self.local_file1.remote_id, None)
        self.assertEqual(self.local_file1.need_to_send, True)

    @patch('ddsc.core.localstore.FileResendChecker')
    def test_update_with_remote_project__need_to_send(self, mock_file_resend_checker):
        mock_file_resend_checker.return_value.need_to_send.return_value = True
        project = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        project.children = [self.local_file1]
        self.remote_project.children.append(self.remote_file1)

        project.update_with_remote_project(self.remote_project, parse_checksum_size_limit('10MB'))

        self.assertEqual(self.local_file1.remote_id, 'abc123')
        self.assertEqual(self.local_file1.need_to_send, True)

    @patch('ddsc.core.localstore.FileResendChecker')
    def test_update_with_remote_project__nested_file_need_to_send(self, mock_file_resend_checker):
        mock_file_resend_checker.return_value.need_to_send.return_value = True
        project = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        local_folder = Mock()
        local_folder.children = [self.local_file1]
        local_folder.is_file = False
        local_folder.name = 'files'
        project.children = [local_folder]

        remote_folder = Mock()
        remote_folder.name = 'files'
        remote_folder.children = [self.remote_file1]
        self.remote_project.children.append(remote_folder)

        project.update_with_remote_project(self.remote_project, parse_checksum_size_limit('10MB'))

        self.assertEqual(self.local_file1.remote_id, 'abc123')
        self.assertEqual(self.local_file1.need_to_send, True)

    @patch('ddsc.core.localstore.FileResendChecker')
    def test_update_with_remote_project__doesnt_need_to_send(self, mock_file_resend_checker):
        mock_file_resend_checker.return_value.need_to_send.return_value = False
        project = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        project.children = [self.local_file1]
        self.remote_project.children.append(self.remote_file1)

        project.update_with_remote_project(self.remote_project, parse_checksum_size_limit('10MB'))

        self.assertEqual(self.local_file1.remote_id, 'abc123')
        self.assertEqual(self.local_file1.need_to_send, False)


class TestFileResendChecker(TestCase):
    def setUp(self):
        self.checker = FileResendChecker(ChecksumSizeLimit(file_size_limit=100))

    def test_need_to_send__different_sizes(self):
        self.assertTrue(self.checker.need_to_send(local_file=Mock(size=100), remote_file=Mock(size=200)))

    def test_need_to_send__no_check_hash(self):
        self.checker.should_check_hash = Mock()
        self.checker.should_check_hash.return_value = False
        self.assertFalse(self.checker.need_to_send(local_file=Mock(size=100), remote_file=Mock(size=100)))

    def test_need_to_send__hash_matches(self):
        self.checker.should_check_hash = Mock()
        self.checker.should_check_hash.return_value = True
        self.checker.hash_matches = Mock()
        self.checker.hash_matches.return_value = True
        self.assertFalse(self.checker.need_to_send(local_file=Mock(size=100), remote_file=Mock(size=100)))

    def test_need_to_send__hash_doesnt_matches(self):
        self.checker.should_check_hash = Mock()
        self.checker.should_check_hash.return_value = True
        self.checker.hash_matches = Mock()
        self.checker.hash_matches.return_value = False
        self.assertTrue(self.checker.need_to_send(local_file=Mock(size=100), remote_file=Mock(size=100)))

    def test_hash_matches(self):
        mock_local_file = Mock()
        mock_remote_file = Mock(hash_alg='md5', file_hash='abc')
        result = FileResendChecker.hash_matches(mock_local_file, mock_remote_file)
        mock_hash_data = mock_local_file.path_data.get_hash.return_value
        self.assertEqual(result, mock_hash_data.matches.return_value)
        mock_hash_data.matches.assert_called_with('md5', 'abc')
