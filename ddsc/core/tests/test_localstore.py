import shutil
import tarfile
from unittest import TestCase
from ddsc.core.localstore import LocalFile, LocalFolder, LocalProject, KindType, LocalItemsCounter, ItemsToSendCounter
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

    @patch('ddsc.core.localstore.isfile')
    def test_top_level_non_regular_file(self, mock_isfile):
        mock_isfile.return_value = False
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        with self.assertRaises(ValueError) as raised_exception:
            content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEqual(str(raised_exception.exception),
                         'Unsupported type of file /tmp/DukeDsClientTestFolder/note.txt')

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

    @patch('ddsc.core.localstore.isfile')
    def test_one_folder_containing_non_regular_file(self, mock_isfile):
        mock_isfile.return_value = False
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        with self.assertRaises(ValueError) as raised_exception:
            content.add_path('/tmp/DukeDsClientTestFolder/scripts')
        self.assertEqual(str(raised_exception.exception),
                         'Unsupported type of file /tmp/DukeDsClientTestFolder/scripts/makemoney.sh')

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

    @patch('ddsc.core.localstore.os')
    @patch('ddsc.core.localstore.PathData')
    def test_set_remote_values_after_send(self, mock_path_data, mock_os):
        f = LocalFile('fakefile.txt')
        self.assertEqual(f.remote_id, '')
        self.assertEqual(f.remote_file_hash_alg, None)
        self.assertEqual(f.remote_file_hash, None)
        self.assertEqual(f.sent_to_remote, False)
        f.set_remote_values_after_send(
            remote_id='abc123',
            remote_hash_alg='md5',
            remote_file_hash='defjkl'
        )
        self.assertEqual(f.remote_id, 'abc123')
        self.assertEqual(f.remote_file_hash_alg, 'md5')
        self.assertEqual(f.remote_file_hash, 'defjkl')
        self.assertEqual(f.sent_to_remote, True)


class TestLocalItemsCounter(TestCase):
    def test_to_str(self):
        local_project = Mock()
        local_project.children = [
            Mock(kind='dds-file'),
            Mock(kind='dds-folder', children=[
                Mock(kind='dds-file'),
            ]),
        ]
        counter = LocalItemsCounter(local_project)
        self.assertEqual(counter.to_str(prefix="Checking"), 'Checking 2 files and 1 folder.')


class TestItemsToSendCounter(TestCase):
    def test_to_str(self):
        local_project = Mock(kind='dds-project')
        mock_file1 = Mock(kind='dds-file')
        mock_file1.count_chunks.return_value = 10
        mock_file2 = Mock(kind='dds-file')
        mock_file2.count_chunks.return_value = 10
        local_project.children = [
            mock_file1,
            Mock(kind='dds-folder', children=[
                mock_file2,
            ]),
        ]
        counter = ItemsToSendCounter(local_project, bytes_per_chunk=100)
        counter_str = counter.to_str(prefix="Synchronizing", local_items_count=Mock(files=3, folders=3))
        self.assertEqual(counter_str,
                         'Synchronizing 1 new file, 2 existing files, 2 new folders and 1 existing folder.')
