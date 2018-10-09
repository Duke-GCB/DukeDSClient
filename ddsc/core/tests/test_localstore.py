import shutil
import tarfile
from unittest import TestCase, skip
from ddsc.core.localstore import LocalFile, LocalFolder, LocalProject
from mock import patch


INCLUDE_ALL = ''


class TestProjectFolderFile(TestCase):
    def test_file_str(self):
        f = LocalFile('setup.py')
        self.assertEqual('file:setup.py', str(f))

    def test_empty_folder_str(self):
        f = LocalFolder('stuff')
        self.assertEqual('folder:stuff []', str(f))

    def test_folder_one_child_str(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('setup.py'))
        self.assertEqual('folder:stuff [file:setup.py]', str(folder))

    def test_folder_two_children_str(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('setup.py'))
        folder.add_child(LocalFile('requirements.txt'))
        self.assertEqual('folder:stuff [file:setup.py, file:requirements.txt]', str(folder))

    def test_nested_folder_str(self):
        grand = LocalFolder('grand')
        parent = LocalFolder('parent')
        parent.add_child(LocalFile('setup.py'))
        parent.add_child(LocalFile('requirements.txt'))
        otherparent = LocalFolder('otherparent')
        grand.add_child(parent)
        grand.add_child(otherparent)
        self.assertEqual(('folder:grand ['
                          'folder:parent [file:setup.py, file:requirements.txt], '
                          'folder:otherparent []]'), str(grand))


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
        self.assertEqual('project: []', str(content))

    def test_top_level_file_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEqual('project: [file:note.txt]', str(content))

    def test_empty_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        self.assertEqual('project: [folder:emptyfolder []]', str(content))

    def test_empty_folder_and_file_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEqual('project: [folder:emptyfolder [], file:note.txt]', str(content))

    def test_one_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/scripts')
        self.assertEqual('project: [folder:scripts [file:makemoney.sh]]', str(content))

    @skip(reason="Fragile test breaks do to item sorting differences on travis")
    def test_nested_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder/results')
        self.assertEqual(('project: [folder:results ['
                          'file:result1929.txt, '
                          'file:result2929.txt, '
                          'folder:subresults [file:result1002.txt, file:result13.txt, file:result15.txt], '
                          'folder:subresults2 []'
                          ']]'), str(content))

    @skip(reason="Fragile test breaks do to item sorting differences on travis")
    def test_big_folder_str(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('/tmp/DukeDsClientTestFolder')
        child_names = [child.name for child in content.children[0].children]
        self.assertEqual(set(['note.txt', 'emptyfolder', 'results', 'scripts']), set(child_names))
        self.assertEqual(('project: [folder:DukeDsClientTestFolder ['
                          'file:note.txt, '
                          'folder:emptyfolder [], '
                          'folder:results ['
                          'file:result1929.txt, file:result2929.txt, folder:subresults '
                          '[file:result1002.txt, file:result13.txt, file:result15.txt], '
                          'folder:subresults2 []'
                          '], '
                          'folder:scripts ['
                          'file:makemoney.sh'
                          ']'
                          ']]'), str(content))

    def test_include_dot_files(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('test_scripts')
        self.assertIn('.hidden_file', str(content))

    def test_exclude_dot_files(self):
        content = LocalProject(False, file_exclude_regex='^\.')
        content.add_path('test_scripts')
        self.assertNotIn('.hidden_file', str(content))

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
