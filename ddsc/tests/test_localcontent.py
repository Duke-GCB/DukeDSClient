from unittest import TestCase
import os
import shutil
import tarfile
from ddsc.localcontent import LocalFile, LocalFolder, LocalContent


class TestProjectFolderFile(TestCase):
    def test_file_repr(self):
        f = LocalFile('notes.txt')
        self.assertEquals('file:notes.txt', repr(f))

    def test_empty_folder_repr(self):
        f = LocalFolder('stuff')
        self.assertEquals('folder:stuff []', repr(f))

    def test_folder_one_child_repr(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('notes.txt'))
        self.assertEquals('folder:stuff [file:notes.txt]', repr(folder))

    def test_folder_two_children_repr(self):
        folder = LocalFolder('stuff')
        folder.add_child(LocalFile('notes.txt'))
        folder.add_child(LocalFile('notes2.txt'))
        self.assertEquals('folder:stuff [file:notes.txt, file:notes2.txt]', repr(folder))

    def test_nested_folder__repr(self):
        grand = LocalFolder('grand')
        parent = LocalFolder('parent')
        parent.add_child(LocalFile('child.txt'))
        parent.add_child(LocalFile('child2.txt'))
        otherparent = LocalFolder('otherparent')
        grand.add_child(parent)
        grand.add_child(otherparent)
        self.assertEquals(('folder:grand ['
                           'folder:parent [file:child.txt, file:child2.txt], '
                           'folder:otherparent []]'), repr(grand))


class TestProjectContent(TestCase):
    """
    These tests exercise code that interacts with the file system.
    We extract ddsc/tests/testfolder.tar to temp and tests are specific to that tar file.
    """
    @classmethod
    def setUpClass(cls):
        test_folder = tarfile.TarFile('ddsc/tests/testfolder.tar')
        test_folder.extractall('/tmp')

    @classmethod
    def tearDownClass(cls):
        shutil.rmtree('/tmp/DukeDsClientTestFolder')

    def test_folder_dot_name(self):
        content = LocalFolder('.')
        self.assertEquals('DukeDSClient', content.name)

    def test_folder_name_removes_slash(self):
        content = LocalFolder('/tmp/DukeDsClientTestFolder/')
        self.assertEquals('DukeDsClientTestFolder', content.name)
        self.assertEquals('/tmp/DukeDsClientTestFolder', content.path)

    def test_folder_name_no_slash(self):
        content = LocalFolder('/tmp/DukeDsClientTestFolder')
        self.assertEquals('DukeDsClientTestFolder', content.name)
        self.assertEquals('/tmp/DukeDsClientTestFolder', content.path)

    def test_folder_up_and_back(self):
        content = LocalFolder('../DukeDSClient')
        self.assertEquals('DukeDSClient', content.name)

    def test_empty_repr(self):
        content = LocalContent()
        self.assertEquals('project: []', repr(content))

    def test_top_level_file_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEquals('project: [file:note.txt]', repr(content))

    def test_empty_folder_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        self.assertEquals('project: [folder:emptyfolder []]', repr(content))

    def test_empty_folder_and_file_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder/emptyfolder')
        content.add_path('/tmp/DukeDsClientTestFolder/note.txt')
        self.assertEquals('project: [folder:emptyfolder [], file:note.txt]', repr(content))

    def test_one_folder_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder/scripts')
        self.assertEquals('project: [folder:scripts [file:makemoney.sh]]', repr(content))

    def test_nested_folder_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder/results')
        self.assertEquals(('project: [folder:results ['
                           'file:result1929.txt, '
                           'file:result2929.txt, '
                           'folder:subresults [file:result1002.txt, file:result13.txt, file:result15.txt], '
                           'folder:subresults2 []'
                           ']]'), repr(content))

    def test_big_folder_repr(self):
        content = LocalContent()
        content.add_path('/tmp/DukeDsClientTestFolder')
        self.assertEquals(('project: [folder:DukeDsClientTestFolder ['
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
                           ']]'), repr(content))
