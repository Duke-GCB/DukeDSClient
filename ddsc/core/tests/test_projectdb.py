import os
from unittest import TestCase
from ddsc.core.projectdb import ProjectDB, ProjectUtil
from ddsc.core.util import KindType
from ddsc.core.localstore import FileFilter

IN_MEMORY_STORAGE = 'sqlite:///:memory:'
INCLUDE_ALL = ''


class TestProjectDB(TestCase):
    def test_upload_single_file_project(self):
        """
        Simulate uploading a new project from /tmp/mouse named MouseRNA
        with data.txt file in /tmp/mouse with one chunk.
        """
        project_db = ProjectDB(IN_MEMORY_STORAGE)

        # Create project and file
        my_project = project_db.create_project(project_name='MouseRNA')
        my_file = project_db.add_file(parent=my_project, local_path='/tmp/mouse/data.txt', remote_path='data.txt')
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('MouseRNA', projects[0].name)
        self.assertEqual(None, projects[0].dds_id)
        self.assertEqual(1, len(projects[0].children))
        self.assertEqual('/tmp/mouse/data.txt', projects[0].children[0].local_path)
        self.assertEqual('data.txt', projects[0].children[0].remote_path)
        self.assertEqual(0, len(projects[0].children[0].chunks))

        # Pretend we upload the project
        my_project.dds_id = '123abc'
        project_db.commit_session()
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('MouseRNA', projects[0].name)
        self.assertEqual('123abc', projects[0].dds_id)

        # Pretend we upload two chunks of the file
        project_db.create_file_chunk(my_file, 1)
        project_db.create_file_chunk(my_file, 2)
        projects = project_db.get_projects()
        self.assertEqual(2, len(projects[0].children[0].chunks))

        # Mark file as done
        my_file.dds_id = '5124cde'
        project_db.commit_session()
        projects = project_db.get_projects()
        self.assertEqual('5124cde', projects[0].children[0].dds_id)

        # Delete the project
        project_db.delete_project(my_project)
        projects = project_db.get_projects()
        self.assertEqual(0, len(projects))

    def test_upload_folder_with_files_project(self):
        """
        Simulate uploading a project containing a folder and two files.
        """
        project_db = ProjectDB(IN_MEMORY_STORAGE)
        my_project = project_db.create_project(project_name='SeqAnalysis')
        my_folder = project_db.add_folder(parent=my_project, local_path='/tmp/seq/data', remote_path='seq/data')
        my_file1 = project_db.add_file(parent=my_folder, local_path='/tmp/seq/data/one.txt', remote_path='seq/data/one.txt')
        my_file2 = project_db.add_file(parent=my_folder, local_path='/tmp/seq/data/two.txt', remote_path='seq/data/two.txt')
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('SeqAnalysis', projects[0].name)
        self.assertEqual(None, projects[0].dds_id)
        self.assertEqual(1, len(projects[0].children))
        self.assertEqual('/tmp/seq/data', projects[0].children[0].local_path)
        self.assertEqual('seq/data', projects[0].children[0].remote_path)
        self.assertEqual(2, len(projects[0].children[0].children))

        my_project.dds_id = 'abc123'
        project_db.commit_session()

        my_folder.dds_id = 'abc124'
        project_db.commit_session()

        projects = project_db.get_projects()
        self.assertEqual('abc123', projects[0].dds_id)
        self.assertEqual('abc124', projects[0].children[0].dds_id)

        my_file1.dds_id = 'abc125'
        my_file2.dds_id = 'abc126'
        project_db.commit_session()
        projects = project_db.get_projects()
        self.assertEqual('abc125', projects[0].children[0].children[0].dds_id)
        self.assertEqual('abc126', projects[0].children[0].children[1].dds_id)


class TestProjectUtil(TestCase):
    def test_create_remote_path(self):
        values = [
            ('/tmp','/tmp/jpb.txt', 'jpb.txt'),
            ('/tmp', '/tmp/data', 'data'),
            ('/tmp', '/tmp/data/', 'data'),

        ]
        for top_local_path, item_local_path, expected_remote_path in values:
            self.assertEqual(expected_remote_path, ProjectUtil.create_remote_path(top_local_path, item_local_path))

    def test_create_project_from_dir(self):
        project_db = ProjectDB(IN_MEMORY_STORAGE)
        project_util = ProjectUtil(project_db)
        project_util.create_project_tree_for_paths('MyTests', ['ddsc/core/tests/'])
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('MyTests', projects[0].name)
        self.assertEqual(1, len(projects[0].children))
        self.assertEqual(os.path.abspath('ddsc/core/tests/'), projects[0].children[0].local_path)
        self.assertEqual('tests', projects[0].children[0].remote_path)
        file_path = os.path.abspath('ddsc/core/tests/test_projectdb.py')
        self.assertIn(file_path, [obj.local_path for obj in projects[0].children[0].children])
        remote_file_path = 'tests/test_projectdb.py'
        self.assertIn(remote_file_path, [obj.remote_path for obj in projects[0].children[0].children])

    def test_create_project_from_file(self):
        project_db = ProjectDB(IN_MEMORY_STORAGE)
        project_util = ProjectUtil(project_db)
        project_util.create_project_tree_for_paths('MyTests2', ['ddsc/core/tests/test_projectdb.py'])
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('MyTests2', projects[0].name)
        self.assertEqual(1, len(projects[0].children))
        self.assertEqual(os.path.abspath('ddsc/core/tests/test_projectdb.py'), projects[0].children[0].local_path)
        self.assertEqual('test_projectdb.py', projects[0].children[0].remote_path)

    def test_create_project_from_multiple_dir(self):
        project_db = ProjectDB(IN_MEMORY_STORAGE)
        project_util = ProjectUtil(project_db)
        project_util.create_project_tree_for_paths('MyTests3', ['ddsc/core'])
        projects = project_db.get_projects()
        self.assertEqual(1, len(projects))
        self.assertEqual('MyTests3', projects[0].name)
        self.assertTrue(len(projects[0].children[0].children) > 1)
        self.assertIn('core/tests', [child.remote_path for child in projects[0].children[0].children])
        test_dir = [child for child in projects[0].children[0].children if child.remote_path == 'core/tests'][0]
        this_file = os.path.abspath('ddsc/core/tests/test_projectdb.py')
        self.assertIn(this_file, [item.local_path for item in test_dir.children])
        self.assertIn('core/tests/test_projectdb.py', [item.remote_path for item in test_dir.children])