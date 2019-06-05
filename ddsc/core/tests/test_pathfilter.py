from unittest import TestCase
from ddsc.core.pathfilter import PathFilter, IncludeFilter, ExcludeFilter, PathFilteredProject
from ddsc.core.remotestore import RemoteProject, RemoteFolder, RemoteFile


class TestPathFilter(TestCase):
    def test_invalid_path_setup(self):
        with self.assertRaises(ValueError):
            PathFilter(include_paths=['data'], exclude_paths=['results'])


class TestPathFilteredProject(TestCase):
    def setUp(self):
        project_fields = {
            "id": "12345",
            "kind": "dds-project",
            "name": "mouse",
            "description": "Mouse RNA Data",
            "is_deleted": False,
        }
        folder1_fields = {
            "id": "12346",
            "kind": "dds-folder",
            "name": "data",
            "is_deleted": False
        }
        folder2_fields = {
            "id": "12347",
            "kind": "dds-folder",
            "name": "results",
            "is_deleted": False
        }
        file1_fields = {
            "id": "12348",
            "kind": "dds-file",
            "name": "rg45.txt",
            "is_deleted": False,
            "current_version": {
                "id": "54321",
                "upload": {
                    "size": 100
                },
            }
        }
        file2_fields = {
            "id": "12349",
            "kind": "dds-file",
            "name": "results.doc",
            "is_deleted": False,
            "current_version": {
                "id": "54322",
                "upload": {
                    "size": 100
                },
            }
        }
        file3_fields = {
            "id": "12310",
            "kind": "dds-file",
            "name": "results.csv",
            "is_deleted": False,
            "current_version": {
                "id": "54323",
                "upload": {
                    "size": 100
                },
            }
        }

        self.project = RemoteProject(project_fields)
        folder1 = RemoteFolder(folder1_fields, "/")
        self.project.add_child(folder1)
        folder2 = RemoteFolder(folder2_fields, "/data")
        folder1.add_child(folder2)
        file1 = RemoteFile(file1_fields, "/data")
        self.project.add_child(file1)
        file2 = RemoteFile(file2_fields, "/data/results")
        folder2.add_child(file2)
        file3 = RemoteFile(file3_fields, "/data/results")
        folder2.add_child(file3)

    def test_no_filter(self):
        path_filter = PathFilter(include_paths=[], exclude_paths=[])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
            '/data',
            '/data/rg45.txt',
            '/data/results',
            '/data/results/results.doc',
            '/data/results/results.csv',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))

    def test_single_file_include_filter(self):
        path_filter = PathFilter(include_paths=['/data/rg45.txt'], exclude_paths=[])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
            '/data',
            '/data/rg45.txt',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))

    def test_single_dir_include_filter(self):
        path_filter = PathFilter(include_paths=['/stuff'], exclude_paths=[])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))
        self.assertEqual(["/stuff"], path_filter.get_unused_paths())

    def test_nested_dir_include_filter(self):
        path_filter = PathFilter(include_paths=['/data/results'], exclude_paths=[])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
            '/data',
            '/data/results',
            '/data/results/results.doc',
            '/data/results/results.csv',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))

    def test_nested_file_include_filter(self):
        path_filter = PathFilter(include_paths=['/data/results/results.csv'], exclude_paths=[])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
            '/data',
            '/data/results',
            '/data/results/results.csv',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))

    def test_nested_dir_exclude_filter(self):
        path_filter = PathFilter(include_paths=[], exclude_paths=['/data/results'])
        collector = ItemPathCollector()
        path_filtered_project = PathFilteredProject(path_filter, collector)
        path_filtered_project.run(self.project)
        expected = [
            '/',
            '/data',
            '/data/rg45.txt',
        ]
        self.assertEqual(set(expected), set(collector.visited_paths))


class ItemPathCollector(object):
    def __init__(self):
        self.visited_paths = []

    def visit_project(self, item):
        self.visited_paths.append(item.remote_path)

    def visit_folder(self, item, parent):
        self.visited_paths.append(item.remote_path)

    def visit_file(self, item, parent):
        self.visited_paths.append(item.remote_path)


class TestIncludeFilter(TestCase):
    def check_filter(self, path_filter, yes_values, no_values):
        for value in yes_values:
            self.assertEqual(True, path_filter.include(value), "should be True {}".format(value))
        for value in no_values:
            self.assertEqual(False, path_filter.include(value), "should be False {}".format(value))

    def test_include_top_level_file(self):
        path_filter = IncludeFilter(["123.txt"])
        yes_values = [
            "123.txt"
        ]
        no_values = [
            "data",
            ".txt",
            "123",
            "data/123.txt",
            "results/123.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_top_level_dir(self):
        path_filter = IncludeFilter(["data"])
        yes_values = [
            "data",
            "data/raw_files",
            "data/123.txt",
        ]
        no_values = [
            "123.txt"
            ".txt",
            "123",
            "results/123.txt"
            "results/data"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_nested_dir(self):
        path_filter = IncludeFilter(["data/raw_files"])
        yes_values = [
            "data",
            "data/raw_files",
            "data/raw_files/123.txt",
            "data/raw_files/data",
            "data/raw_files/one/two",
        ]
        no_values = [
            "data/123.txt",
            "some/raw_files"
            "raw_files"
            "raw_files/data.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_nested_file(self):
        path_filter = IncludeFilter(["data/raw_files/mine.txt"])
        yes_values = [
            "data",
            "data/raw_files",
            "data/raw_files/mine.txt",
        ]
        no_values = [
            "mine.txt",
            "raw_files/mine.txt",
            "dat/raw_files/mine.txt"
            "data/raw_files/other.txt",
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_multiple(self):
        path_filter = IncludeFilter(["data", "stuff/info.txt"])
        yes_values = [
            "data",
            "data/raw_files",
            "data/raw_files/mine.txt",
            "stuff",
            "stuff/info.txt"
        ]
        no_values = [
            "mine.txt",
            "raw_files/mine.txt",
            "dat/raw_files/mine.txt"
            "data/raw_files/other.txt",
            "stuff/other.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)


class TestExcludeFilter(TestCase):
    def check_filter(self, path_filter, yes_values, no_values):
        for value in yes_values:
            self.assertEqual(True, path_filter.include(value), "should be True {}".format(value))
        for value in no_values:
            self.assertEqual(False, path_filter.include(value), "should be False {}".format(value))

    def test_include_top_level_file(self):
        path_filter = ExcludeFilter(["123.txt"])
        yes_values = [
            "data",
            ".txt",
            "123",
            "data/123.txt",
            "results/123.txt"
        ]
        no_values = [
            "123.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_top_level_dir(self):
        path_filter = ExcludeFilter(["data"])
        yes_values = [
            "123.txt",
            ".txt",
            "123",
            "results/123.txt"
        ]
        no_values = [
            "data",
            "data/123.txt",
            "data/something",
            "data/something/123.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_nested_dir(self):
        path_filter = ExcludeFilter(["data/results"])
        yes_values = [
            "data",
            "results"
            "results/123.txt",
        ]
        no_values = [
            "data/results",
            "data/results/123.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)

    def test_include_multiple(self):
        path_filter = ExcludeFilter(["data/results", "test/data.txt"])
        yes_values = [
            "data",
            "results"
            "results/123.txt",
            "test",
            "test/data.doc"
        ]
        no_values = [
            "data/results",
            "data/results/123.txt",
            "test/data.txt"
        ]
        self.check_filter(path_filter, yes_values, no_values)
