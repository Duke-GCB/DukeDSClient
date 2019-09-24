from unittest import TestCase
import subprocess
import os
import tempfile
from ddsc.core.util import mode_allows_group_or_other, verify_file_private, ProjectDetailsList, KindType, \
    parse_bytes_str, parse_checksum_size_limit, ChecksumSizeLimit
from mock import patch, Mock


def make_temp_filename():
    tempfilename = tempfile.mktemp()
    with open(tempfilename, 'w'):
        pass
    return tempfilename


def set_file_perm(filename, mode):
    subprocess.call(['chmod', mode, filename])


class TestUtil(TestCase):
    def test_mode_allows_group_or_other_true(self):
        tempfilename = make_temp_filename()
        modes = [
            '0777',
            '0666',
            '0555',
            '0770',
            '0707',
            '0660',
            '0606',
            '0010',
            '0001',
        ]
        for mode in modes:
            set_file_perm(tempfilename, mode)
            st_mode = os.stat(tempfilename).st_mode
            self.assertEqual(True, mode_allows_group_or_other(st_mode))
        os.unlink(tempfilename)

    def test_mode_allows_group_or_other_false(self):
        tempfilename = make_temp_filename()
        modes = [
            '0700',
            '0600',
            '0500',
        ]
        for mode in modes:
            set_file_perm(tempfilename, mode)
            st_mode = os.stat(tempfilename).st_mode
            self.assertEqual(False, mode_allows_group_or_other(st_mode))
        os.unlink(tempfilename)

    @patch("ddsc.core.util.platform")
    def test_verify_file_private_on_windows_no_raise(self, mock_platform):
        mock_platform.system.return_value = 'Windows'
        tempfilename = make_temp_filename()
        set_file_perm(tempfilename, '0777')
        verify_file_private(tempfilename)

    @patch("ddsc.core.util.platform")
    def test_verify_file_private_not_windows_raises_when_bad(self, mock_platform):
        mock_platform.system.return_value = 'Linux'
        tempfilename = make_temp_filename()
        set_file_perm(tempfilename, '0777')
        with self.assertRaises(ValueError):
            verify_file_private(tempfilename)

    @patch("ddsc.core.util.platform")
    def test_verify_file_private_not_windows_ok(self, mock_platform):
        mock_platform.system.return_value = 'Linux'
        tempfilename = make_temp_filename()
        set_file_perm(tempfilename, '0700')
        verify_file_private(tempfilename)
        set_file_perm(tempfilename, '0600')
        verify_file_private(tempfilename)
        set_file_perm(tempfilename, '0500')
        verify_file_private(tempfilename)

    @patch("ddsc.core.util.platform")
    def test_non_existant_file_no_raises(self, mock_platform):
        mock_platform.system.return_value = 'Linux'
        tempfilename = './file.never.gonna.exist'
        verify_file_private(tempfilename)

    def test_parse_bytes_str(self):
        value_and_expected = {
            (1, 1),
            (2, 2),
            ("1", 1),
            ("2", 2),
            ("1MB", 1024 * 1024),
            ("1 MB", 1024 * 1024),
            ("3MB", 3 * 1024 * 1024),
            ("100MB", 100 * 1024 * 1024),
        }
        for value, exp in value_and_expected:
            self.assertEqual(exp, parse_bytes_str(value))


class TestProjectDetailsList(TestCase):
    def setUp(self):
        self.mock_project_item = Mock()
        self.mock_project_item.id = '123'
        self.mock_project_item.name = 'mouse'
        self.mock_project_item.kind = KindType.project_str
        self.mock_folder_item = Mock()
        self.mock_folder_item.id = '456'
        self.mock_folder_item.name = 'data'
        self.mock_file_item = Mock()
        self.mock_file_item.id = '789'
        self.mock_file_item.name = 'results.csv'
        self.mock_file_item.hash_alg = 'md5'
        self.mock_file_item.file_hash = 'abcdefg'

    def test_visit_methods_short_format(self):
        project_details_list = ProjectDetailsList(long_format=False)
        project_details_list.visit_project(self.mock_project_item)
        project_details_list.visit_folder(self.mock_folder_item, self.mock_project_item)
        project_details_list.visit_file(self.mock_file_item, self.mock_folder_item)
        expected_details = [
            'Project mouse Contents:',
            '/data',
            '/data/results.csv',
        ]
        self.assertEqual(expected_details, project_details_list.details)

    def test_visit_methods_long_format(self):
        project_details_list = ProjectDetailsList(long_format=True)
        project_details_list.visit_project(self.mock_project_item)
        project_details_list.visit_folder(self.mock_folder_item, self.mock_project_item)
        project_details_list.visit_file(self.mock_file_item, self.mock_folder_item)
        expected_details = [
            '123 - Project mouse Contents:',
            '456\t/data',
            '789\t/data/results.csv\t(md5:abcdefg)',
        ]
        self.assertEqual(expected_details, project_details_list.details)

    def test_get_name_with_project_parent(self):
        item = Mock()
        item.name = 'file1.txt'
        parent = Mock()
        parent.kind = KindType.project_str
        parent.id = 'abc123'
        project_details_list = ProjectDetailsList(long_format=True)
        name = project_details_list.get_name(item, parent)
        self.assertEqual(name, '/file1.txt')

    def test_get_name_no_parent(self):
        item = Mock()
        item.name = 'file1.txt'
        parent = Mock()
        parent.kind = KindType.project_str
        parent.id = 'abc123'
        project_details_list = ProjectDetailsList(long_format=True)
        name = project_details_list.get_name(item, parent)
        self.assertEqual(name, '/file1.txt')


class ChecksumSizeLimitTestCase(TestCase):
    def test_parse_checksum_size_limit(self):
        csl = parse_checksum_size_limit('100')
        self.assertEqual(csl.always_hash, False)
        self.assertEqual(csl.file_size_limit, 100)
        csl = parse_checksum_size_limit('None')
        self.assertEqual(csl.always_hash, True)
        self.assertEqual(csl.file_size_limit, 0)

    def test_should_check_hash(self):
        csl = ChecksumSizeLimit(100)
        self.assertEqual(True, csl.should_check_hash(99))
        self.assertEqual(True, csl.should_check_hash(100))
        self.assertEqual(False, csl.should_check_hash(101))
        csl = ChecksumSizeLimit(0, always_hash=True)
        self.assertEqual(True, csl.should_check_hash(99))
        self.assertEqual(True, csl.should_check_hash(100))
        self.assertEqual(True, csl.should_check_hash(101))
