from unittest import TestCase
from ddsc.core.ignorefile import FileFilter, FilenamePatternList, IgnoreFilePatterns
from mock import patch, mock_open, call, MagicMock
from ddsc.config import FILE_EXCLUDE_REGEX_DEFAULT


class TestFileFilter(TestCase):
    def test_default_file_exclude_regex(self):
        include_file = FileFilter(FILE_EXCLUDE_REGEX_DEFAULT).include
        good_files = [
            'data.txt',
            'long file with many words and stuff 2000.csv',
            '.gitignore',
            '.ddsclient_other',
            '.DS_Storeage',
            'DS_Store'
        ]
        bad_files = [
            '.ddsclient',
            '.DS_Store',
            '._anything',
            '._abc'
        ]
        # include good filenames
        for good_filename in good_files:
            self.assertEqual(include_file(good_filename, is_file=True), True)
        # exclude bad filenames
        for bad_filename in bad_files:
            self.assertEqual(include_file(bad_filename, is_file=True), False)


class FilenamePatternListTests(TestCase):
    def test_add_filename_pattern(self):
        filename_pattern_list = FilenamePatternList()
        self.assertEqual(True, filename_pattern_list.include("/tmp/data/file1.zip"))
        self.assertEqual(True, filename_pattern_list.include("/tmp/data/file2.dat"))

        filename_pattern_list.add_filename_pattern("/tmp/data", "file1.zip")
        self.assertEqual(False, filename_pattern_list.include("/tmp/data/file1.zip"))
        self.assertEqual(True, filename_pattern_list.include("/tmp/data/file2.dat"))

        filename_pattern_list.add_filename_pattern("/tmp/data", "*.dat")
        self.assertEqual(False, filename_pattern_list.include("/tmp/data/file1.zip"))
        self.assertEqual(False, filename_pattern_list.include("/tmp/data/file2.dat"))


class IgnoreFilePatternsTests(TestCase):
    def test_add_patterns(self):
        mock_file_filter = MagicMock()
        mock_file_filter.include.return_value = True
        ignore_file_data = IgnoreFilePatterns(mock_file_filter)
        ignore_file_data.add_patterns('/tmp/data', ['*.log', '*.sv'])
        ignore_file_data.add_patterns('/tmp/data/backup', ['*.zip'])
        include_filenames = [
            '/tmp/data/data.dat',
            '/tmp/data/data.log.dat',
            '/tmp/data/backup/file1.zip.tar',
        ]
        for include_filename in include_filenames:
            self.assertEqual(True, ignore_file_data.include(include_filename, is_file=True))
        exclude_filenames = [
            '/tmp/data/file1.log',
            '/tmp/data/file2.sv',
            '/tmp/data/backup/file1.zip',
            '/tmp/data/backup/data.log',
        ]
        for exclude_filename in exclude_filenames:
            self.assertEqual(False, ignore_file_data.include(exclude_filename, is_file=True))

    @patch('ddsc.core.ignorefile.os')
    def test_load_directory(self, mock_os):
        mock_os.sep = '/'
        mock_os.walk.return_value = [
            ('/tmp/data', ['results', 'script'], ['.ddsignore']),
            ('/tmp/data/results', ['files'], ['result.bam']),
            ('/tmp/data/results/files', [], ['file1.zip', 'file2.log', 'file3.bam']),
            ('/tmp/data/script', [], ['run.sh']),
        ]

        mock_file_filter = MagicMock()
        mock_file_filter.include.return_value = True
        ignore_file_data = IgnoreFilePatterns(mock_file_filter)
        file_data = '*.log\n*.zip'
        fake_open = mock_open(read_data=file_data)
        with patch('ddsc.core.ignorefile.open', fake_open, create=True):
            ignore_file_data.load_directory(top_path='/tmp/data', followlinks=False)
        fake_open.assert_has_calls([
            call('/tmp/data/.ddsignore', 'r'),
        ], any_order=True)

        self.assertEqual(False, ignore_file_data.include('/tmp/data/toplevel.log', is_file=True))
        self.assertEqual(False, ignore_file_data.include('/tmp/data/results/file2.log', is_file=True))
        self.assertEqual(False, ignore_file_data.include('/tmp/data/results/file1.zip', is_file=True))

        self.assertEqual(True, ignore_file_data.include('/tmp/data/toplevel.bam', is_file=True))
        self.assertEqual(True, ignore_file_data.include('/tmp/data/script/run.sh', is_file=True))
        self.assertEqual(True, ignore_file_data.include('/tmp/data/results/file1.txt', is_file=True))
        self.assertEqual(True, ignore_file_data.include('/tmp/data/results/foies/result.bam', is_file=True))
