from unittest import TestCase

from ddsc.core.util import verify_terminal_encoding, ProgressBar, ProgressPrinter, KindType, RemotePath, humanize_bytes,\
    plural_fmt
from mock import patch, Mock


class TestUtil(TestCase):

    def test_verify_terminal_encoding_upper(self):
        verify_terminal_encoding('UTF')

    def test_verify_terminal_encoding_lower(self):
        verify_terminal_encoding('utf')

    def test_verify_terminal_encoding_ascii_raises(self):
        with self.assertRaises(ValueError):
            verify_terminal_encoding('ascii')

    def test_verify_terminal_encoding_empty_is_ok(self):
        verify_terminal_encoding('')

    def test_verify_terminal_encoding_none_is_ok(self):
        verify_terminal_encoding(None)


class TestProgressBar(TestCase):
    @patch('ddsc.core.util.sys.stdout')
    def test_show_no_waiting(self, mock_stdout):
        progress_bar = ProgressBar()

        # replace line with our progress
        progress_bar.update(percent_done=0, details='sending really_long_filename.txt')
        progress_bar.show()
        expected = '\rProgress: 0% - sending really_long_filename.txt'
        mock_stdout.write.assert_called_with(expected)

        # replace line with our progress (make sure it is long enough to blank out previous line)
        progress_bar.update(percent_done=10, details='sending short.txt')
        progress_bar.show()
        expected = '\rProgress: 10% - sending short.txt              '
        mock_stdout.write.assert_called_with(expected)

        progress_bar.update(percent_done=15, details='sending short.txt')
        progress_bar.show()
        expected = '\rProgress: 15% - sending short.txt              '
        mock_stdout.write.assert_called_with(expected)

        # we finish uploading(go to newline)
        progress_bar.set_state(ProgressBar.STATE_DONE)
        progress_bar.show()
        expected = '\rDone: 100%                                     \n'
        mock_stdout.write.assert_called_with(expected)

    @patch('ddsc.core.util.sys.stdout')
    def test_show_with_waiting(self, mock_stdout):
        progress_bar = ProgressBar()

        # we make some progress
        progress_bar.update(percent_done=10, details='sending short.txt')
        progress_bar.show()
        expected = '\rProgress: 10% - sending short.txt'
        mock_stdout.write.assert_called_with(expected)

        # we get stuck waiting
        progress_bar.wait_msg = "Waiting for project"
        progress_bar.set_state(ProgressBar.STATE_WAITING)
        progress_bar.show()
        expected = '\rProgress: 10% - Waiting for project'
        mock_stdout.write.assert_called_with(expected)

        # waiting takes priority over progress updates
        # (we may be able to upload some folders while waiting to upload files)
        progress_bar.update(percent_done=15, details='sending short.txt')
        progress_bar.show()
        expected = '\rProgress: 15% - Waiting for project'
        mock_stdout.write.assert_called_with(expected)

        # we are not longer waiting
        progress_bar.set_state(ProgressBar.STATE_RUNNING)
        progress_bar.show()
        expected = '\rProgress: 15% - sending short.txt  '
        mock_stdout.write.assert_called_with(expected)

        # we finish uploading(go to newline)
        progress_bar.set_state(ProgressBar.STATE_DONE)
        progress_bar.show()
        expected = '\rDone: 100%                         \n'
        mock_stdout.write.assert_called_with(expected)

    @patch('ddsc.core.util.sys.stdout')
    def test_show_running(self, mock_stdout):
        progress_bar = ProgressBar()
        progress_bar.percent_done = 4
        progress_bar.current_item_details = 'somefile.dat'
        progress_bar.show_running()
        self.assertEqual(progress_bar.state, ProgressBar.STATE_RUNNING)
        expected = '\rProgress: 4% - somefile.dat'
        mock_stdout.write.assert_called_with(expected)

    @patch('ddsc.core.util.sys.stdout')
    def test_show_waiting(self, mock_stdout):
        progress_bar = ProgressBar()
        progress_bar.percent_done = 5
        progress_bar.current_item_details = 'somefile.dat'
        progress_bar.show_waiting('waiting for consistency')
        self.assertEqual(progress_bar.state, ProgressBar.STATE_WAITING)
        expected = '\rProgress: 5% - waiting for consistency'
        mock_stdout.write.assert_called_with(expected)


class TestProgressPrinter(TestCase):
    @patch('ddsc.core.util.ProgressBar')
    def test_stuff(self, mock_progress_bar):
        progress_printer = ProgressPrinter(total=10, msg_verb='sending')

        # pretend we just created a project
        mock_project = Mock(kind=KindType.project_str, path='')
        progress_printer.transferring_item(item=mock_project, increment_amt=1)
        mock_progress_bar.return_value.update.assert_called_with(10, 'sending project')
        mock_progress_bar.return_value.show.assert_called()
        mock_progress_bar.reset_mock()

        # pretend we just created a folder
        mock_project = Mock(kind=KindType.folder_str, path='/data')
        progress_printer.transferring_item(item=mock_project, increment_amt=2)
        mock_progress_bar.return_value.update.assert_called_with(30, 'sending data')
        mock_progress_bar.return_value.show.assert_called()
        mock_progress_bar.reset_mock()

        # pretend we get stuck waiting for project uploads to be ready
        progress_printer.start_waiting()
        mock_progress_bar.return_value.show_waiting.assert_called_with('Waiting for project to become ready for sending')
        mock_progress_bar.reset_mock()

        # pretend project uploads are ready
        progress_printer.done_waiting()
        mock_progress_bar.return_value.show_running.assert_called()
        mock_progress_bar.reset_mock()

        # pretend we uploaded a file
        mock_project = Mock(kind=KindType.file_str, path='/data/log.txt')
        progress_printer.transferring_item(item=mock_project, increment_amt=2)
        mock_progress_bar.return_value.update.assert_called_with(50, 'sending log.txt')
        mock_progress_bar.return_value.show.assert_called()
        mock_progress_bar.reset_mock()

        # pretend we are finished uploading
        progress_printer.finished()
        mock_progress_bar.return_value.set_state.assert_called_with(mock_progress_bar.STATE_DONE)
        mock_progress_bar.return_value.show.assert_called()
        mock_progress_bar.reset_mock()

    @patch('ddsc.core.util.ProgressBar')
    def test_start_waiting_debounces(self, mock_progress_bar):
        progress_printer = ProgressPrinter(total=10, msg_verb='uploading')
        progress_printer.start_waiting()
        self.assertEqual(1, progress_printer.progress_bar.show_waiting.call_count)
        progress_printer.progress_bar.show_waiting.assert_called_with(
            'Waiting for project to become ready for uploading')
        progress_printer.start_waiting()
        progress_printer.start_waiting()
        self.assertEqual(1, progress_printer.progress_bar.show_waiting.call_count)

    @patch('ddsc.core.util.ProgressBar')
    def test_done_waiting(self, mock_progress_bar):
        progress_printer = ProgressPrinter(total=10, msg_verb='downloading')
        progress_printer.start_waiting()
        self.assertEqual(True, progress_printer.waiting)
        self.assertEqual(1, progress_printer.progress_bar.show_waiting.call_count)
        progress_printer.progress_bar.show_waiting.assert_called_with(
            'Waiting for project to become ready for downloading')
        progress_printer.done_waiting()
        self.assertEqual(False, progress_printer.waiting)
        self.assertEqual(1, progress_printer.progress_bar.show_running.call_count)


class TestRemotePath(TestCase):
    def test_add_leading_slash(self):
        self.assertEqual(RemotePath.add_leading_slash('data'), '/data')

    def test_strip_leading_slash(self):
        self.assertEqual(RemotePath.strip_leading_slash('/data'), 'data')
        self.assertEqual(RemotePath.strip_leading_slash('data'), 'data')

    def test_split(self):
        self.assertEqual(RemotePath.split('/data'), ['data'])
        self.assertEqual(RemotePath.split('/data/file1.txt'), ['data', 'file1.txt'])
        self.assertEqual(RemotePath.split('/data/other/file1.txt'), ['data', 'other', 'file1.txt'])


class TestHumanizeBytes(TestCase):
    def test_humanize_bytes(self):
        vals = [
            (1, "1 B"),
            (1023, "1023 B"),
            (1024, "1 KiB"),
            (1536, "1.5 KiB"),
            (1024 ** 2, "1 MiB"),
            (1024 ** 3, "1 GiB"),
            (1024 ** 4, "1024 GiB"),
        ]
        for input_val, expected_result in vals:
            self.assertEqual(humanize_bytes(input_val), expected_result)


class TestPluralFmt(TestCase):
    def test_plural_fmt(self):
        self.assertEqual(plural_fmt("taco", 1), "1 taco")
        self.assertEqual(plural_fmt("taco", 2), "2 tacos")
        self.assertEqual(plural_fmt("folder", 0), "0 folders")
        self.assertEqual(plural_fmt("folder", 1), "1 folder")
        self.assertEqual(plural_fmt("folder", 2), "2 folders")
        self.assertEqual(plural_fmt("folder", 3), "3 folders")
