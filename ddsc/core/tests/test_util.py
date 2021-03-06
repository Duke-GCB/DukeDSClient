from unittest import TestCase

from ddsc.core.util import verify_terminal_encoding, ProgressBar, ProgressPrinter, KindType, RemotePath, humanize_bytes,\
    plural_fmt, join_with_commas_and_and
from ddsc.exceptions import DDSUserException
from mock import patch, Mock


class TestUtil(TestCase):

    def test_verify_terminal_encoding_upper(self):
        verify_terminal_encoding('UTF')

    def test_verify_terminal_encoding_lower(self):
        verify_terminal_encoding('utf')

    def test_verify_terminal_encoding_ascii_raises(self):
        with self.assertRaises(DDSUserException):
            verify_terminal_encoding('ascii')

    def test_verify_terminal_encoding_empty_is_ok(self):
        verify_terminal_encoding('')

    def test_verify_terminal_encoding_none_is_ok(self):
        verify_terminal_encoding(None)


class TestProgressBar(TestCase):
    @patch('ddsc.core.util.sys.stdout')
    @patch('ddsc.core.util.transfer_speed_str')
    def test_show_no_waiting(self, mock_transfer_speed_str, mock_stdout):
        progress_bar = ProgressBar()
        mock_transfer_speed_str.return_value = ' @ 100 MB/s'

        # replace line with our progress
        progress_bar.update(percent_done=0, details='sending really_long_filename.txt', transferred_bytes=0)
        progress_bar.show()
        expected = '\rProgress: 0% @ 100 MB/s - sending really_long_filename.txt'
        mock_stdout.write.assert_called_with(expected)

        # replace line with our progress (make sure it is long enough to blank out previous line)
        progress_bar.update(percent_done=10, details='sending short.txt', transferred_bytes=100)
        progress_bar.show()
        expected = '\rProgress: 10% @ 100 MB/s - sending short.txt              '
        mock_stdout.write.assert_called_with(expected)

        progress_bar.update(percent_done=15, details='sending short.txt', transferred_bytes=50)
        progress_bar.show()
        expected = '\rProgress: 15% @ 100 MB/s - sending short.txt              '
        mock_stdout.write.assert_called_with(expected)

        # we finish uploading(go to newline)
        progress_bar.set_state(ProgressBar.STATE_DONE)
        progress_bar.show()
        expected = '\rDone: 100% @ 100 MB/s                                     \n'
        mock_stdout.write.assert_called_with(expected)

    @patch('ddsc.core.util.sys.stdout')
    @patch('ddsc.core.util.transfer_speed_str')
    def test_show_with_waiting(self, mock_transfer_speed_str, mock_stdout):
        progress_bar = ProgressBar()
        mock_transfer_speed_str.return_value = ' @ 100 MB/s'

        # we make some progress
        progress_bar.update(percent_done=10, details='sending short.txt', transferred_bytes=100)
        progress_bar.show()
        expected = '\rProgress: 10% @ 100 MB/s - sending short.txt'
        mock_stdout.write.assert_called_with(expected)

        # we get stuck waiting
        progress_bar.wait_msg = "Waiting for project"
        progress_bar.set_state(ProgressBar.STATE_WAITING)
        progress_bar.show()
        expected = '\rProgress: 10% @ 100 MB/s - Waiting for project'
        mock_stdout.write.assert_called_with(expected)

        # waiting takes priority over progress updates
        # (we may be able to upload some folders while waiting to upload files)
        progress_bar.update(percent_done=15, details='sending short.txt', transferred_bytes=50)
        progress_bar.show()
        expected = '\rProgress: 15% @ 100 MB/s - Waiting for project'
        mock_stdout.write.assert_called_with(expected)

        # we are not longer waiting
        progress_bar.set_state(ProgressBar.STATE_RUNNING)
        progress_bar.show()
        expected = '\rProgress: 15% @ 100 MB/s - sending short.txt  '
        mock_stdout.write.assert_called_with(expected)

        # we finish uploading(go to newline)
        progress_bar.set_state(ProgressBar.STATE_DONE)
        progress_bar.show()
        expected = '\rDone: 100% @ 100 MB/s                         \n'
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
    def test_general_functionality(self, mock_progress_bar):
        progress_printer = ProgressPrinter(total=10, msg_verb='sending')

        # pretend we just created a project
        mock_project = Mock(kind=KindType.project_str, path='')
        progress_printer.transferring_item(item=mock_project, increment_amt=1, transferred_bytes=100)
        mock_progress_bar.return_value.update.assert_called_with(10, 100, 'sending project')
        mock_progress_bar.return_value.show.assert_called()
        mock_progress_bar.reset_mock()

        # pretend we just created a folder
        mock_project = Mock(kind=KindType.folder_str, path='/data')
        progress_printer.transferring_item(item=mock_project, increment_amt=2, transferred_bytes=200)
        mock_progress_bar.return_value.update.assert_called_with(30, 300, 'sending data')
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
        mock_progress_bar.return_value.update.assert_called_with(50, 300, 'sending log.txt')
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

    @patch('ddsc.core.util.ProgressBar')
    @patch('ddsc.core.util.os')
    def test_transferring_item_override_msg_verb(self, mock_os, mock_progress_bar):
        progress_bar = mock_progress_bar.return_value
        mock_os.path.basename.return_value = 'somefile.txt'
        progress_printer = ProgressPrinter(total=10, msg_verb='sending')
        progress_printer.transferring_item(Mock(), increment_amt=0)
        progress_bar.update.assert_called_with(0, 0, "sending somefile.txt")
        progress_bar.reset_mock()
        progress_printer.transferring_item(Mock(), increment_amt=0, override_msg_verb='checking')
        progress_bar.update.assert_called_with(0, 0, "checking somefile.txt")

    def test_increment_progress(self):
        progress_printer = ProgressPrinter(total=10, msg_verb='sending')
        self.assertEqual(progress_printer.cnt, 0)
        progress_printer.increment_progress(5)
        self.assertEqual(progress_printer.cnt, 5)
        progress_printer.increment_progress(4)
        self.assertEqual(progress_printer.cnt, 9)
        progress_printer.increment_progress()
        self.assertEqual(progress_printer.cnt, 10)


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
            (999, "999 B"),
            (1000, "1 KB"),
            (1536, "1.5 KB"),
            (1000 ** 2, "1 MB"),
            (1000 ** 3, "1 GB"),
            (1000 ** 4, "1000 GB"),
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


class TestJoinWithCommasAndAnd(TestCase):
    def test_join_with_commas_and_and(self):
        items = []
        self.assertEqual(join_with_commas_and_and(items), '')
        items = ['abc']
        self.assertEqual(join_with_commas_and_and(items), 'abc')
        items = ['abc', 'def']
        self.assertEqual(join_with_commas_and_and(items), 'abc and def')
        items = ['abc', 'def', 'hij']
        self.assertEqual(join_with_commas_and_and(items), 'abc, def and hij')
        items = ['abc', 'def', 'hij', 'klm']
        self.assertEqual(join_with_commas_and_and(items), 'abc, def, hij and klm')
