from unittest import TestCase
import pickle
import multiprocessing
from ddsc.core.projectuploader import UploadSettings, UploadContext, ProjectUploadDryRun, CreateProjectCommand, \
    upload_project_run, create_small_file, ProjectUploader, HashFileCommand, CreateSmallFileCommand, upload_folder_run
from ddsc.core.util import KindType
from ddsc.core.remotestore import ProjectNameOrId
from mock import MagicMock, Mock, patch, call, ANY


class FakeDataServiceApi(object):
    def __init__(self):
        self.auth = FakeDataServiceAuth()


class FakeDataServiceAuth(object):
    def get_auth_data(self):
        return ()


class TestUploadContext(TestCase):
    def test_can_pickle(self):
        """Make sure we can pickle context since it must be passed to another process."""
        settings = UploadSettings(None, FakeDataServiceApi(), None, ProjectNameOrId.create_from_name('mouse'), None)
        params = ('one', 'two', 'three')
        context = UploadContext(settings, params, multiprocessing.Manager().Queue(), 12)
        pickle.dumps(context)

    def test_start_waiting(self):
        mock_message_queue = MagicMock()
        context = UploadContext(settings=MagicMock(),
                                params=[],
                                message_queue=mock_message_queue,
                                task_id=12)
        context.start_waiting()
        mock_message_queue.put.assert_called_with((12, True))

    def test_done_waiting(self):
        mock_message_queue = MagicMock()
        context = UploadContext(settings=MagicMock(),
                                params=[],
                                message_queue=mock_message_queue,
                                task_id=13)
        context.done_waiting()
        mock_message_queue.put.assert_called_with((13, False))


class TestProjectUploadDryRun(TestCase):
    def test_single_empty_non_existant_directory(self):
        local_file = MagicMock(kind=KindType.folder_str, children=[], path='joe', remote_id=None)
        local_project = MagicMock(kind=KindType.project_str, children=[local_file])
        upload_dry_run = ProjectUploadDryRun(local_project)
        self.assertEqual(['joe'], upload_dry_run.upload_items)

    def test_single_empty_existing_directory(self):
        local_file = MagicMock(kind=KindType.folder_str, children=[], path='joe', remote_id='abc')
        local_project = MagicMock(kind=KindType.project_str, children=[local_file])
        upload_dry_run = ProjectUploadDryRun(local_project)
        self.assertEqual([], upload_dry_run.upload_items)

    def test_some_files(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='joe.txt')
        local_file1.hash_matches_remote.return_value = False
        local_file2 = MagicMock(kind=KindType.file_str, path='data.txt')
        local_file2.hash_matches_remote.return_value = True
        local_file3 = MagicMock(kind=KindType.file_str, path='results.txt')
        local_file3.hash_matches_remote.return_value = False
        local_project = MagicMock(kind=KindType.project_str, children=[local_file1, local_file2, local_file3])
        upload_dry_run = ProjectUploadDryRun(local_project)
        self.assertEqual(['joe.txt', 'results.txt'], upload_dry_run.upload_items)
        local_file1.hash_matches_remote.assert_called_with(
            local_file1.calculate_local_hash.return_value
        )
        local_file2.hash_matches_remote.assert_called_with(
            local_file2.calculate_local_hash.return_value
        )
        local_file3.hash_matches_remote.assert_called_with(
            local_file3.calculate_local_hash.return_value
        )

    def test_nested_directories(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/joe.txt')
        local_file1.hash_matches_remote.return_value = False
        local_file2 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/data.txt')
        local_file2.hash_matches_remote.return_value = True
        local_file3 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/results.txt')
        local_file3.hash_matches_remote.return_value = False
        grandchild_folder = MagicMock(kind=KindType.folder_str,
                                      path="/data/2017/08/flyresults",
                                      children=[local_file1, local_file2, local_file3],
                                      remote_id=None)
        child_folder = MagicMock(kind=KindType.folder_str,
                                 path="/data/2017/08",
                                 children=[grandchild_folder],
                                 remote_id=None)
        parent_folder = MagicMock(kind=KindType.folder_str,
                                  path="/data/2017",
                                  children=[child_folder],
                                  remote_id=None)
        local_project = MagicMock(kind=KindType.project_str, children=[parent_folder])
        upload_dry_run = ProjectUploadDryRun(local_project)
        expected_results = [
            '/data/2017',
            '/data/2017/08',
            '/data/2017/08/flyresults',
            '/data/2017/08/flyresults/joe.txt',
            '/data/2017/08/flyresults/results.txt',
        ]
        self.assertEqual(expected_results, upload_dry_run.upload_items)

    def test_nested_directories_skip_parents(self):
        local_file1 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/joe.txt')
        local_file1.hash_matches_remote.return_value = False
        local_file2 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/data.txt')
        local_file2.hash_matches_remote.return_value = True
        local_file3 = MagicMock(kind=KindType.file_str, path='/data/2017/08/flyresults/results.txt')
        local_file3.hash_matches_remote.return_value = False
        grandchild_folder = MagicMock(kind=KindType.folder_str,
                                      path="/data/2017/08/flyresults",
                                      children=[local_file1, local_file2, local_file3],
                                      remote_id=None)
        child_folder = MagicMock(kind=KindType.folder_str,
                                 path="/data/2017/08",
                                 children=[grandchild_folder],
                                 remote_id='355')
        parent_folder = MagicMock(kind=KindType.folder_str,
                                  path="/data/2017",
                                  children=[child_folder],
                                  remote_id='123')
        local_project = MagicMock(kind=KindType.project_str, children=[parent_folder])
        upload_dry_run = ProjectUploadDryRun(local_project)
        expected_results = [
            '/data/2017/08/flyresults',
            '/data/2017/08/flyresults/joe.txt',
            '/data/2017/08/flyresults/results.txt',
        ]
        self.assertEqual(expected_results, upload_dry_run.upload_items)

    def test_get_report_no_changes(self):
        local_project = MagicMock(kind=KindType.project_str, children=[])
        upload_dry_run = ProjectUploadDryRun(local_project)
        upload_dry_run.upload_items = []
        self.assertEqual(upload_dry_run.get_report().strip(), 'No changes found. Nothing needs to be uploaded.')

    def test_get_report_changes_exist(self):
        local_project = MagicMock(kind=KindType.project_str, children=[])
        upload_dry_run = ProjectUploadDryRun(local_project)
        upload_dry_run.upload_items = ['somefile']
        self.assertEqual(upload_dry_run.get_report().strip(), 'Files/Folders that need to be uploaded:\nsomefile')


class TestCreateProjectCommand(TestCase):
    def test_constructor_fails_for_id(self):
        mock_settings = Mock(project_name_or_id=ProjectNameOrId.create_from_project_id('123'))
        mock_local_project = Mock()
        with self.assertRaises(ValueError):
            CreateProjectCommand(mock_settings, mock_local_project)

    def test_constructor_ok_for_name(self):
        mock_settings = Mock(project_name_or_id=ProjectNameOrId.create_from_name('mouse'))
        mock_local_project = Mock()
        CreateProjectCommand(mock_settings, mock_local_project)

    def test_upload_project_run(self):
        mock_data_service = Mock()
        mock_data_service.create_project.return_value = MagicMock()
        mock_upload_context = Mock()
        mock_upload_context.make_data_service.return_value = mock_data_service
        mock_upload_context.project_name_or_id = ProjectNameOrId.create_from_name('mouse')
        upload_project_run(mock_upload_context)
        mock_data_service.create_project.assert_called_with('mouse', 'mouse')
        mock_data_service.close.assert_called_with()


class TestCreateFolderCommand(TestCase):
    def test_upload_folder_run(self):
        context = Mock(params=["data", "dds-project", "1234"])
        mock_data_service = context.make_data_service.return_value
        mock_data_service.create_folder.return_value.json.return_value = {"id": "5678"}
        upload_id = upload_folder_run(context)

        self.assertEqual(upload_id, "5678")
        mock_data_service.create_folder.assert_called_with("data", "dds-project", "1234")
        mock_data_service.close.assert_called_with()


class TestCreateSmallFile(TestCase):
    @patch('ddsc.core.projectuploader.FileUploadOperations', autospec=True)
    def test_create_small_file_passes_zero_index(self, mock_file_operations):
        mock_path_data = Mock()
        mock_hash_data = Mock()
        mock_hash_data.matches.return_value = False
        mock_path_data.read_whole_file.return_value = 'data'
        mock_file_operations.return_value.create_upload_and_chunk_url.return_value = (
            'someId', {'host': 'somehost', 'url': 'someurl'}
        )

        upload_context = Mock(params=(Mock(), mock_path_data, mock_hash_data, Mock(), 'md5', 'abc'))
        resp = create_small_file(upload_context)

        self.assertEqual(resp, mock_file_operations.return_value.finish_upload.return_value)
        mock_file_operations.return_value.create_file_chunk_url.assert_not_called()
        upload_context.make_data_service.return_value.close.assert_called_with()

    @patch('ddsc.core.projectuploader.FileUploadOperations', autospec=True)
    def test_create_small_file_hash_matches(self, mock_file_operations):
        mock_path_data = Mock()
        mock_hash_data = Mock()
        mock_hash_data.matches.return_value = True
        mock_path_data.read_whole_file.return_value = 'data'
        mock_file_operations.return_value.create_upload_and_chunk_url.return_value = (
            'someId', {'host': 'somehost', 'url': 'someurl'}
        )

        upload_context = Mock(params=(Mock(), mock_path_data, mock_hash_data, Mock(), 'md5', 'abc'))
        resp = create_small_file(upload_context)

        self.assertEqual(resp, None)
        mock_file_operations.return_value.create_file_chunk_url.assert_not_called()


class TestProjectUploader(TestCase):
    @patch('ddsc.core.projectuploader.ProjectWalker')
    @patch('ddsc.core.projectuploader.TaskRunner')
    @patch('ddsc.core.projectuploader.SmallItemUploadTaskBuilder')
    def test_run_sorts_new_files_first(self, mock_small_task_builder, mock_task_runner, mock_project_walker):
        settings = Mock()
        settings.config.upload_bytes_per_chunk = 100
        num_upload_workers = 6
        settings.config.upload_workers = num_upload_workers
        uploader = ProjectUploader(settings)
        uploader.process_large_file = Mock()
        small_file_existing = Mock(remote_id='abc123', size=1000)
        small_file_new = Mock(remote_id='', size=2000)
        uploader.small_files = [
            (small_file_existing, None),
            (small_file_new, None),
        ]
        large_file_existing = Mock(remote_id='def456', size=1000)
        large_file_existing.hash_matches_remote.return_value = False
        large_file_new = Mock(remote_id='', size=2000)
        large_file_new.hash_matches_remote.return_value = False
        uploader.large_files = [
            (large_file_existing, None),
            (large_file_new, None),
        ]
        local_project = Mock()

        uploader.run(local_project)

        mock_task_runner.assert_called_with(num_upload_workers)
        # small files should be sorted with new first
        uploader.small_item_task_builder.visit_file.assert_has_calls([
            call(small_file_new, None),
            call(small_file_existing, None),
        ])
        # large files should be sorted with new first
        uploader.process_large_file.assert_has_calls([
            call(large_file_new, None, ANY),
            call(large_file_existing, None, ANY),
        ])

    @patch('ddsc.core.projectuploader.ProjectWalker')
    @patch('ddsc.core.projectuploader.TaskRunner')
    @patch('ddsc.core.projectuploader.SmallItemUploadTaskBuilder')
    def test_run_with_large_files_hash_matching(self, mock_small_task_builder, mock_task_runner, mock_project_walker):
        settings = Mock()
        settings.config.upload_bytes_per_chunk = 100
        uploader = ProjectUploader(settings)
        uploader.process_large_file = Mock()
        large_file_existing = Mock(remote_id='def456', size=1000)
        large_file_existing.hash_matches_remote.return_value = True
        large_file_new = Mock(remote_id='', size=2000)
        large_file_new.hash_matches_remote.return_value = True
        uploader.large_files = [
            (large_file_existing, None),
            (large_file_new, None),
        ]
        local_project = Mock()
        uploader.run(local_project)
        uploader.process_large_file.assert_not_called()
        settings.watcher.transferring_item.assert_has_calls([
            call(large_file_new, increment_amt=0, override_msg_verb='checking'),  # show checking
            call(large_file_existing, increment_amt=0, override_msg_verb='checking'),   # show checking
        ])
        # The progress bar should be incremented for both files that are already at the remote store
        settings.watcher.increment_progress.assert_has_calls([
            call(20), call(10)
        ])

    @patch('ddsc.core.projectuploader.TaskRunner')
    @patch('ddsc.core.projectuploader.SmallItemUploadTaskBuilder')
    @patch('ddsc.core.projectuploader.FileUploader')
    def test_process_large_file_sets_file_hash_and_alg(self, mock_file_uploader, mock_small_task_builder,
                                                       mock_task_runner):
        uploader = ProjectUploader(Mock())
        local_file = Mock()
        parent = Mock()
        hash_data = Mock(
            alg='md5',
            value='defg'
        )
        mock_file_uploader.return_value.upload.return_value = 'abc'
        uploader.process_large_file(local_file, parent, hash_data)
        local_file.set_remote_values_after_send.assert_called_with(
            'abc', 'md5', 'defg'
        )

    @patch('ddsc.core.projectuploader.TaskRunner')
    @patch('ddsc.core.projectuploader.SmallItemUploadTaskBuilder')
    @patch('ddsc.core.projectuploader.FileUploader')
    def test_upload_large_files__updates_watcher(self, mock_file_uploader, mock_small_task_builder, mock_task_runner):
        local_file1 = Mock(size=1000)
        local_file1.hash_matches_remote.return_value = False
        local_file2 = Mock(size=2000)
        local_file2.hash_matches_remote.return_value = True
        settings = Mock()
        settings.config.upload_bytes_per_chunk = 1000
        uploader = ProjectUploader(settings)
        uploader.large_files = [
            (local_file1, Mock()),
            (local_file2, Mock()),
        ]

        uploader.upload_large_files()

        settings.watcher.transferring_item.assert_has_calls([
            # Show checking for file1
            call(local_file1, increment_amt=0, override_msg_verb='checking'),
            # Reset verb to sending for file1 (additional calls are made from ParallelChunkProcessor)
            call(local_file1, increment_amt=0),
            # Show checking for file2
            call(local_file2, increment_amt=0, override_msg_verb='checking'),
        ])
        # The progress bar should be incremented for local_file2 since local matches remote
        settings.watcher.increment_progress.assert_has_calls([
            call(2)
        ])


class TestHashFileCommand(TestCase):
    def test_before_run_shows_checking_message(self):
        cmd = HashFileCommand(settings=Mock(), local_file=Mock())
        cmd.before_run(None)
        cmd.settings.watcher.transferring_item.assert_called_with(cmd.local_file, increment_amt=0,
                                                                  override_msg_verb='checking')

    def test_function_hashes_file(self):
        cmd = HashFileCommand(settings=Mock(), local_file=Mock())
        context = cmd.create_context(Mock(), '123')
        result = cmd.func(context)
        mock_path_data = cmd.local_file.get_path_data.return_value
        self.assertEqual(result, mock_path_data.get_hash.return_value)


class TestCreateSmallFileCommand(TestCase):
    def test_before_run_updates_watcher(self):
        cmd = CreateSmallFileCommand(settings=Mock(), local_file=Mock(), parent=Mock(),
                                     file_upload_post_processor=Mock())
        cmd.before_run('hash_data')
        self.assertEqual(cmd.hash_data, 'hash_data')

    def test_after_run_when_file_is_already_good(self):
        cmd = CreateSmallFileCommand(settings=Mock(), local_file=Mock(), parent=Mock(),
                                     file_upload_post_processor=Mock())
        cmd.after_run(None)
        cmd.file_upload_post_processor.run.assert_not_called()
        cmd.local_file.set_remote_id_after_send.assert_not_called()
        cmd.settings.watcher.increment_progress()

    def test_after_run_when_file_is_sent(self):
        cmd = CreateSmallFileCommand(settings=Mock(), local_file=Mock(), parent=Mock(),
                                     file_upload_post_processor=Mock())
        remote_file_data = {
            'id': 'abc123',
            'current_version': {
                'upload': {
                    'hashes': [
                        {
                            "algorithm": "md5",
                            "value": "abcdefg",
                        }
                    ]
                }
            }
        }
        cmd.after_run(remote_file_data)
        cmd.file_upload_post_processor.run.assert_called_with(cmd.settings.data_service, remote_file_data)
        cmd.local_file.set_remote_values_after_send.assert_called_with('abc123', 'md5', 'abcdefg')
        cmd.settings.watcher.transferring_item.assert_called_with(cmd.local_file, transferred_bytes=cmd.local_file.size)
