from unittest import TestCase
from ddsc.core.transferdata import TransferDB, TransferItemType
from ddsc.core.projectuploader import ProjectUploader
from ddsc.core.util import ProjectWalker
from ddsc.core.upload import ProjectUpload
from ddsc.core.localstore import LocalFile, LocalFolder, LocalProject, FileFilter


IN_MEMORY_STORAGE = 'sqlite:///:memory:'
INCLUDE_ALL = ''


class TestTransferDB(TestCase):
    def test_process_one_file_transfer(self):
        transfer_db = TransferDB(IN_MEMORY_STORAGE)

        # Create a transfer for uploading a project with a single top level file
        transfer = transfer_db.create_transfer('123', 'mouseRNA')
        tranfer_item = transfer_db.create_transfer_item(transfer, 1, TransferItemType.FILE)

        # Pretend we uploaded chunk 1
        transfer_db.create_transferred_chunk(tranfer_item, 1)

        # Check listing functionalty
        transfers = transfer_db.list_transfers()
        self.assertEqual(1, len(transfers))
        self.assertEqual(transfer.uuid, transfers[0].uuid)
        self.assertEqual('123', transfer.project_id)
        self.assertEqual('mouseRNA', transfers[0].project_name)
        self.assertEqual(1, len(transfers[0].items))

        # Pretend the transfer finished - delete it
        transfer_db.delete_transfer(transfer)
        transfers = transfer_db.list_transfers()
        self.assertEqual(0, len(transfers))

    def test_create_from_new_local_project(self):
        content = LocalProject(False, file_exclude_regex=INCLUDE_ALL)
        content.add_path('docs')
        ProjectWalker.walk_project()
