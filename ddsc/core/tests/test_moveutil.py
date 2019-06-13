from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.moveutil import MoveUtil, KindType
from mock import Mock


class MoveUtilTestCase(TestCase):
    def test_run__change_parent_and_rename(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data/file2.txt')
        mock_parent = Mock()
        move_util.get_new_parent = Mock()
        move_util.get_new_parent.return_value = mock_parent
        move_util.get_new_name = Mock()
        move_util.get_new_name.return_value = 'file2.txt'
        result = move_util.run()
        mock_source = mock_project.get_child_for_path.return_value
        mock_source.change_parent.assert_called_with(mock_parent)
        mock_source.change_parent.return_value.rename.assert_called_with('file2.txt')
        self.assertEqual(result, mock_source.change_parent.return_value.rename.return_value)

    def test_run__noop(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', 'data/file2.txt')
        move_util.get_new_parent = Mock()
        move_util.get_new_parent.return_value = None
        move_util.get_new_name = Mock()
        move_util.get_new_name.return_value = None
        result = move_util.run()
        mock_source = mock_project.get_child_for_path.return_value
        mock_source.change_parent.assert_not_called()
        mock_source.rename.assert_not_called()
        self.assertEqual(result, mock_source)

    def test_get_new_parent__target_is_folder(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data/somedir')
        move_util.is_folder_or_project = Mock()
        move_util.is_folder_or_project.return_value = True
        mock_folder = Mock()
        mock_project.try_get_item_for_path.return_value = mock_folder
        result = move_util.get_new_parent()
        self.assertEqual(result, mock_folder)

    def test_get_new_parent__target_is_file(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', 'data/file2.txt')
        move_util.is_folder_or_project = Mock()
        move_util.is_folder_or_project.return_value = False
        mock_folder = Mock()
        mock_project.try_get_item_for_path.return_value = mock_folder
        with self.assertRaises(ValueError) as raised_exception:
            move_util.get_new_parent()
        self.assertEqual(str(raised_exception.exception), 'Cannot move to existing file data/file2.txt.')

    def test_get_new_parent__parent_matches(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data/data2.txt')
        mock_project.try_get_item_for_path.return_value = None
        result = move_util.get_new_parent()
        self.assertEqual(result, None)

    def test_get_new_parent__parent_different(self):
        mock_project = Mock()
        mock_parent_folder = Mock(kind=KindType.folder_str)
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data2/file1.txt')
        mock_project.try_get_item_for_path.side_effect = [
            None,
            mock_parent_folder
        ]
        result = move_util.get_new_parent()
        self.assertEqual(result, mock_parent_folder)

    def test_get_new_parent__target_parent_file(self):
        mock_project = Mock()
        mock_parent_folder = Mock(kind=KindType.file_str)
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data2/file1.txt/file1.txt')
        mock_project.try_get_item_for_path.side_effect = [
            None,
            mock_parent_folder
        ]
        with self.assertRaises(ValueError) as raised_exception:
            move_util.get_new_parent()
        self.assertEqual(str(raised_exception.exception), 'Target parent /data2/file1.txt is a file.')

    def test_get_new_parent__target_parent_not_found(self):
        mock_project = Mock()
        move_util = MoveUtil(mock_project, '/data/file1.txt', '/data2/file1.txt')
        mock_project.try_get_item_for_path.side_effect = [
            None,
            None
        ]
        with self.assertRaises(ValueError) as raised_exception:
            move_util.get_new_parent()
        self.assertEqual(str(raised_exception.exception), 'Target parent directory /data2 does not exist.')

    def test_get_new_name__basenames_different(self):
        move_util = MoveUtil(Mock(), '/data/file1.txt', 'data/file2.txt')
        self.assertEqual(move_util.get_new_name(), 'file2.txt')

    def test_get_new_name__basenames_same(self):
        move_util = MoveUtil(Mock(), '/data/file1.txt', 'data/file1.txt')
        self.assertEqual(move_util.get_new_name(), None)

    def test_is_folder_or_project(self):
        item = Mock()
        item.kind = KindType.file_str
        self.assertEqual(MoveUtil.is_folder_or_project(item), False)
        item.kind = KindType.folder_str
        self.assertEqual(MoveUtil.is_folder_or_project(item), True)
        item.kind = KindType.project_str
        self.assertEqual(MoveUtil.is_folder_or_project(item), True)
