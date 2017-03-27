from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.d4s2 import D4S2Project
from mock import patch, MagicMock, Mock


class TestD4S2Project(TestCase):
    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_share(self, mock_requests, mock_d4s2api):
        mock_d4s2api().get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        project.share(project_name='mouserna',
                      to_user=MagicMock(id='123'),
                      force_send=False,
                      auth_role='project_viewer',
                      user_message='This is a test.')
        args, kwargs = mock_d4s2api().create_item.call_args
        item = args[0]
        self.assertEqual(mock_d4s2api.SHARE_DESTINATION, item.destination)
        self.assertEqual('123', item.to_user_id)
        self.assertEqual('project_viewer', item.auth_role)
        self.assertEqual('This is a test.', item.user_message)
        mock_d4s2api().send_item.assert_called()

    @patch('ddsc.core.d4s2.D4S2Api')
    @patch('ddsc.core.d4s2.requests')
    def test_deliver(self, mock_requests, mock_d4s2api):
        mock_d4s2api().get_existing_item.return_value = Mock(json=Mock(return_value=[]))
        project = D4S2Project(config=MagicMock(), remote_store=MagicMock(), print_func=MagicMock())
        project.deliver(project_name='mouserna',
                        new_project_name=None,
                        to_user=MagicMock(id='456'),
                        force_send=False,
                        path_filter='',
                        user_message='Yet Another Message.')
        args, kwargs = mock_d4s2api().create_item.call_args
        item = args[0]
        self.assertEqual(mock_d4s2api.DELIVER_DESTINATION, item.destination)
        self.assertEqual('456', item.to_user_id)
        self.assertEqual('Yet Another Message.', item.user_message)
        mock_d4s2api().send_item.assert_called()
