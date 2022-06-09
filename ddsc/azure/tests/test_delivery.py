from unittest import TestCase
from mock import patch, Mock, call
from ddsc.azure.delivery import DataDelivery, DDSUserException, DeliveryApi, UNAUTHORIZED_MESSAGE


class TestDataDelivery(TestCase):
    def setUp(self):
        self.config = Mock()
        self.api = Mock()
        self.api.current_user_netid = 'user1'
        self.api.users.ensure_user_exists.return_value = True
        self.api.get_container_url.return_value = 'someurl'

    @patch('ddsc.azure.delivery.DeliveryApi')
    @patch('ddsc.azure.delivery.print')
    def test_deliver(self, mock_print, mock_delivery_api):
        mock_delivery_api.return_value.find_incomplete_delivery.return_value = None
        mock_delivery_api.return_value.create_delivery.return_value = "123"
        data_delivery = DataDelivery(self.config, self.api)
        data_delivery.deliver(project_path="user1/mouse",
                              to_netid="user2",
                              user_message="Test",
                              share_user_ids=["user3", "user4"],
                              resend=False)
        mock_print.assert_called_with("\nDelivery email message sent to user2\n")
        mock_delivery_api.return_value.create_delivery.assert_called_with({
            'source_project': {
                'path': 'user1/mouse',
                'container_url': 'someurl'
            },
            'from_netid': 'user1',
            'to_netid': 'user2',
            'user_message': 'Test',
            'share_user_ids': ['user3', 'user4']
        })
        mock_delivery_api.return_value.send_delivery.assert_called_with("123")
        self.api.users.ensure_user_exists.assert_has_calls([
            call("user2"),
            call("user3"),
            call("user4"),
        ])

    @patch('ddsc.azure.delivery.DeliveryApi')
    @patch('ddsc.azure.delivery.print')
    def test_deliver_to_self(self, mock_print, mock_delivery_api):
        mock_delivery_api.return_value.find_incomplete_delivery.return_value = None
        mock_delivery_api.return_value.create_delivery.return_value = "123"
        data_delivery = DataDelivery(self.config, self.api)
        with self.assertRaises(DDSUserException):
            data_delivery.deliver(project_path="user1/mouse",
                                  to_netid="user1",
                                  user_message="Test",
                                  share_user_ids=["user3", "user4"],
                                  resend=False)

    @patch('ddsc.azure.delivery.DeliveryApi')
    @patch('ddsc.azure.delivery.print')
    def test_deliver_already_exists(self, mock_print, mock_delivery_api):
        mock_delivery_api.return_value.find_incomplete_delivery.return_value = '123'
        mock_delivery_api.return_value.create_delivery.return_value = "123"
        data_delivery = DataDelivery(self.config, self.api)
        with self.assertRaises(DDSUserException):
            data_delivery.deliver(project_path="user1/mouse",
                                  to_netid="user2",
                                  user_message="Test",
                                  share_user_ids=["user3", "user4"],
                                  resend=False)

    @patch('ddsc.azure.delivery.DeliveryApi')
    @patch('ddsc.azure.delivery.print')
    def test_deliver_resend(self, mock_print, mock_delivery_api):
        mock_delivery_api.return_value.find_incomplete_delivery.return_value = '456'
        mock_delivery_api.return_value.create_delivery.return_value = "123"
        data_delivery = DataDelivery(self.config, self.api)
        data_delivery.deliver(project_path="user1/mouse",
                              to_netid="user2",
                              user_message="Test",
                              share_user_ids=["user3", "user4"],
                              resend=True)
        mock_print.assert_called_with("\nDelivery email message sent to user2\n")
        mock_delivery_api.return_value.send_delivery.assert_called_with("456")


class TestDeliveryApi(TestCase):
    def setUp(self):
        self.payload = {
            'source_project': {
                'path': 'user1/mouse',
                'container_url': 'someurl'
            },
            'from_netid': 'user1',
            'to_netid': 'user2',
            'user_message': 'Test',
            'share_user_ids': ['user3', 'user4']
        }

    @patch('ddsc.azure.delivery.requests')
    def test_create_delivery(self, mock_requests):
        mock_result = Mock(status_code=201)
        mock_result.json.return_value = {"id": "123"}
        mock_requests.post.return_value = mock_result
        api = DeliveryApi(config=Mock(azure_delivery_url='someurl', delivery_token='mytoken'))
        result = api.create_delivery(payload={"data": "here1"})
        self.assertEqual(result, "123")
        mock_requests.post.assert_called_with('someurl/az-deliveries/', data='{"data": "here1"}',
                                              headers={'Content-Type': 'application/json',
                                                       'Authorization': 'Token mytoken'})

    @patch('ddsc.azure.delivery.requests')
    def test_find_incomplete_delivery(self, mock_requests):
        mock_response = Mock(status_code=200)
        mock_response.json.return_value = [
            {
                "complete": True,
                "id": "777"
            },
            {
                "complete": False,
                "id": "888"
            },
        ]
        mock_requests.get.return_value = mock_response
        api = DeliveryApi(config=Mock(azure_delivery_url='someurl', delivery_token='mytoken'))
        result = api.find_incomplete_delivery(payload=self.payload)
        self.assertEqual(result, "888")

    @patch('ddsc.azure.delivery.requests')
    def test_find_incomplete_delivery_too_many(self, mock_requests):
        mock_response = Mock(status_code=200)
        mock_response.json.return_value = [
            {
                "complete": False,
                "id": "777"
            },
            {
                "complete": False,
                "id": "888"
            },
        ]
        mock_requests.get.return_value = mock_response
        api = DeliveryApi(config=Mock(azure_delivery_url='someurl', delivery_token='mytoken'))
        with self.assertRaises(DDSUserException):
            api.find_incomplete_delivery(payload=self.payload)

    @patch('ddsc.azure.delivery.requests')
    def test_find_incomplete_delivery_not_found(self, mock_requests):
        mock_response = Mock(status_code=200)
        mock_response.json.return_value = [
            {
                "complete": True,
                "id": "777"
            },
            {
                "complete": True,
                "id": "888"
            },
        ]
        mock_requests.get.return_value = mock_response
        api = DeliveryApi(config=Mock(azure_delivery_url='someurl', delivery_token='mytoken'))
        with self.assertRaises(DDSUserException):
            api.find_incomplete_delivery(payload=self.payload)
        result = api.find_incomplete_delivery(payload=self.payload, raise_on_not_found=False)
        self.assertEqual(result, None)

    @patch('ddsc.azure.delivery.requests')
    def test_send_delivery(self, mock_requests):
        mock_response = Mock(status_code=200)
        mock_requests.post.return_value = mock_response
        api = DeliveryApi(config=Mock(azure_delivery_url='someurl', delivery_token='mytoken'))
        api.send_delivery(delivery_id='123')
        mock_requests.post.assert_called_with('someurl/az-deliveries/123/send/',
                                              headers={'Content-Type': 'application/json',
                                                       'Authorization': 'Token mytoken'})

    def test_check_response(self):
        DeliveryApi._check_response(Mock(status_code=200))
        with self.assertRaises(DDSUserException) as raised_exception:
            DeliveryApi._check_response(Mock(status_code=401))
        self.assertEqual(str(raised_exception.exception), UNAUTHORIZED_MESSAGE)
        with self.assertRaises(DDSUserException) as raised_exception:
            DeliveryApi._check_response(Mock(status_code=500, url='someurl', text='Some error'))
        self.assertEqual(str(raised_exception.exception), "Request to someurl failed with 500:\nSome error")
