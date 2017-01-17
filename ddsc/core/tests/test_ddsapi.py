from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.ddsapi import MultiJSONResponse, DataServiceApi, ContentType
from mock import MagicMock, call


def fake_response(status_code, json_return_value, num_pages=1):
    mock_response = MagicMock(status_code=status_code, headers={'x-total-pages': "{}".format(num_pages)})
    mock_response.json.return_value = json_return_value
    return mock_response


class TestMultiJSONResponse(TestCase):
    """
    Tests that we can merge multiple JSON responses arrays with a given name(merge_array_field_name).
    """
    def test_pass_through_works_with_one_response(self):
        mock_response = fake_response(status_code=200, json_return_value={"results": [1, 2, 3]})
        multi_response = MultiJSONResponse(mock_response, "results")
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3], multi_response.json()["results"])

    def test_pass_through_works_with_two_responses(self):
        mock_response = fake_response(status_code=200, json_return_value={"results": [1, 2, 3]})
        mock_response2 = fake_response(status_code=200, json_return_value={"results": [4, 5, 6]})
        multi_response = MultiJSONResponse(mock_response, "results")
        multi_response.add_response(mock_response2)
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3, 4, 5, 6], multi_response.json()["results"])

    def test_pass_through_works_with_three_responses(self):
        mock_response = fake_response(status_code=200, json_return_value={"results": [1, 2, 3]})
        mock_response2 = fake_response(status_code=200, json_return_value={"results": [7, 8]})
        mock_response3 = fake_response(status_code=200, json_return_value={"results": [4, 4]})
        multi_response = MultiJSONResponse(mock_response, "results")
        multi_response.add_response(mock_response2)
        multi_response.add_response(mock_response3)
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3, 7, 8, 4, 4], multi_response.json()["results"])


class TestDataServiceApi(TestCase):
    def test_get_all_pages_one(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200,
                          json_return_value={"results": [1, 2, 3]},
                          num_pages=1)]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_all_pages(url_suffix="users", get_data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(1, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/users', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])

    def test_get_all_pages_two(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200,
                          json_return_value={"results": [1, 2, 3]},
                          num_pages=2),
            fake_response(status_code=200,
                          json_return_value={"results": [4, 5]},
                          num_pages=2)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_all_pages(url_suffix="projects", get_data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3, 4, 5], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(2, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])
        self.assertIn('"page": 2', dict_param['params'])

    def test_get_all_pages_three(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200,
                          json_return_value={"results": [1, 2, 3]},
                          num_pages=3),
            fake_response(status_code=200,
                          json_return_value={"results": [4, 5]},
                          num_pages=3),
            fake_response(status_code=200,
                          json_return_value={"results": [6, 7]},
                          num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_all_pages(url_suffix="uploads", get_data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3, 4, 5, 6, 7], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(3, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])
        self.assertIn('"page": 2', dict_param['params'])
        # Check third request
        call_args = call_args_list[2]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual({'Content-Type': 'application/json'}, dict_param['headers'])
        self.assertIn('"per_page": 10000', dict_param['params'])
        self.assertIn('"page": 3', dict_param['params'])

        #mock_requests.get.assert_has_calls(calls, any_order=False)

