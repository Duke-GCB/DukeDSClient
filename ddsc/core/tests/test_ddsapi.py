from __future__ import absolute_import
from unittest import TestCase
from ddsc.core.ddsapi import MultiJSONResponse, DataServiceApi, ContentType, UNEXPECTED_PAGING_DATA_RECEIVED
from mock import MagicMock


def fake_response_with_pages(status_code, json_return_value, num_pages=1):
    mock_response = MagicMock(status_code=status_code, headers={'x-total-pages': "{}".format(num_pages)})
    mock_response.json.return_value = json_return_value
    return mock_response


def fake_response(status_code, json_return_value):
    mock_response = MagicMock(status_code=status_code, headers={})
    mock_response.json.return_value = json_return_value
    return mock_response


class TestMultiJSONResponse(TestCase):
    """
    Tests that we can merge multiple JSON responses arrays with a given name(merge_array_field_name).
    """

    def test_pass_through_works_with_one_response(self):
        mock_response = fake_response_with_pages(status_code=200, json_return_value={"results": [1, 2, 3]})
        multi_response = MultiJSONResponse(mock_response, "results")
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3], multi_response.json()["results"])

    def test_pass_through_works_with_two_responses(self):
        mock_response = fake_response_with_pages(status_code=200, json_return_value={"results": [1, 2, 3]})
        mock_response2 = fake_response_with_pages(status_code=200, json_return_value={"results": [4, 5, 6]})
        multi_response = MultiJSONResponse(mock_response, "results")
        multi_response.add_response(mock_response2)
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3, 4, 5, 6], multi_response.json()["results"])

    def test_pass_through_works_with_three_responses(self):
        mock_response = fake_response_with_pages(status_code=200, json_return_value={"results": [1, 2, 3]})
        mock_response2 = fake_response_with_pages(status_code=200, json_return_value={"results": [7, 8]})
        mock_response3 = fake_response_with_pages(status_code=200, json_return_value={"results": [4, 4]})
        multi_response = MultiJSONResponse(mock_response, "results")
        multi_response.add_response(mock_response2)
        multi_response.add_response(mock_response3)
        self.assertEqual(200, multi_response.status_code)
        self.assertEqual([1, 2, 3, 7, 8, 4, 4], multi_response.json()["results"])


class TestDataServiceApi(TestCase):
    def test_get_collection_one_page(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [1, 2, 3]},
                                     num_pages=1)]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_collection(url_suffix="users", data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(1, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/users', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])

    def test_get_collection_two_pages(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [1, 2, 3]},
                                     num_pages=2),
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [4, 5]},
                                     num_pages=2)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_collection(url_suffix="projects", data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3, 4, 5], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(2, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])
        self.assertIn('"page": 2', dict_param['params'])

    def test_get_collection_three_pages(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [1, 2, 3]},
                                     num_pages=3),
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [4, 5]},
                                     num_pages=3),
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [6, 7]},
                                     num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        response = api._get_collection(url_suffix="uploads", data={}, content_type=ContentType.json)
        self.assertEqual([1, 2, 3, 4, 5, 6, 7], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(3, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])
        self.assertIn('"page": 2', dict_param['params'])
        # Check third request
        call_args = call_args_list[2]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/json', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertIn('"per_page": 100', dict_param['params'])
        self.assertIn('"page": 3', dict_param['params'])

    def test_put_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        with self.assertRaises(ValueError) as er:
            api._put(url_suffix='stuff', data={})
        self.assertEqual(UNEXPECTED_PAGING_DATA_RECEIVED, str(er.exception))

    def test_post_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        with self.assertRaises(ValueError) as er:
            api._post(url_suffix='stuff', data={})
        self.assertEqual(UNEXPECTED_PAGING_DATA_RECEIVED, str(er.exception))

    def test_delete_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        with self.assertRaises(ValueError) as er:
            api._delete(url_suffix='stuff', data={})
        self.assertEqual(UNEXPECTED_PAGING_DATA_RECEIVED, str(er.exception))

    def test_get_single_item_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        with self.assertRaises(ValueError) as er:
            api._get_single_item(url_suffix='stuff', data={})
        self.assertEqual(UNEXPECTED_PAGING_DATA_RECEIVED, str(er.exception))

    def test_get_single_page_works_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={"ok": True}, num_pages=3)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        resp = api._get_single_page(url_suffix='stuff', data={}, content_type=ContentType.json, page_num=1)
        self.assertEqual(True, resp.json()['ok'])

    def test_get_auth_providers(self):
        provider = {
            "id": "aca35ba3-a44a-47c2-8b3b-afe43a88360d",
            "service_id": "cfde039d-f550-47e7-833c-9ebc4e257847",
            "name": "Duke Authentication Service",
            "is_deprecated": True,
            "is_default": True,
            "login_initiation_url": "https://someurl"
        }
        json_results = {
            "results": [
                provider
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=json_results, num_pages=1)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1", http=mock_requests)
        result = api.get_auth_providers()
        self.assertEqual(200, result.status_code)
        self.assertEqual(json_results, result.json())
        self.assertEqual('something.com/v1/auth_providers', mock_requests.get.call_args_list[0][0][0])

    def test_auth_provider_add_user(self):
        user = {
            "id": "abc4e9-9987-47eb-bb4e-19f0203efbf6",
            "username": "joe",
        }
        mock_requests = MagicMock()
        mock_requests.post.side_effect = [
            fake_response(status_code=200, json_return_value=user)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1", http=mock_requests)
        result = api.auth_provider_add_user('123', "joe")
        self.assertEqual(200, result.status_code)
        self.assertEqual(user, result.json())
        expected_url = 'something.com/v1/auth_providers/123/affiliates/joe/dds_user/'
        self.assertEqual(expected_url, mock_requests.post.call_args_list[0][0][0])

    def test_list_auth_roles(self):
        return_value = {
            "results": [
                {
                    "id": "project_admin",
                    "name": "Project Admin",
                }
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=return_value, num_pages=1)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        resp = api.get_auth_roles(context='')
        self.assertEqual(1, len(resp.json()['results']))
        self.assertEqual("Project Admin", resp.json()['results'][0]['name'])

    def test_get_project_transfers(self):
        return_value = {
            "results": [
                {
                    "id": "1234"
                }
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=return_value, num_pages=1)
        ]
        api = DataServiceApi(auth=None, url="something.com/v1/", http=mock_requests)
        resp = api.get_project_transfers(project_id='4521')
        self.assertEqual(1, len(resp.json()['results']))
        self.assertEqual("1234", resp.json()['results'][0]['id'])
