from __future__ import absolute_import, print_function
from unittest import TestCase
import requests
import json
from ddsc.core.ddsapi import MultiJSONResponse, DataServiceApi, DataServiceAuth, SETUP_GUIDE_URL
from ddsc.core.ddsapi import MissingInitialSetupError, SoftwareAgentNotFoundError, AuthTokenCreationError, \
    UnexpectedPagingReceivedError, DataServiceError, DSResourceNotConsistentError, \
    retry_until_resource_is_consistent, retry_when_service_unavailable, CONNECTION_RETRY_MESSAGE
from mock import MagicMock, Mock, patch, ANY


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
    def create_mock_auth(self, config_page_size):
        mock_auth = MagicMock(set_status_msg=print, config=Mock(page_size=config_page_size))
        mock_auth.get_auth.return_value = 'authkey'
        return mock_auth

    def test_get_collection_one_page(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200,
                                     json_return_value={"results": [1, 2, 3]},
                                     num_pages=1)]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=50), url="something.com/v1/",
                             http=mock_requests)
        response = api._get_collection(url_suffix="users", data={})
        self.assertEqual([1, 2, 3], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(1, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/users', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(50, dict_param['params']['per_page'])
        self.assertEqual(1, dict_param['params']['page'])

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
        api = DataServiceApi(self.create_mock_auth(config_page_size=100), url="something.com/v1/", http=mock_requests)
        response = api._get_collection(url_suffix="projects", data={})
        self.assertEqual([1, 2, 3, 4, 5], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(2, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(100, dict_param['params']['per_page'])
        self.assertEqual(1, dict_param['params']['page'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/projects', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(100, dict_param['params']['per_page'])
        self.assertEqual(2, dict_param['params']['page'])

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
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/", http=mock_requests)
        response = api._get_collection(url_suffix="uploads", data={})
        self.assertEqual([1, 2, 3, 4, 5, 6, 7], response.json()["results"])
        call_args_list = mock_requests.get.call_args_list
        self.assertEqual(3, len(call_args_list))
        # Check first request
        call_args = call_args_list[0]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(100, dict_param['params']['per_page'])
        self.assertEqual(1, dict_param['params']['page'])
        # Check second request
        call_args = call_args_list[1]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(100, dict_param['params']['per_page'])
        self.assertEqual(2, dict_param['params']['page'])

        # Check third request
        call_args = call_args_list[2]
        first_param = call_args[0][0]
        self.assertEqual('something.com/v1/uploads', first_param)
        dict_param = call_args[1]
        self.assertEqual('application/x-www-form-urlencoded', dict_param['headers']['Content-Type'])
        self.assertIn("DukeDSClient/", dict_param['headers']['User-Agent'])
        self.assertEqual(100, dict_param['params']['per_page'])
        self.assertEqual(3, dict_param['params']['page'])

    def test_put_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        with self.assertRaises(UnexpectedPagingReceivedError):
            api._put(url_suffix='stuff', data={})

    def test_post_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        with self.assertRaises(UnexpectedPagingReceivedError):
            api._post(url_suffix='stuff', data={})

    def test_delete_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        with self.assertRaises(UnexpectedPagingReceivedError):
            api._delete(url_suffix='stuff', data={})

    def test_get_single_item_raises_error_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={}, num_pages=3)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        with self.assertRaises(UnexpectedPagingReceivedError):
            api._get_single_item(url_suffix='stuff', data={})

    def test_get_single_page_works_on_paging_response(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value={"ok": True}, num_pages=3)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        resp = api._get_single_page(url_suffix='stuff', data={}, page_num=1)
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
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_providers()
        self.assertEqual(200, result.status_code)
        self.assertEqual(json_results, result.json())
        self.assertEqual('something.com/v1/auth_providers', mock_requests.get.call_args_list[0][0][0])

    def test_get_auth_provider(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200, json_return_value={"ok": True})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_provider('provider_id_123')
        self.assertEqual(200, result.status_code)
        mock_requests.get.assert_called_with('something.com/v1/auth_providers/provider_id_123/',
                                             headers=ANY, params=ANY)

    def test_get_default_auth_provider_id(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200, json_return_value={"results": [
                {"id": "456", "is_default": False},
                {"id": "123", "is_default": True},
            ]})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        provider_id = api.get_default_auth_provider_id()
        self.assertEqual('123', provider_id)
        mock_requests.get.assert_called_with('something.com/v1/auth_providers', headers=ANY, params=ANY)

    def test_get_default_auth_provider_id_no_default_provider(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200, json_return_value={"results": [
                {"id": "456", "is_default": False},
                {"id": "123", "is_default": False},
            ]})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        with self.assertRaises(ValueError) as raised_exception:
            api.get_default_auth_provider_id()
        self.assertEqual(str(raised_exception.exception), 'Unable to find a default DukeDS auth provider.')
        mock_requests.get.assert_called_with('something.com/v1/auth_providers', headers=ANY, params=ANY)

    def make_mock_requests_for_auth_provider_affiliates(self):
        user = {
            "id": "abc4e9-9987-47eb-bb4e-19f0203efbf6",
            "username": "joe",
        }
        json_results = {
            "results": [
                user
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=json_results, num_pages=1)
        ]
        return mock_requests, json_results

    def test_get_auth_provider_affiliates_full_name_contains(self):
        mock_requests, json_results = self.make_mock_requests_for_auth_provider_affiliates()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_provider_affiliates('provider_id_123', full_name_contains='Joe Smith')
        self.assertEqual(200, result.status_code)
        self.assertEqual(json_results, result.json())
        mock_requests.get.assert_called_with('something.com/v1/auth_providers/provider_id_123/affiliates/',
                                             headers=ANY,
                                             params={'full_name_contains': 'Joe Smith', 'page': 1, 'per_page': 100})

    def test_get_auth_provider_affiliates_email(self):
        mock_requests, json_results = self.make_mock_requests_for_auth_provider_affiliates()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_provider_affiliates('provider_id_123', email='joe.smith@example.org')
        self.assertEqual(200, result.status_code)
        self.assertEqual(json_results, result.json())
        mock_requests.get.assert_called_with('something.com/v1/auth_providers/provider_id_123/affiliates/',
                                             headers=ANY,
                                             params={'email': 'joe.smith@example.org', 'page': 1, 'per_page': 100})

    def test_get_auth_provider_affiliates_username(self):
        mock_requests, json_results = self.make_mock_requests_for_auth_provider_affiliates()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_provider_affiliates('provider_id_123', username='js123')
        self.assertEqual(200, result.status_code)
        self.assertEqual(json_results, result.json())
        mock_requests.get.assert_called_with('something.com/v1/auth_providers/provider_id_123/affiliates/',
                                             headers=ANY,
                                             params={'username': 'js123', 'page': 1, 'per_page': 100})

    def test_auth_provider_affiliate_single(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200, json_return_value={"ok": True})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        result = api.get_auth_provider_affiliate('provider_id_123', 'az567')
        self.assertEqual(200, result.status_code)
        mock_requests.get.assert_called_with('something.com/v1/auth_providers/provider_id_123/affiliates/az567/',
                                             headers=ANY,
                                             params=ANY)

    def test_auth_provider_add_user(self):
        user = {
            "id": "abc4e9-9987-47eb-bb4e-19f0203efbf6",
            "username": "joe",
        }
        mock_requests = MagicMock()
        mock_requests.post.side_effect = [
            fake_response(status_code=200, json_return_value=user)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
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
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
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
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        resp = api.get_project_transfers(project_id='4521')
        self.assertEqual(1, len(resp.json()['results']))
        self.assertEqual("1234", resp.json()['results'][0]['id'])

    def test_get_all_project_transfers(self):
        return_value = {
            "results": [
                {"id": "abcd"},
                {"id": "efgh"}
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=return_value, num_pages=1)
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/",
                             http=mock_requests)
        resp = api.get_all_project_transfers()
        self.assertEqual(2, len(resp.json()['results']))
        self.assertEqual("abcd", resp.json()['results'][0]['id'])
        self.assertEqual("efgh", resp.json()['results'][1]['id'])

    def test_relations_methods(self):
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="base/v1/", http=MagicMock())
        api._get_collection = MagicMock()
        api._get_single_item = MagicMock()
        api._post = MagicMock()
        api._delete = MagicMock()

        # This endpoint is a little strange with using object_kind and relation types as magic values
        api.get_relations('dds-file', '123')
        api._get_collection.assert_called_with('/relations/dds-file/123', {})
        api.get_relation('124')
        api._get_single_item.assert_called_with('/relations/124/', {})

        api.create_used_relation('125', 'dds-file', '456')
        payload = {
            'entity': {
                'kind': 'dds-file',
                'id': '456'
            }, 'activity': {
                'id': '125'
            }
        }
        api._post.assert_called_with('/relations/used', payload)

        api.create_was_generated_by_relation('126', 'dds-file', '457')
        payload = {
            'entity': {
                'kind': 'dds-file',
                'id': '457'
            }, 'activity': {
                'id': '126'
            }
        }
        api._post.assert_called_with('/relations/was_generated_by', payload)

        api.create_was_generated_by_relation('126', 'dds-file', '457')
        payload = {
            'entity': {
                'kind': 'dds-file',
                'id': '457'
            }, 'activity': {
                'id': '126'
            }
        }
        api._post.assert_called_with('/relations/was_generated_by', payload)

        api.create_was_invalidated_by_relation('127', 'dds-file', '458')
        payload = {
            'entity': {
                'kind': 'dds-file',
                'id': '458'
            }, 'activity': {
                'id': '127'
            }
        }
        api._post.assert_called_with('/relations/was_invalidated_by', payload)

        api.create_was_derived_from_relation('128', 'dds-file', '129', 'dds-file')
        payload = {
            'generated_entity': {
                'kind': 'dds-file',
                'id': '129'
            }, 'used_entity': {
                'kind': 'dds-file',
                'id': '128'
            }
        }
        api._post.assert_called_with('/relations/was_derived_from', payload)

        api.delete_relation('130')
        api._delete.assert_called_with('/relations/130/', {})

    def test_constructor_creates_session_when_passed_none(self):
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1/", http=None)
        self.assertIsNotNone(api.http)
        self.assertEqual(type(api.http), requests.sessions.Session)

    def test_check_err_with_good_response(self):
        resp = Mock(headers={}, status_code=202)
        url_suffix = ""
        data = None
        DataServiceApi._check_err(resp, url_suffix, data, allow_pagination=False)

    def test_check_err_with_500(self):
        resp = Mock(headers={}, status_code=500)
        url_suffix = ""
        data = None
        with self.assertRaises(DataServiceError):
            DataServiceApi._check_err(resp, url_suffix, data, allow_pagination=False)

    def test_check_err_with_400(self):
        resp = Mock(headers={}, status_code=400)
        url_suffix = ""
        data = None
        with self.assertRaises(DataServiceError):
            DataServiceApi._check_err(resp, url_suffix, data, allow_pagination=False)

    def test_check_err_with_404(self):
        resp = Mock(headers={}, status_code=404)
        resp.json.return_value = {"code": "not_found"}
        url_suffix = ""
        data = None
        with self.assertRaises(DataServiceError):
            DataServiceApi._check_err(resp, url_suffix, data, allow_pagination=False)

    def test_check_err_with_404_with_flag(self):
        resp = Mock(headers={}, status_code=404)
        resp.json.return_value = {"code": "resource_not_consistent"}
        url_suffix = ""
        data = None
        with self.assertRaises(DSResourceNotConsistentError):
            DataServiceApi._check_err(resp, url_suffix, data, allow_pagination=False)

    def test_get_projects(self):
        page1 = {
            "results": [
                {
                    "id": "1234"
                }
            ]
        }
        page2 = {
            "results": [
                {
                    "id": "1235"
                }
            ]
        }
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=2),
            fake_response_with_pages(status_code=200, json_return_value=page2, num_pages=2),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        resp = api.get_projects()
        self.assertEqual(2, len(resp.json()['results']))
        self.assertEqual("1234", resp.json()['results'][0]['id'])
        self.assertEqual("1235", resp.json()['results'][1]['id'])
        self.assertEqual(2, mock_requests.get.call_count)
        first_call_second_arg = mock_requests.get.call_args_list[0][1]
        self.assertEqual('application/x-www-form-urlencoded', first_call_second_arg['headers']['Content-Type'])
        self.assertEqual(100, first_call_second_arg['params']['per_page'])
        self.assertEqual(1, first_call_second_arg['params']['page'])
        second_call_second_arg = mock_requests.get.call_args_list[0][1]
        self.assertEqual(100, second_call_second_arg['params']['per_page'])
        self.assertEqual(1, second_call_second_arg['params']['page'])

    def test_get_project_children(self):
        mock_requests = MagicMock()
        page1 = {
            "results": [
                {
                    "id": "1234"
                }
            ]
        }
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=1),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        api.get_project_children(project_id='123', name_contains='test', exclude_response_fields=['this', 'that'])
        args, kwargs = mock_requests.get.call_args
        params = kwargs['params']
        self.assertEqual('test', params['name_contains'])
        self.assertEqual('this that', params['exclude_response_fields'])

    def test_get_folder(self):
        mock_requests = MagicMock()
        mock_requests.get.side_effect = [
            fake_response(status_code=200, json_return_value={})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="base/v1", http=mock_requests)
        api.get_folder(folder_id='1023aef')
        mock_requests.get.assert_called()
        args, kw = mock_requests.get.call_args
        self.assertEqual(args[0], 'base/v1/folders/1023aef')

    def test_delete_folder(self):
        mock_requests = MagicMock()
        mock_requests.delete.side_effect = [
            fake_response(status_code=200, json_return_value={})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="base/v1", http=mock_requests)
        api.delete_folder(folder_id='1023cde')
        mock_requests.delete.assert_called()
        args, kw = mock_requests.delete.call_args
        self.assertEqual(args[0], 'base/v1/folders/1023cde')

    def test_delete_file(self):
        mock_requests = MagicMock()
        mock_requests.delete.side_effect = [
            fake_response(status_code=200, json_return_value={})
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="base/v1", http=mock_requests)
        api.delete_file(file_id='1023cde')
        mock_requests.delete.assert_called()
        args, kw = mock_requests.delete.call_args
        self.assertEqual(args[0], 'base/v1/files/1023cde')

    def test_get_project_files(self):
        mock_requests = MagicMock()
        page1 = {
            "results": [
                {
                    "id": "1234"
                }
            ]
        }
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=1),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)

        api.get_project_files(project_id='123')

        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'something.com/v1/projects/123/files')

    def test_get_project_permissions(self):
        mock_requests = MagicMock()
        page1 = {
            "results": [
                {
                    "project": {
                        "id": "8593ab3c-9999-11e8-9eb6-529269fb1459",
                    },
                    "user": {
                        "id": "8593aeac-9999-11e8-9eb6-529269fb1459",
                    },
                    "auth_role": {
                        "id": "project_admin",
                    }
                }
            ]
        }
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=1),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)

        response = api.get_project_permissions(project_id='123')

        args, kwargs = mock_requests.get.call_args
        self.assertEqual(args[0], 'something.com/v1/projects/123/permissions/')
        results = response.json()['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['project']['id'], '8593ab3c-9999-11e8-9eb6-529269fb1459')
        self.assertEqual(results[0]['user']['id'], '8593aeac-9999-11e8-9eb6-529269fb1459')
        self.assertEqual(results[0]['auth_role']['id'], 'project_admin')

    def test_get_users_no_filtering(self):
        mock_requests = MagicMock()
        page1 = {
            "results": [
                {
                    "id": "8593aeac-9999-11e8-9eb6-529269fb1459"
                }
            ]
        }
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=1),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        response = api.get_users()

        mock_requests.get.assert_called_with('something.com/v1/users', headers=ANY, params={'page': 1, 'per_page': 100})

        results = response.json()['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], '8593aeac-9999-11e8-9eb6-529269fb1459')

    def test_get_users_with_filtering(self):
        mock_requests = MagicMock()
        page1 = {
            "results": [
                {
                    "id": "8593aeac-9999-11e8-9eb6-529269fb1459"
                }
            ]
        }
        mock_requests.get.side_effect = [
            fake_response_with_pages(status_code=200, json_return_value=page1, num_pages=1),
        ]
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100), url="something.com/v1",
                             http=mock_requests)
        response = api.get_users(full_name='Joe Bob', email='joe@joe.com', username='joe')

        mock_requests.get.assert_called_with('something.com/v1/users', headers=ANY,
                                             params={'full_name_contains': 'Joe Bob', 'email': 'joe@joe.com', 'username': 'joe',
                                                     'page': 1, 'per_page': 100})

        results = response.json()['results']
        self.assertEqual(len(results), 1)
        self.assertEqual(results[0]['id'], '8593aeac-9999-11e8-9eb6-529269fb1459')

    def test_put_create_upload_url(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        mock_response = {
            "id": "8593aeac-9999-11e8-9eb6-529269fb1459"
        }
        mock_requests.put.side_effect = [
            fake_response(status_code=200, json_return_value=mock_response),
        ]
        resp = api.create_upload_url(upload_id='someId', number=1, size=200, hash_value='somehash', hash_alg='md5')
        self.assertEqual(resp.json(), mock_response)

    def test_put_create_upload_url_invalid_number(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        with self.assertRaises(ValueError) as raised_exception:
            api.create_upload_url(upload_id='someId', number=0, size=200, hash_value='somehash', hash_alg='md5')
        self.assertEqual(str(raised_exception.exception), "Chunk number must be > 0")

    def test_create_upload(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        mock_requests.post.return_value = fake_response(status_code=201, json_return_value={})
        api.create_upload(project_id='123', filename='data.txt', content_type='sometype', size=10,
                          hash_value='somehash', hash_alg='md5', storage_provider_id='abc456')
        expected_data = json.dumps({
            "name": "data.txt",
            "content_type": "sometype",
            "size": 10,
            "hash": {
                "value": "somehash",
                "algorithm": "md5"
            },
            "chunked": True,
            "storage_provider": {"id": "abc456"}
        })
        mock_requests.post.assert_called_with('something.com/v1/projects/123/uploads',
                                              expected_data,
                                              headers=ANY)

    def test_create_non_chunked_upload(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        mock_requests.post.return_value = fake_response(status_code=201, json_return_value={})
        api.create_upload(project_id='123', filename='data.txt', content_type='sometype', size=10,
                          hash_value='somehash', hash_alg='md5', storage_provider_id='abc456',
                          chunked=False)
        expected_data = json.dumps({
            "name": "data.txt",
            "content_type": "sometype",
            "size": 10,
            "hash": {
                "value": "somehash",
                "algorithm": "md5"
            },
            "chunked": False,
            "storage_provider": {"id": "abc456"}
        })
        mock_requests.post.assert_called_with('something.com/v1/projects/123/uploads',
                                              expected_data,
                                              headers=ANY)

    def test_rename_file(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        api._put = Mock()
        resp = api.rename_file(file_id='abc123', name='bettername.txt')
        self.assertEqual(resp, api._put.return_value)
        api._put.assert_called_with('/files/abc123/rename', {'name': 'bettername.txt'})

    def test_move_file(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        api._put = Mock()
        resp = api.move_file(file_id='abc123', parent_kind_str='dds-folder', parent_uuid='def456')
        self.assertEqual(resp, api._put.return_value)
        api._put.assert_called_with('/files/abc123/move', {'parent': {'kind': 'dds-folder', 'id': 'def456'}})

    def test_rename_folder(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        api._put = Mock()
        resp = api.rename_folder(folder_id='abc123', name='betterfolder')
        self.assertEqual(resp, api._put.return_value)
        api._put.assert_called_with('/folders/abc123/rename', {'name': 'betterfolder'})

    def test_move_folder(self):
        mock_requests = MagicMock()
        api = DataServiceApi(auth=self.create_mock_auth(config_page_size=100),
                             url="something.com/v1",
                             http=mock_requests)
        api._put = Mock()
        resp = api.move_folder(folder_id='abc123', parent_kind_str='dds-folder', parent_uuid='def456')
        self.assertEqual(resp, api._put.return_value)
        api._put.assert_called_with('/folders/abc123/move', {'parent': {'kind': 'dds-folder', 'id': 'def456'}})


class TestDataServiceAuth(TestCase):
    @patch('ddsc.core.ddsapi.get_user_agent_str')
    @patch('ddsc.core.ddsapi.requests')
    def test_claim_new_token(self, mock_requests, mock_get_user_agent_str):
        mock_get_user_agent_str.return_value = ''
        payload = {
            'api_token': 'abc',
            'expires_on': '123'
        }
        response = Mock(status_code=201)
        response.json.return_value = payload
        mock_requests.post.return_value = response
        config = Mock(url='', user_key='', agent_key='')
        auth = DataServiceAuth(config)
        auth.claim_new_token()
        self.assertEqual(auth.get_auth_data(), ('abc', '123'))

    @patch('ddsc.core.ddsapi.get_user_agent_str')
    @patch('ddsc.core.ddsapi.requests')
    def test_claim_new_token_missing_setup(self, mock_requests, mock_get_user_agent_str):
        config = Mock(url='', user_key='', agent_key='')
        mock_requests.exceptions.ConnectionError = requests.exceptions.ConnectionError
        mock_requests.post.return_value = Mock(status_code=404)
        auth = DataServiceAuth(config)
        with self.assertRaises(MissingInitialSetupError):
            auth.claim_new_token()

    @patch('ddsc.core.ddsapi.get_user_agent_str')
    @patch('ddsc.core.ddsapi.requests')
    def test_claim_new_token_missing_agent(self, mock_requests, mock_get_user_agent_str):
        config = Mock(url='', user_key='', agent_key='abc')
        mock_requests.exceptions.ConnectionError = requests.exceptions.ConnectionError
        mock_requests.post.return_value = Mock(status_code=404)
        auth = DataServiceAuth(config)
        with self.assertRaises(SoftwareAgentNotFoundError):
            auth.claim_new_token()

    @patch('ddsc.core.ddsapi.get_user_agent_str')
    @patch('ddsc.core.ddsapi.requests')
    def test_claim_new_token_error(self, mock_requests, mock_get_user_agent_str):
        config = Mock(url='', user_key='', agent_key='abc')
        mock_requests.post.return_value = Mock(status_code=500, text='service down')
        mock_requests.exceptions.ConnectionError = requests.exceptions.ConnectionError
        auth = DataServiceAuth(config)
        with self.assertRaises(AuthTokenCreationError) as err:
            auth.claim_new_token()
        error_message = str(err.exception)
        self.assertIn('500', error_message)
        self.assertIn('service down', error_message)


class TestMissingInitialSetupError(TestCase):
    @patch('ddsc.core.ddsapi.get_user_config_filename')
    def test_constructor(self, mock_get_user_config_filename):
        mock_get_user_config_filename.return_value = '/tmp/ddsc.config'
        with self.assertRaises(MissingInitialSetupError) as err:
            raise MissingInitialSetupError()
        error_message = str(err.exception)
        self.assertIn('Missing initial setup', error_message)
        self.assertIn('/tmp/ddsc.config', error_message)
        self.assertIn(SETUP_GUIDE_URL, error_message)


class TestSoftwareAgentNotFoundError(TestCase):
    @patch('ddsc.core.ddsapi.get_user_config_filename')
    def test_constructor(self, mock_get_user_config_filename):
        mock_get_user_config_filename.return_value = '/tmp/ddsc_other.config'
        with self.assertRaises(SoftwareAgentNotFoundError) as err:
            raise SoftwareAgentNotFoundError()
        error_message = str(err.exception)
        self.assertIn('Your software agent was not found', error_message)
        self.assertIn('/tmp/ddsc_other.config', error_message)


class TestUnexpectedPagingReceivedError(TestCase):
    def test_constructor(self):
        with self.assertRaises(UnexpectedPagingReceivedError) as err:
            raise UnexpectedPagingReceivedError()
        error_message = str(err.exception)
        self.assertIn('Received unexpected paging data', error_message)


class TestAuthTokenCreationError(TestCase):
    def test_constructor(self):
        request = Mock(status_code=400, text='Bad data')
        with self.assertRaises(AuthTokenCreationError) as err:
            raise AuthTokenCreationError(request)
        error_message = str(err.exception)
        self.assertIn('Failed to create auth token', error_message)
        self.assertIn('400', error_message)
        self.assertIn('Bad data', error_message)


class TestInconsistentResourceMonitoring(TestCase):
    @patch('ddsc.core.ddsapi.time.sleep')
    def test_retry_until_resource_is_consistent(self, mock_sleep):
        func = MagicMock()
        func.return_value = 'ok'
        monitor = MagicMock()
        ret = retry_until_resource_is_consistent(func, monitor)
        self.assertEqual('ok', ret)
        monitor.start_waiting.assert_not_called()
        monitor.done_waiting.assert_not_called()

    @patch('ddsc.core.ddsapi.time.sleep')
    def test_retry_until_resource_is_consistent_with_one_retry(self, mock_sleep):
        func = MagicMock()
        func.side_effect = [
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            'ok'
        ]
        monitor = MagicMock()
        ret = retry_until_resource_is_consistent(func, monitor)
        self.assertEqual('ok', ret)
        monitor.start_waiting.assert_called()
        self.assertEqual(1, monitor.start_waiting.call_count)
        monitor.done_waiting.assert_called()
        self.assertEqual(1, monitor.done_waiting.call_count)

    @patch('ddsc.core.ddsapi.time.sleep')
    def test_retry_until_resource_is_consistent_with_two_retries(self, mock_sleep):
        func = MagicMock()
        func.side_effect = [
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            DSResourceNotConsistentError(MagicMock(), MagicMock(), MagicMock()),
            'ok'
        ]
        monitor = MagicMock()
        ret = retry_until_resource_is_consistent(func, monitor)
        self.assertEqual('ok', ret)
        monitor.start_waiting.assert_called()
        self.assertEqual(1, monitor.start_waiting.call_count)
        monitor.done_waiting.assert_called()
        self.assertEqual(1, monitor.done_waiting.call_count)


class TestRetryWhenServiceUnavailable(TestCase):
    def setUp(self):
        self.raise_error_once = None
        self.raise_error_always = None
        self.status_messages = []

    @retry_when_service_unavailable
    def func(self, param):
        if self.raise_error_once:
            raise_error_once = self.raise_error_once
            self.raise_error_once = None
            raise raise_error_once
        if self.raise_error_always:
            raise self.raise_error_always
        return 'result' + param

    def set_status_message(self, msg):
        self.status_messages.append(msg)

    @patch('ddsc.core.ddsapi.time')
    def test_returns_value_when_ok(self, mock_time):
        self.assertEqual('result123', self.func('123'))
        self.assertEqual(0, mock_time.sleep.call_count)
        self.assertEqual([], self.status_messages)

    @patch('ddsc.core.ddsapi.time')
    def test_will_retry_after_waiting_after_503(self, mock_time):
        mock_response = MagicMock(status_code=503)
        self.raise_error_once = DataServiceError(mock_response, '', '')
        self.assertEqual('result123', self.func('123'))
        self.assertEqual(1, mock_time.sleep.call_count)
        self.assertEqual(2, len(self.status_messages))
        self.assertIn('Duke Data Service is currently unavailable', self.status_messages[0])
        self.assertEqual('', self.status_messages[1])

    @patch('ddsc.core.ddsapi.time')
    def test_will_retry_after_waiting_after_connection_error(self, mock_time):
        self.raise_error_once = requests.exceptions.ConnectionError()
        self.assertEqual('result123', self.func('123'))
        self.assertEqual(1, mock_time.sleep.call_count)
        self.assertEqual(2, len(self.status_messages))
        self.assertEqual(CONNECTION_RETRY_MESSAGE, self.status_messages[0])
        self.assertEqual('', self.status_messages[1])

    @patch('ddsc.core.ddsapi.time')
    def test_will_retry_after_waiting_after_continous_connection_errors(self, mock_time):
        self.raise_error_always = requests.exceptions.ConnectionError()
        try:
            self.assertEqual('result123', self.func('123'))
        except requests.exceptions.ConnectionError:
            pass
        self.assertEqual(5, mock_time.sleep.call_count)
        self.assertEqual(1, len(self.status_messages))
        self.assertEqual(CONNECTION_RETRY_MESSAGE, self.status_messages[0])

    @patch('ddsc.core.ddsapi.time')
    def test_will_just_raise_when_other_error(self, mock_time):
        mock_response = MagicMock(status_code=500)
        self.raise_error_once = DataServiceError(mock_response, '', '')
        with self.assertRaises(DataServiceError):
            self.func('123')
        self.assertEqual(0, mock_time.sleep.call_count)
        self.assertEqual([], self.status_messages)
