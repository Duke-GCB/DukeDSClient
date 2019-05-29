from unittest import TestCase
from mock import Mock
from ddsc.core.userutil import UserUtil


class UserUtilTestCase(TestCase):
    def setUp(self):
        self.data_service = Mock()
        self.logging_func = Mock()
        self.user_util = UserUtil(self.data_service, self.logging_func)

    def test_constructor(self):
        self.assertEqual(self.user_util.auth_provider_id,
                         self.data_service.get_default_auth_provider_id.return_value)

    def test_find_user_by_username(self):
        self.data_service.get_users.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_user_by_username("fakeuser")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_users.assert_called_with(username="fakeuser")

    def test_register_user_by_username(self):
        self.data_service.auth_provider_add_user.return_value.json.return_value = {"id": "123"}
        result = self.user_util.register_user_by_username("fakeuser")
        self.assertEqual(result, {"id": "123"})
        self.data_service.auth_provider_add_user.assert_called_with(self.user_util.auth_provider_id, "fakeuser")

    def test_find_user_by_email(self):
        self.data_service.get_users.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_user_by_email("fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_users.assert_called_with(email="fakeuser@duke.edu")

    def test_find_affiliate_by_email(self):
        self.data_service.get_auth_provider_affiliates.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_affiliate_by_email("fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_auth_provider_affiliates.assert_called_with(self.user_util.auth_provider_id,
                                                                          email="fakeuser@duke.edu")

    def test_find_affiliate_by_username(self):
        self.data_service.get_auth_provider_affiliates.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_affiliate_by_username("fakeuser")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_auth_provider_affiliates.assert_called_with(self.user_util.auth_provider_id,
                                                                          username="fakeuser")

    def test_user_or_affiliate_exists_for_email__dds_user_exists(self):
        self.user_util.find_user_by_email = Mock()
        self.user_util.find_user_by_email.return_value = {"id": "123"}

        self.assertTrue(self.user_util.user_or_affiliate_exists_for_email("fakeuser@duke.edu"))
        self.user_util.find_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.logging_func.assert_called_with(
            "Found DukeDS user for email address fakeuser@duke.edu.")

    def test_user_or_affiliate_exists_for_email__valid_affiliate_exists(self):
        self.user_util.find_user_by_email = Mock()
        self.user_util.find_user_by_email.return_value = None
        self.user_util.find_affiliate_by_email = Mock()
        self.user_util.find_affiliate_by_email.return_value = {"uid": "123"}

        self.assertTrue(self.user_util.user_or_affiliate_exists_for_email("fakeuser@duke.edu"))
        self.user_util.find_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.user_util.find_affiliate_by_email.assert_called_with("fakeuser@duke.edu")
        self.logging_func.assert_called_with(
            "Found affiliate for email address fakeuser@duke.edu.")

    def test_valid_dds_user_or_affiliate_exists_for_email__no_user_found(self):
        self.user_util.find_user_by_email = Mock()
        self.user_util.find_user_by_email.return_value = None
        self.user_util.find_affiliate_by_email = Mock()
        self.user_util.find_affiliate_by_email.return_value = None

        self.assertFalse(self.user_util.user_or_affiliate_exists_for_email("fakeuser@duke.edu"))
        self.user_util.find_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.user_util.find_affiliate_by_email.assert_called_with("fakeuser@duke.edu")
        self.logging_func.assert_called_with(
            "No valid DukeDS user or affiliate found for email address fakeuser@duke.edu.")

    def test_user_or_affiliate_exists_for_username__dds_user_exists(self):
        self.user_util.find_user_by_username = Mock()
        self.user_util.find_user_by_username.return_value = {"id": "123"}

        self.assertTrue(self.user_util.user_or_affiliate_exists_for_username("fakeuser"))
        self.user_util.find_user_by_username.assert_called_with("fakeuser")
        self.logging_func.assert_called_with(
            "Found DukeDS user for username fakeuser.")

    def test_user_or_affiliate_exists_for_username__valid_affiliate_exists(self):
        self.user_util.find_user_by_username = Mock()
        self.user_util.find_user_by_username.return_value = None
        self.user_util.find_affiliate_by_username = Mock()
        self.user_util.find_affiliate_by_username.return_value = {"uid": "123"}

        self.assertTrue(self.user_util.user_or_affiliate_exists_for_username("fakeuser"))
        self.user_util.find_user_by_username.assert_called_with("fakeuser")
        self.user_util.find_affiliate_by_username.assert_called_with("fakeuser")
        self.logging_func.assert_called_with(
            "Found affiliate for username fakeuser.")

    def test_valid_dds_user_or_affiliate_exists_for_username__no_user_found(self):
        self.user_util.find_user_by_username = Mock()
        self.user_util.find_user_by_username.return_value = None
        self.user_util.find_affiliate_by_username = Mock()
        self.user_util.find_affiliate_by_username.return_value = None

        self.assertFalse(self.user_util.user_or_affiliate_exists_for_username("fakeuser"))
        self.user_util.find_user_by_username.assert_called_with("fakeuser")
        self.user_util.find_affiliate_by_username.assert_called_with("fakeuser")
        self.logging_func.assert_called_with(
            "No valid DukeDS user or affiliate found for username fakeuser.")

    def test__get_single_user_or_none(self):
        response = Mock()

        # When no items found should return None
        response.json.return_value = {"results": []}
        result = self.user_util._get_single_user_or_none(response, lookup_value="fakeuser@duke.edu")
        self.assertEqual(result, None)

        # When one item found should return first (only) item
        response.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util._get_single_user_or_none(response, lookup_value="fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})

        # When multiple found raise exception
        response.json.return_value = {"results": [{"id": "123"}, {"id": "456"}]}
        with self.assertRaises(ValueError) as raised_exception:
            self.user_util._get_single_user_or_none(response, lookup_value="fakeuser@duke.edu")
        self.assertEqual(str(raised_exception.exception), 'Found multiple users for fakeuser@duke.edu.')
