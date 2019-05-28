from unittest import TestCase
from mock import Mock, patch, call
from ddsc.core.userutil import UserUtil, LookupUserByEmail


class UserUtilTestCase(TestCase):
    def setUp(self):
        self.data_service = Mock()
        self.logging_func = Mock()
        self.user_util = UserUtil(self.data_service, self.logging_func)

    def test_constructor(self):
        self.assertEqual(self.user_util.auth_provider_id,
                         self.data_service.get_default_auth_provider_id.return_value)

    @patch('ddsc.core.userutil.LookupUserByEmail')
    def test_valid_dds_user_or_affiliate_exists_for_email__valid_user_exists(self, mock_lookup):
        mock_lookup.return_value.get_dds_user_with_valid_email.return_value = {"id": "123"}
        mock_lookup.return_value.get_affiliate_user_with_valid_email.return_value = None
        result = self.user_util.valid_dds_user_or_affiliate_exists_for_email("fakeuser@duke.edu")
        self.assertEqual(result, True)
        mock_lookup.assert_called_with(self.user_util, "fakeuser@duke.edu", self.logging_func)
        self.logging_func.assert_not_called()

    @patch('ddsc.core.userutil.LookupUserByEmail')
    def test_valid_dds_user_or_affiliate_exists_for_email__valid_user_exists(self, mock_lookup):
        mock_lookup.return_value.get_dds_user_with_valid_email.return_value = None
        mock_lookup.return_value.get_affiliate_user_with_valid_email.return_value = {"id": "123"}
        result = self.user_util.valid_dds_user_or_affiliate_exists_for_email("fakeuser@duke.edu")
        self.assertEqual(result, True)
        mock_lookup.assert_called_with(self.user_util, "fakeuser@duke.edu", self.logging_func)
        self.logging_func.assert_not_called()

    @patch('ddsc.core.userutil.LookupUserByEmail')
    def test_valid_dds_user_or_affiliate_exists_for_email__valid_user_exists(self, mock_lookup_user_by_email):
        mock_lookup_user_by_email.return_value.get_dds_user_with_valid_email.return_value = None
        mock_lookup_user_by_email.return_value.get_affiliate_user_with_valid_email.return_value = None
        result = self.user_util.valid_dds_user_or_affiliate_exists_for_email("fakeuser@duke.edu")
        self.assertEqual(result, False)
        mock_lookup_user_by_email.assert_called_with(self.user_util, "fakeuser@duke.edu", self.logging_func)
        self.logging_func.assert_called_with(
            "No valid DukeDS/Affiliate user found for email address fakeuser@duke.edu.")

    def test_find_dds_user_by_email(self):
        self.data_service.get_users.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_dds_user_by_email("fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_users.assert_called_with(email="fakeuser@duke.edu")

    def test_find_dds_user_by_username(self):
        self.data_service.get_users.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_dds_user_by_username("fakeuser")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_users.assert_called_with(username="fakeuser")

    def test_find_affiliate_user_by_email(self):
        self.data_service.get_auth_provider_affiliates.return_value.json.return_value = {"results": [{"id": "123"}]}
        result = self.user_util.find_affiliate_user_by_email("fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})
        self.data_service.get_auth_provider_affiliates.assert_called_with(self.user_util.auth_provider_id,
                                                                          email="fakeuser@duke.edu")

    def test_register_dds_user_by_username(self):
        self.data_service.auth_provider_add_user.return_value.json.return_value = {"id": "123"}
        result = self.user_util.register_dds_user_by_username("fakeuser")
        self.assertEqual(result, {"id": "123"})
        self.data_service.auth_provider_add_user.assert_called_with(self.user_util.auth_provider_id, "fakeuser")

    @patch('ddsc.core.userutil.LookupUserByEmail')
    def test_register_dds_user_with_email__finds_affiliate(self, mock_lookup):
        mock_lookup.return_value.get_affiliate_user_with_valid_email.return_value = {"uid": "fakeuser"}
        self.data_service.auth_provider_add_user.return_value.json.return_value = {"id": "123"}
        result = self.user_util.register_dds_user_with_email("fakeuser@duke.edu")
        self.assertEqual(result, {"id": "123"})
        mock_lookup.assert_called_with(self.user_util, "fakeuser@duke.edu", self.logging_func)
        self.data_service.auth_provider_add_user.assert_called_with(self.user_util.auth_provider_id, "fakeuser")

    @patch('ddsc.core.userutil.LookupUserByEmail')
    def test_register_dds_user_with_email__no_affiliate_found(self, mock_lookup):
        mock_lookup.return_value.get_affiliate_user_with_valid_email.return_value = None
        self.data_service.auth_provider_add_user.return_value.json.return_value = {"id": "123"}
        with self.assertRaises(ValueError) as raised_exception:
            result = self.user_util.register_dds_user_with_email("fakeuser@duke.edu")
        self.assertEqual(str(raised_exception.exception), 'Unable to register user with email address fakeuser@duke.edu')
        mock_lookup.assert_called_with(self.user_util, "fakeuser@duke.edu", self.logging_func)
        self.data_service.auth_provider_add_user.assert_not_called()

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


class LookupUserByEmailTestCase(TestCase):
    def setUp(self):
        self.dds_user_util = Mock()
        self.logging_func = Mock()
        self.lookup = LookupUserByEmail(self.dds_user_util, "fakeuser@duke.edu", self.logging_func)

    def test_constructor(self):
        self.assertEqual(self.lookup.possible_username, self.lookup._extract_username_or_none("fakeuser@duke.edu"))

    def test_get_dds_user_with_valid_email__dds_user_has_email(self):
        self.dds_user_util.find_dds_user_by_email.return_value = {"email": "fakeuser@duke.edu"}
        dds_user = self.lookup.get_dds_user_with_valid_email()
        self.assertEqual(dds_user, {"email": "fakeuser@duke.edu"})
        self.logging_func.assert_called_with('Found valid DukeDS user for email address fakeuser@duke.edu.')
        self.dds_user_util.find_dds_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_dds_user_by_username.assert_not_called()

    def test_get_dds_user_with_valid_email__affiliate_has_email(self):
        self.dds_user_util.find_dds_user_by_email.return_value = None
        self.dds_user_util.find_dds_user_by_username.return_value = {"email": "fakeuser@duke.edu"}
        dds_user = self.lookup.get_dds_user_with_valid_email()
        self.assertEqual(dds_user, {"email": "fakeuser@duke.edu"})
        self.logging_func.assert_called_with('Found valid DukeDS user for username fakeuser.')
        self.dds_user_util.find_dds_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_dds_user_by_username.assert_called_with("fakeuser")

    def test_get_dds_user_with_valid_email__not_found(self):
        self.dds_user_util.find_dds_user_by_email.return_value = None
        self.dds_user_util.find_dds_user_by_username.return_value = None
        dds_user = self.lookup.get_dds_user_with_valid_email()
        self.assertEqual(dds_user, None)
        self.logging_func.assert_not_called()
        self.dds_user_util.find_dds_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_dds_user_by_username.assert_called_with("fakeuser")

    def test_get_affiliate_user_with_valid_email__affiliate_has_email(self):
        self.dds_user_util.find_affiliate_user_by_email.return_value = {"email": "fakeuser@duke.edu"}
        affiliate_user = self.lookup.get_affiliate_user_with_valid_email()
        self.assertEqual(affiliate_user, {"email": "fakeuser@duke.edu"})
        self.logging_func.assert_called_with('Found valid affiliate user for email address fakeuser@duke.edu.')
        self.dds_user_util.find_affiliate_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_affiliate_user_by_username.assert_not_called()

    def test_get_affiliate_user_with_valid_email__affiliate_username_has_email(self):
        self.dds_user_util.find_affiliate_user_by_email.return_value = None
        self.dds_user_util.find_affiliate_user_by_username.return_value = {"email": "fakeuser@duke.edu"}
        affiliate_user = self.lookup.get_affiliate_user_with_valid_email()
        self.assertEqual(affiliate_user, {"email": "fakeuser@duke.edu"})
        self.logging_func.assert_called_with('Found valid affiliate user for username fakeuser.')
        self.dds_user_util.find_affiliate_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_affiliate_user_by_username.assert_called_with("fakeuser")

    def test_get_affiliate_user_with_valid_email__not_found(self):
        self.dds_user_util.find_affiliate_user_by_email.return_value = None
        self.dds_user_util.find_affiliate_user_by_username.return_value = None
        affiliate_user = self.lookup.get_affiliate_user_with_valid_email()
        self.assertEqual(affiliate_user, None)
        self.logging_func.assert_not_called()
        self.dds_user_util.find_affiliate_user_by_email.assert_called_with("fakeuser@duke.edu")
        self.dds_user_util.find_affiliate_user_by_username.assert_called_with("fakeuser")

    def test__item_has_email(self):
        self.assertFalse(self.lookup._item_has_email(None))
        self.assertFalse(self.lookup._item_has_email({'email': None}))
        self.assertFalse(self.lookup._item_has_email({'email': ''}))
        self.assertTrue(self.lookup._item_has_email({'email': 'fakeuser@duke.edu'}))

    def test__extract_username_or_none(self):
        self.assertEqual(self.lookup._extract_username_or_none('fakeuser123@duke.edu'), 'fakeuser123')
        self.assertEqual(self.lookup._extract_username_or_none('Fake.User@duke.edu'), None)
        self.assertEqual(self.lookup._extract_username_or_none('Fakeuser123@duke.edu'), None)
        self.assertEqual(self.lookup._extract_username_or_none('fakeuser123@fake.com'), None)

    def test_is_duke_email(self):
        self.assertEqual(LookupUserByEmail.is_duke_email('fakeuser@fake.com'), False)
        self.assertEqual(LookupUserByEmail.is_duke_email('fakeuser@duke.edu'), True)

    def test_strip_email_suffix(self):
        self.assertEqual(LookupUserByEmail.strip_email_suffix('fakeuser@fake.com'), 'fakeuser')
