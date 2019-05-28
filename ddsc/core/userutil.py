import re
import logging

DUKE_EMAIL_SUFFIX = "@duke.edu"


class UserUtil(object):
    def __init__(self, data_service, logging_func=logging.info):
        self.data_service = data_service
        self.auth_provider_id = data_service.get_default_auth_provider_id()
        self.logging_func = logging_func

    def valid_dds_user_or_affiliate_exists_for_email(self, email_address):
        lookup = LookupUserByEmail(self, email_address, self.logging_func)
        if lookup.valid_dds_user_exists() or lookup.valid_affiliate_user_exists():
            return True
        else:
            self.logging_func("No valid DukeDS/Affiliate user found for email address {}.".format(email_address))
            return False

    def find_dds_user_by_email(self, email_address):
        response = self.data_service.get_users(email=email_address)
        return self._get_single_user_or_none(response, email_address)

    def find_dds_user_by_username(self, username):
        response = self.data_service.get_users(username=username)
        return self._get_single_user_or_none(response, username)

    def find_affiliate_user_by_email(self, email_address):
        response = self.data_service.get_auth_provider_affiliates(self.auth_provider_id, email=email_address)
        return self._get_single_user_or_none(response, email_address)

    def find_affiliate_user_by_username(self, username):
        response = self.data_service.get_auth_provider_affiliates(self.auth_provider_id, username=username)
        return self._get_single_user_or_none(response, username)

    def register_dds_user_by_username(self, username):
        response = self.data_service.auth_provider_add_user(self.auth_provider_id, username)
        return response.json()

    def register_user_with_email(self, email_address):
        lookup = LookupUserByEmail(self, email_address, self.logging_func)
        affiliate_user = lookup.get_affiliate_user_with_valid_email()
        if affiliate_user:
            return self.register_user_with_username(affiliate_user["uid"])
        else:
            raise ValueError("Unable to register user with email address {}".format(self.email_address))

    @staticmethod
    def _get_single_user_or_none(response, lookup_value):
        results = response.json()['results']
        if not results:
            return None
        if len(results) == 1:
            return results[0]
        raise ValueError("Found multiple users for {}.".format(lookup_value))


class LookupUserByEmail(object):
    def __init__(self, dds_user_util, email_address, logging_func):
        self.dds_user_util = dds_user_util
        self.email_address = email_address
        self.logging_func = logging_func
        self.possible_username = self._extract_username_or_none(email_address)

    def valid_dds_user_exists(self):
        if self.get_dds_user_with_valid_email():
            return True
        return False

    def get_dds_user_with_valid_email(self):
        dds_user = self.dds_user_util.find_dds_user_by_email(self.email_address)
        if self._item_has_email(dds_user):
            self.logging_func("Found valid DukeDS user for email address {}.".format(self.email_address))
            return dds_user
        if self.possible_username:
            dds_user = self.dds_user_util.find_dds_user_by_username(self.possible_username)
            if self._item_has_email(dds_user):
                self.logging_func("Found valid DukeDS user for username {}.".format(self.possible_username))
                return dds_user
        return None

    def valid_affiliate_user_exists(self):
        if self.get_affiliate_user_with_valid_email():
            return True
        return False

    def get_affiliate_user_with_valid_email(self):
        affiliate_user = self.dds_user_util.find_affiliate_user_by_email(self.email_address)
        if self._item_has_email(affiliate_user):
            self.logging_func("Found valid affiliate user for email address {}.".format(self.email_address))
            return affiliate_user
        if self.possible_username:
            affiliate_user = self.dds_user_util.find_affiliate_user_by_username(self.possible_username)
            if self._item_has_email(affiliate_user):
                self.logging_func("Found valid affiliate user for username {}.".format(self.possible_username))
                return affiliate_user
        return None

    @staticmethod
    def _item_has_email(item):
        return item and item['email']

    def _extract_username_or_none(self, email_address):
        """
        If email_address is a duke email address and the local part is a username return that username or None.
        Duke emails take two forms one with the full name and another where the local part is the username.
        Usernames must be only lowercase alphanumeric.
        :param email_address: str
        :return: bool: str or None
        """
        if self.is_duke_email():
            local_part = self.strip_email_suffix(email_address)
            if local_part.islower() and local_part.isalnum():
                return local_part
        return None

    def is_duke_email(self):
        return self.email_address.endswith(DUKE_EMAIL_SUFFIX)

    @staticmethod
    def strip_email_suffix(email_address):
        return re.sub('@.*$', '', email_address)
