import re
import logging

DUKE_EMAIL_SUFFIX = "@duke.edu"


class UserUtil(object):
    def __init__(self, data_service, logging_func=logging.info):
        self.data_service = data_service
        self.auth_provider_id = data_service.get_default_auth_provider_id()
        self.logging_func = logging_func

    def find_user_by_username(self, username):
        response = self.data_service.get_users(username=username)
        return self._get_single_user_or_none(response, username)

    def register_user_by_username(self, username):
        response = self.data_service.auth_provider_add_user(self.auth_provider_id, username)
        return response.json()

    def find_user_by_email(self, email_address):
        response = self.data_service.get_users(email=email_address)
        return self._get_single_user_or_none(response, email_address)

    def find_affiliate_by_email(self, email_address):
        response = self.data_service.get_auth_provider_affiliates(self.auth_provider_id, email=email_address)
        return self._get_single_user_or_none(response, email_address)

    def find_affiliate_by_username(self, username):
        response = self.data_service.get_auth_provider_affiliates(self.auth_provider_id, username=username)
        return self._get_single_user_or_none(response, username)

    def user_or_affiliate_exists_for_email(self, email_address):
        if self.find_user_by_email(email_address):
            self.logging_func("Found DukeDS user for email address {}.".format(email_address))
            return True
        if self.find_affiliate_by_email(email_address):
            self.logging_func("Found affiliate for email address {}.".format(email_address))
            return True
        potential_username = EmailUtil.try_get_username_from_email(email_address)
        if potential_username and self.find_affiliate_by_username(potential_username):
            self.logging_func("Found DukeDS user for username {}.".format(potential_username))
            return True
        self.logging_func("No valid DukeDS user or affiliate found for email address {}.".format(email_address))
        return False

    @staticmethod
    def _get_single_user_or_none(response, lookup_value):
        results = response.json()['results']
        if not results:
            return None
        if len(results) == 1:
            return results[0]
        raise ValueError("Found multiple users for {}.".format(lookup_value))

    def try_determine_username_from_email(self, email_address):
        """
        Tries to find a username based on an email address. First looks for an affiliate with that email address
        and returns the 'uid' for that affiliate. Otherwise it tries to extract the username from the email address.
        Returns None if no affiliate was found and the email address doesn't contain a username.
        :param email_address: str: email address to find a username for
        :return: str: username or None
        """
        affiliate = self.find_affiliate_by_email(email_address)
        if affiliate:
            return affiliate['uid']
        else:
            return EmailUtil.try_get_username_from_email(email_address)


class EmailUtil(object):
    @staticmethod
    def try_get_username_from_email(email_address):
        """
        If email_address ends in @duke.edu and the local part is a username return that username otherwise return None.
        Duke emails take two forms one with the full name and another where the local part is the username.
        Usernames must be only lowercase alphanumeric.
        :param email_address: str
        :return: bool: str or None
        """
        if EmailUtil.is_duke_email(email_address):
            local_part = EmailUtil.strip_email_suffix(email_address)
            if local_part.islower() and local_part.isalnum():
                return local_part
        return None

    @staticmethod
    def is_duke_email(email_address):
        return email_address.endswith(DUKE_EMAIL_SUFFIX)

    @staticmethod
    def strip_email_suffix(email_address):
        return re.sub('@.*$', '', email_address)
