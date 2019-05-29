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
        self.logging_func("No valid DukeDS user or affiliate found for email address {}.".format(email_address))
        return False

    def user_or_affiliate_exists_for_username(self, username):
        if self.find_user_by_username(username):
            self.logging_func("Found DukeDS user for username {}.".format(username))
            return True
        if self.find_affiliate_by_username(username):
            self.logging_func("Found affiliate for username {}.".format(username))
            return True
        self.logging_func("No valid DukeDS user or affiliate found for username {}.".format(username))
        return False

    @staticmethod
    def _get_single_user_or_none(response, lookup_value):
        results = response.json()['results']
        if not results:
            return None
        if len(results) == 1:
            return results[0]
        raise ValueError("Found multiple users for {}.".format(lookup_value))
