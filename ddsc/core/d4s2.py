""" Handles sharing a project with another user either in share or deliver modes."""

import json
import os
import shutil
import tempfile
import requests
from ddsc.core.upload import ProjectUpload
from ddsc.core.download import ProjectDownload

UNAUTHORIZED_MESSAGE = """
ERROR: Your account does not have authorization for D4S2 (the deliver/share service).
Please send an email to gcb-help@duke.edu titled 'D4S2 setup' so we can work with you to setup your account.

"""


class D4S2Error(Exception):
    def __init__(self, message, warning=False):
        """
        Setup error.
        :param message: str reason for the error
        :param warning: boolean is this just a warning
        """
        Exception.__init__(self, message)
        self.message = message
        self.warning = warning


class D4S2Api(object):
    """
    API for sending messages to a service that will email the user we are sharing with.
    Service also gives user permission to access the project for deliver mode.
    """
    SHARE_DESTINATION = '/shares/'
    DELIVER_DESTINATION = '/deliveries/'
    DEST_TO_NAME = {
        SHARE_DESTINATION: "Share",
        DELIVER_DESTINATION: "Delivery"
    }

    def __init__(self, url, user_key):
        """
        Setup url we will be talking to.
        :param url: str url of the service including "/api/v1" portion
        """
        self.url = url
        self.json_headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token {}'.format(user_key)
        }

    def make_url(self, destination, extra=''):
        """
        Build url based on destionation with optional suffix(extra).
        :param destination: str base suffix(with slashes)
        :param extra: str optional suffix
        """
        return '{}{}{}'.format(self.url, destination, extra)

    def get_existing_item(self, item):
        """
        Lookup item in remote service based on keys.
        :param item: D4S2Item data contains keys we will use for lookup.
        :return: requests.Response containing the successful result
        """
        params = {
            'project_id': item.project_id,
            'from_user_id': item.from_user_id,
            'to_user_id': item.to_user_id,
        }
        resp = requests.get(self.make_url(item.destination), headers=self.json_headers, params=params)
        self.check_response(resp)
        return resp

    def create_item(self, item):
        """
        Create a new item in D4S2 service for item at the specified destination.
        :param item: D4S2Item data to use for creating a D4S2 item
        :return: requests.Response containing the successful result
        """
        data = json.dumps({
            'project_id': item.project_id,
            'from_user_id': item.from_user_id,
            'to_user_id': item.to_user_id,
            'role': item.auth_role,
        })
        resp = requests.post(self.make_url(item.destination), headers=self.json_headers, data=data)
        self.check_response(resp)
        return resp

    def send_item(self, destination, item_id, force_send):
        """
        Run send method for item_id at destination.
        :param destination: str which type of operation are we doing (SHARE_DESTINATION or DELIVER_DESTINATION)
        :param item_id: str D4S2 service id representing the item we want to send
        :param force_send: bool it's ok to email the item again
        :return: requests.Response containing the successful result
        """
        data = json.dumps({
            'force': force_send,
        })
        url_suffix = "{}/send/".format(item_id)
        resp = requests.post(self.make_url(destination, url_suffix), headers=self.json_headers, data=data)
        self.check_response(resp)
        return resp

    def check_response(self, response):
        """
        Raises error if the response isn't successful.
        :param response: requests.Response response to be checked
        """
        if response.status_code == 401:
            raise D4S2Error(UNAUTHORIZED_MESSAGE)
        if not 200 <= response.status_code < 300:
            raise D4S2Error("Request to {} failed with {}.".format(response.url, response.status_code))


class D4S2Item(object):
    """
    Contains data for processing either share or deliver.
    """
    def __init__(self, destination, from_user_id, to_user_id, project_id, project_name, auth_role):
        """
        Save data for use with send method.
        :param destination: str type of message we are sending(SHARE_DESTINATION or DELIVER_DESTINATION)
        :param from_user_id: str uuid(duke-data-service) of the user who is sending the share/delivery
        :param to_user_id: str uuid(duke-data-service) of the user is receiving the share/delivery
        :param project_id: str uuid(duke-data-service) of project we are sharing
        :param project_name: str name of the project (sent for debugging purposes)
        :param auth_role: str authorization role to given to the user (determines which email to send)
        """
        self.destination = destination
        self.from_user_id = from_user_id
        self.to_user_id = to_user_id
        self.project_id = project_id
        self.project_name = project_name
        self.auth_role = auth_role

    def send(self, api, force_send):
        """
        Send this item using api.
        :param api: D4S2Api sends messages to D4S2
        :param force_send: bool should we send even if the item already exists
        """
        item_id = self.get_existing_item_id(api)
        if not item_id:
            item_id = self.create_item_returning_id(api)
            api.send_item(self.destination, item_id, force_send)
        else:
            if force_send:
                api.send_item(self.destination, item_id, force_send)
            else:
                item_type = D4S2Api.DEST_TO_NAME.get(self.destination, "Item")
                msg = "{} already sent. Run with --resend argument to resend."
                raise D4S2Error(msg.format(item_type), warning=True)

    def get_existing_item_id(self, api):
        """
        Lookup the id for this item via the D4S2 service.
        :param api: D4S2Api object who communicates with D4S2 server.
        :return str id of this item or None if not found
        """
        resp = api.get_existing_item(self)
        items = resp.json()
        num_items = len(items)
        if num_items == 0:
            return None
        else:
            return items[0]['id']

    def create_item_returning_id(self, api):
        """
        Create this item in the D4S2 service.
        :param api: D4S2Api object who communicates with D4S2 server.
        :return str newly created id for this item
        """
        resp = api.create_item(self)
        item = resp.json()
        return item['id']


class D4S2Project(object):
    """
    Transfers a project to another user via share and final delivery.
    Uses eternal API to send messages.
    """
    def __init__(self, config, remote_store, print_func):
        """
        Setup for sharing a project and sending email on the D4S2 service.
        :param config: Config configuration specifying which remote_store to use.
        :param remote_store: RemoteStore remote store we will be sharing a project from
        :param print_func: func used to print output somewhere
        """
        self.config = config
        self.api = D4S2Api(config.d4s2_url, config.user_key)
        self.remote_store = remote_store
        self.print_func = print_func

    def share(self, project_name, to_user, force_send, auth_role):
        """
        Send mail and give user specified access to the project.
        :param project_name: str name of the project to share
        :param to_user: RemoteUser user to receive email/access
        :param auth_role: str project role eg 'project_admin' to give to the user
        :return: str email we share the project with
        """
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.set_user_project_permission(project, to_user, auth_role)
        return self._share_project(D4S2Api.SHARE_DESTINATION, project, to_user, force_send, auth_role)

    def fetch_remote_project(self, project_name, must_exist=False):
        """
        Download project metadata from a remote store.
        :param project_name: str project to download
        :param must_exist: bool is it ok if the project doesn't exist
        :return: RemoteProject
        """
        return self.remote_store.fetch_remote_project(project_name, must_exist=must_exist)

    def set_user_project_permission(self, project, user, auth_role):
        """
        Give user access permissions for a project.
        :param project: RemoteProject project to update permissions on
        :param user: RemoteUser user to receive permissions
        :param auth_role: str project role eg 'project_admin'
        """
        self.remote_store.set_user_project_permission(project, user, auth_role)

    def deliver(self, project_name, new_project_name, to_user, force_send, path_filter):
        """
        Remove access to project_name for to_user, copy to new_project_name if not None,
        send message to service to email user so they can have access.
        :param project_name: str name of the pre-existing project
        :param new_project_name: str name of non-existing project to copy project_name to, if None we don't copy
        :param to_user: RemoteUser user we are handing over the project to
        :param force_send: boolean enables resending of email for existing projects
        :param path_filter: PathFilter: filters what files are shared
        :return: str email we sent deliver to
        """
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.remove_user_permission(project, to_user)
        if new_project_name:
            project = self._copy_project(project_name, new_project_name, path_filter)
        return self._share_project(D4S2Api.DELIVER_DESTINATION, project, to_user, force_send)

    def remove_user_permission(self, project, user):
        """
        Take away user's access to project.
        :param project: RemoteProject project to remove permissions on
        :param user: RemoteUser user who should no longer have access
        """
        self.remote_store.revoke_user_project_permission(project, user)

    def _share_project(self, destination, project, to_user, force_send, auth_role=''):
        """
        Send message to remove service to email/share project with to_user.
        :param destination: str which type of sharing we are doing (SHARE_DESTINATION or DELIVER_DESTINATION)
        :param project: RemoteProject project we are sharing
        :param to_user: RemoteUser user we are sharing with
        :param auth_role: str project role eg 'project_admin' email is customized based on this setting.
        :return: the email the user should receive a message on soon
        """
        from_user = self.remote_store.get_current_user()
        item = D4S2Item(destination=destination,
                        from_user_id=from_user.id,
                        to_user_id=to_user.id,
                        project_id=project.id,
                        project_name=project.name,
                        auth_role=auth_role)
        sent = item.send(self.api, force_send)
        return to_user.email

    def _copy_project(self, project_name, new_project_name, path_filter):
        """
        Copy pre-existing project with name project_name to non-existing project new_project_name.
        :param project_name: str project to copy from
        :param new_project_name: str project to copy to
        :param path_filter: PathFilter: filters what files are shared
        :return: RemoteProject new project we copied data to
        """
        temp_directory = tempfile.mkdtemp()
        remote_project = self.remote_store.fetch_remote_project(new_project_name)
        if remote_project:
            raise ValueError("A project with name '{}' already exists.".format(new_project_name))
        self._download_project(project_name, temp_directory, path_filter)
        self._upload_project(new_project_name, temp_directory)
        shutil.rmtree(temp_directory)
        return self.remote_store.fetch_remote_project(new_project_name, must_exist=True)

    def _download_project(self, project_name, temp_directory, path_filter):
        """
        Download the project with project_name to temp_directory.
        :param project_name: str name of the pre-existing project
        :param temp_directory: str path to directory we can download into
        :param path_filter: PathFilter: filters what files are shared
        """
        self.print_func("Downloading a copy of '{}'.".format(project_name))
        downloader = ProjectDownload(self.remote_store, project_name, temp_directory, path_filter)
        downloader.run()

    def _upload_project(self, project_name, temp_directory):
        """
        Upload the contents of temp_directory into project_name
        :param project_name: str project name we will upload files to
        :param temp_directory: str path to directory who's files we will upload
        """
        self.print_func("Uploading to '{}'.".format(project_name))
        items_to_send = [os.path.join(temp_directory, item) for item in os.listdir(os.path.abspath(temp_directory))]
        project_upload = ProjectUpload(self.config, project_name, items_to_send)
        project_upload.run()
