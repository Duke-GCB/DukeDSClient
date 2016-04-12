""" Handles sharing a project with another user either in draft or handover modes."""

import json
import os
import shutil
import tempfile
import requests
from ddsc.core.upload import ProjectUpload
from ddsc.core.download import ProjectDownload

DRAFT_USER_ACCESS_ROLE = 'project_viewer'


class HandoverError(Exception):
    def __init__(self, message, warning=False):
        """
        Setup error.
        :param message: str reason for the error
        :param warning: boolean is this just a warning
        """
        Exception.__init__(self, message)
        self.message = message
        self.warning = warning


class HandoverApi(object):
    """
    API for sending messages to a service that will email the user we are sharing with.
    Service also gives user permission to access the project for handover mode.
    """
    MAIL_DRAFT_DESTINATION = '/drafts/'
    HANDOVER_DESTINATION = '/handovers/'
    DEST_TO_NAME = {
        MAIL_DRAFT_DESTINATION: "Mail draft",
        HANDOVER_DESTINATION: "Handover"
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

    def get_existing_item(self, handover_item):
        """
        Lookup handover_item in remote service based on keys.
        :param handover_item: HandoverItem data contains keys we will use for lookup.
        :return: requests.Response containing the successful result
        """
        params = {
            'project_id': handover_item.project_id,
            'from_user_id': handover_item.from_user_id,
            'to_user_id': handover_item.to_user_id,
        }
        resp = requests.get(self.make_url(handover_item.destination), headers=self.json_headers, params=params)
        self.check_response(resp)
        return resp

    def create_item(self, handover_item):
        """
        Create a new item in handover service for handover_item at the specified destination.
        :param handover_item: HandoverItem data to use for creating a handover item
        :return: requests.Response containing the successful result
        """
        data = json.dumps({
            'project_id': handover_item.project_id,
            'from_user_id': handover_item.from_user_id,
            'to_user_id': handover_item.to_user_id,

        })
        resp = requests.post(self.make_url(handover_item.destination), headers=self.json_headers, data=data)
        self.check_response(resp)
        return resp

    def send_item(self, destination, item_id, force_send):
        """
        Run send method for item_id at destination.
        :param destination: str which type of handover are we doing (MAIL_DRAFT_DESTINATION or HANDOVER_DESTINATION)
        :param item_id: str handover service id representing the item we want to send
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
        if not 200 <= response.status_code < 300:
            raise HandoverError("Request to {} failed with {}.".format(response.url, response.status_code))


class HandoverItem(object):
    """
    Contains data for processing either a mail draft or handover.
    """
    def __init__(self, destination, from_user_id, to_user_id, project_id, project_name):
        """
        Save data for use with send method.
        :param destination: str type of message we are sending(MAIL_DRAFT_DESTINATION or HANDOVER_DESTINATION)
        :param from_user_id: str uuid(duke-data-service) of the user who is sending the handover
        :param to_user_id: str uuid(duke-data-service) of the user is receiving the email/handover
        :param project_id: str uuid(duke-data-service) of project we are sharing
        :param project_name: str name of the project (sent for debugging purposes)
        """
        self.destination = destination
        self.from_user_id = from_user_id
        self.to_user_id = to_user_id
        self.project_id = project_id
        self.project_name = project_name

    def send(self, handover_api, force_send):
        """
        Send this item using handover_api.
        :param handover_api: HandoverApi sends messages to DukeDSHandoverService
        :param force_send: bool should we send even if the item already exists
        """
        item_id = self.get_existing_item_id(handover_api)
        if not item_id:
            item_id = self.create_item_returning_id(handover_api)
            handover_api.send_item(self.destination, item_id, force_send)
        else:
            if force_send:
                handover_api.send_item(self.destination, item_id, force_send)
            else:
                item_type = HandoverApi.DEST_TO_NAME.get(self.destination, "Item")
                msg = "{} already sent. Run with --resend argument to resend."
                raise HandoverError(msg.format(item_type), warning=True)

    def get_existing_item_id(self, handover_api):
        """
        Lookup the id for this item via the handover service.
        :param handover_api: HandoverApi object who communicates with handover server.
        :return str id of this item or None if not found
        """
        resp = handover_api.get_existing_item(self)
        items = resp.json()
        num_items = len(items)
        if num_items == 0:
            return None
        else:
            return items[0]['id']

    def create_item_returning_id(self, handover_api):
        """
        Create this item in the handover service.
        :param handover_api: HandoverApi object who communicates with handover server.
        :return str newly created id for this item
        """
        resp = handover_api.create_item(self)
        item = resp.json()
        return item['id']


class ProjectHandover(object):
    """
    Transfers a project to another user via draft and final handover.
    Uses eternal API to send messages.
    """
    def __init__(self, config, remote_store, print_func):
        """
        Setup for sharing a project and sending email on the handover service.
        :param config: Config configuration specifying which remote_store to use.
        :param remote_store: RemoteStore remote store we will be sharing a project from
        :param print_func: func used to print output somewhere
        """
        self.config = config
        self.handover_api = HandoverApi(config.handover_url, config.user_key)
        self.remote_store = remote_store
        self.print_func = print_func

    def mail_draft(self, project_name, to_user, force_send):
        """
        Send mail draft and give user read only access to the project.
        :param project_name: str name of the project to share
        :param to_user: RemoteUser user to receive email/access
        :return: str email we sent the draft to
        """
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.give_user_read_only_access(project, to_user)
        return self._share_project(HandoverApi.MAIL_DRAFT_DESTINATION, project, to_user, force_send)

    def fetch_remote_project(self, project_name, must_exist=False):
        """
        Download project metadata from a remote store.
        :param project_name: str project to download
        :param must_exist: bool is it ok if the project doesn't exist
        :return: RemoteProject
        """
        return self.remote_store.fetch_remote_project(project_name, must_exist=must_exist)

    def give_user_read_only_access(self, project, user):
        """
        Give user read only access to project.
        :param project: RemoteProject project to update permissions on
        :param user: RemoteUser user to receive permissions
        """
        self.remote_store.set_user_project_permission(project, user, DRAFT_USER_ACCESS_ROLE)

    def handover(self, project_name, new_project_name, to_user, force_send):
        """
        Remove access to project_name for to_user, copy to new_project_name if not None,
        send message to service to email user so they can have access.
        :param project_name: str name of the pre-existing project
        :param new_project_name: str name of non-existing project to copy project_name to, if None we don't copy
        :param to_user: RemoteUser user we are handing over the project to
        :return: str email we sent handover to
        """
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.remove_user_permission(project, to_user)
        if new_project_name:
            project = self._copy_project(project_name, new_project_name)
        return self._share_project(HandoverApi.HANDOVER_DESTINATION, project, to_user, force_send)

    def remove_user_permission(self, project, user):
        """
        Take away user's access to project.
        :param project: RemoteProject project to remove permissions on
        :param user: RemoteUser user who should no longer have access
        """
        self.remote_store.revoke_user_project_permission(project, user)

    def _share_project(self, destination, project, to_user, force_send):
        """
        Send message to remove service to email/share project with to_user.
        :param destination: str which type of sharing we are doing (MAIL_DRAFT_DESTINATION or HANDOVER_DESTINATION)
        :param project: RemoteProject project we are sharing
        :param to_user: RemoteUser user we are sharing with
        :return: the email the user should receive a message on soon
        """
        from_user = self.remote_store.get_current_user()
        handover_item = HandoverItem(destination=destination,
                                     from_user_id=from_user.id,
                                     to_user_id=to_user.id,
                                     project_id=project.id,
                                     project_name=project.name)
        sent = handover_item.send(self.handover_api, force_send)
        return to_user.email

    def _copy_project(self, project_name, new_project_name):
        """
        Copy pre-existing project with name project_name to non-existing project new_project_name.
        :param project_name: str project to copy from
        :param new_project_name: str project to copy to
        :return: RemoteProject new project we copied data to
        """
        temp_directory = tempfile.mkdtemp()
        remote_project = self.remote_store.fetch_remote_project(new_project_name)
        if remote_project:
            raise ValueError("A project with name '{}' already exists.".format(new_project_name))
        self._download_project(project_name, temp_directory)
        self._upload_project(new_project_name, temp_directory)
        shutil.rmtree(temp_directory)
        return self.remote_store.fetch_remote_project(new_project_name, must_exist=True)

    def _download_project(self, project_name, temp_directory):
        """
        Download the project with project_name to temp_directory.
        :param project_name: str name of the pre-existing project
        :param temp_directory: str path to directory we can download into
        """
        self.print_func("Downloading a copy of '{}'.".format(project_name))
        downloader = ProjectDownload(self.remote_store, project_name, temp_directory)
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
