""" Handles sharing a project with another user either in draft or handover modes."""

import json
import os
import shutil
import tempfile

import requests
from ddsc.core.upload import ProjectUpload
from passlib.hash import sha256_crypt

from ddsc.core.download import ProjectDownload

DRAFT_USER_ACCESS_ROLE = 'project_viewer'


class HandoverApi(object):
    """
    API for sending messages to a service that will email the user we are sharing with.
    Service also gives user permission to access the project for handover mode.
    """
    MAIL_DRAFT_DESTINATION = '/drafts/'
    HANDOVER_DESTINATION = '/handovers/'

    def __init__(self, url):
        """
        Setup url we will be talking to.
        :param url: str url of the service including "/api/v1" portion
        """
        self.url = url

    def send(self, destination, from_user_id, to_user_id, project_id, project_name, user_key_signature):
        """
        Sends a message to the service requesting an email for sharing the project.
        :param destination: str url suffix we want to talk to (MAIL_DRAFT_DESTINATION or HANDOVER_DESTINATION)
        :param from_user_id: str uuid for the current user
        :param to_user_id: str uuid for the user we are sharing the project with
        :param project_id: str uuid of the project we want to share
        :param project_name: str name of the project(for debugging purposes only)
        :param user_key_signature: str hash of our private key to make API secure
        :return: requests.Response this will be returned no matter the result
        """
        destination = self.url + destination
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            'from_user_id': from_user_id,
            'to_user_id': to_user_id,
            'project_id': project_id,
            'project_name': project_name,
            'user_key_signature': user_key_signature
        }

        return requests.post(destination, headers=headers, data=json.dumps(data))


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
        self.handover_api = HandoverApi(config.handover_url)
        self.remote_store = remote_store
        self.print_func = print_func

    def mail_draft(self, project_name, to_user):
        """
        Send mail draft and give user read only access to the project.
        :param project_name: str name of the project to share
        :param to_user: RemoteUser user to receive email/access
        :return: str email we sent the draft to
        """
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.give_user_read_only_access(project, to_user)
        return self._share_project(HandoverApi.MAIL_DRAFT_DESTINATION, project, to_user)

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

    def handover(self, project_name, new_project_name, to_user):
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
        return self._share_project(HandoverApi.HANDOVER_DESTINATION, project, to_user)

    def remove_user_permission(self, project, user):
        """
        Take away user's access to project.
        :param project: RemoteProject project to remove permissions on
        :param user: RemoteUser user who should no longer have access
        """
        self.remote_store.revoke_user_project_permission(project, user)

    def _share_project(self, destination, project, to_user):
        """
        Send message to remove service to email/share project with to_user.
        :param destination: str which type of sharing we are doing (MAIL_DRAFT_DESTINATION or HANDOVER_DESTINATION)
        :param project: RemoteProject project we are sharing
        :param to_user: RemoteUser user we are sharing with
        :return: the email the user should receive a message on soon
        """
        from_user = self.remote_store.get_current_user()
        user_api_key = self.config.user_key
        user_key_signature = sha256_crypt.encrypt(user_api_key)
        response = self.handover_api.send(destination=destination,
                                          from_user_id=from_user.id,
                                          to_user_id=to_user.id,
                                          project_id=project.id,
                                          project_name=project.name,
                                          user_key_signature=user_key_signature)
        if 200 <= response.status_code < 300:
            return to_user.email
        else:
            msg = response.json().get('message','')
            raise ValueError("Failed to send email. Status {} : {}".format(response.status_code, msg))

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
