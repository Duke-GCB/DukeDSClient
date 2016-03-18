import os
import requests
import json
import tempfile
import shutil
from passlib.hash import sha256_crypt
from ddsc.remotestore import RemoteContentDownloader
from ddsc.upload import ProjectUpload
from ddsc.localstore import LocalProject, LocalOnlyCounter
from ddsc.util import ProgressPrinter


class HandoverApi(object):
    MAIL_DRAFT_DESTINATION = '/drafts'
    HANDOVER_DESTINATION = '/handovers'

    def __init__(self, url):
        self.url = url

    def send(self, destination, from_user_id, to_user_id, project_id, project_name, user_key_signature):
        destination = self.url + destination
        headers = {
            'Content-Type': 'application/json',
        }
        data = {
            from_user_id: from_user_id,
            to_user_id: to_user_id,
            project_id: project_id,
            project_name: project_name,
            user_key_signature: user_key_signature
        }

        return requests.post(destination, headers=headers, data=json.dumps(data))


class Handover(object):
    """
    Handles operations dealing with sending draft and final handover for a project.
    """
    def __init__(self, config, remote_store):
        """
        Setup for pulling data from a remote store and updating the handover service.
        :param config: Config configuration of the app
        :param remote_store: RemoteStore remote store we will be sharing a project from
        :return:
        """
        self.config = config
        self.handover_api = HandoverApi(config.handover_url)
        self.remote_store = remote_store

    def mail_draft(self, project_name, to_user):
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.give_user_read_only_access(project_name, to_user)
        return self._share_project(HandoverApi.MAIL_DRAFT_DESTINATION, project, to_user)

    def fetch_remote_project(self, project_name, must_exist=False):
        return self.remote_store.fetch_remote_project(project_name, must_exist=must_exist)

    def give_user_read_only_access(self, project, user):
        self.remote_store.set_user_project_permission(project, user, 'project_viewer')

    def handover(self, project_name, new_project_name, to_user):
        project = self.fetch_remote_project(project_name, must_exist=True)
        self.remove_user_permission(project, to_user)
        if new_project_name:
            project = self._copy_project(project_name, new_project_name)
        return self._share_project(HandoverApi.HANDOVER_DESTINATION, project, to_user)

    def remove_user_permission(self, project, user):
        self.remote_store.revoke_user_project_permission(project, user)

    def _share_project(self, destination, project, to_user):
        from_user = self.remote_store.get_current_user()
        user_api_key = self.config.user_key
        user_key_signature = sha256_crypt.encrypt(user_api_key)
        response = self.handover_api.send(destination=destination,
                                          from_user_id=from_user.id,
                                          to_user_id=to_user.id,
                                          project_id=project.id,
                                          project_name=project.name,
                                          user_key_signature=user_key_signature)
        if response.status_code == 200:
            return to_user.email
        else:
            raise ValueError("Failed to send email status" + response.status_code)

    def _copy_project(self, project_name, new_project_name):
        temp_directory = tempfile.mkdtemp()
        remote_project = self.remote_store.fetch_remote_project(new_project_name)
        if remote_project:
            raise ValueError("A project with name '{}' already exists.".format(new_project_name))
        self._download_project(project_name, temp_directory)
        self._upload_project(new_project_name, temp_directory)
        shutil.rmtree(temp_directory)
        return self.remote_store.fetch_remote_project(new_project_name, must_exist=True)

    def _download_project(self, project_name, temp_directory):
        print("Downloading a copy of '{}'.".format(project_name))
        remote_project = self.remote_store.fetch_remote_project(project_name, must_exist=True)
        downloader = RemoteContentDownloader(self.remote_store, temp_directory)
        downloader.walk_project(remote_project)

    def _upload_project(self, project_name, temp_directory):
        print("Uploading to '{}'.".format(project_name))
        items_to_send = [os.path.join(temp_directory, item) for item in os.listdir(os.path.abspath(temp_directory))]
        upload_tool = ProjectUpload(self.config, project_name, items_to_send)
        upload_tool.upload()
