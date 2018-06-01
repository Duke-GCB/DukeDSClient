""" Handles sharing a project with another user either in share or deliver modes."""

import json
import os
import datetime
import pytz
import shutil
import tempfile
import requests
from ddsc.core.upload import ProjectUpload
from ddsc.core.download import ProjectDownload
from ddsc.core.ddsapi import DataServiceAuth
from ddsc.core.util import KindType
from ddsc.versioncheck import get_internal_version_str
from ddsc.core.remotestore import ProjectNameOrId

UNAUTHORIZED_MESSAGE = """
ERROR: Your account does not have authorization for D4S2 (the deliver/share service).
Please send an email to gcb-help@duke.edu titled 'D4S2 setup' so we can work with you to setup your account.

"""

SHARE_WITH_SELF_MESSAGE = """
ERROR: You cannot {} a project to yourself.

"""

USER_WITHOUT_EMAIL_MESSAGE = """
ERROR: The user you are trying to {} to has no email setup in DukeDS.
We are unable to contact them to {} your project.

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


class ShareWithSelfError(Exception):
    """
    Error raised whe user attempts to share/deliver a project just themselves
    """
    def __init__(self, message):
        """
        :param message: str reason for the error
        """
        Exception.__init__(self, message)


class UserMissingEmailError(Exception):
    """
    Raised when attempting to deliver or share with a DukeDS user that has a null email
    """
    def __init__(self, message):
        """
        :param message: str reason for the error
        """
        Exception.__init__(self, message)


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

    def __init__(self, url, api_token):
        """
        Setup url we will be talking to.
        :param url: str url of the service including "/api/v1" portion
        """
        self.url = url
        self.json_headers = {
            'Content-Type': 'application/json',
            'X-DukeDS-Authorization': api_token
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
        item_dict = {
            'project_id': item.project_id,
            'from_user_id': item.from_user_id,
            'to_user_id': item.to_user_id,
            'role': item.auth_role,
            'user_message': item.user_message
        }
        if item.share_user_ids:
            item_dict['share_user_ids'] = item.share_user_ids
        data = json.dumps(item_dict)
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
            raise D4S2Error("Request to {} failed with {}:\n{}.".format(response.url, response.status_code,
                                                                        response.text))


class D4S2Item(object):
    """
    Contains data for processing either share or deliver.
    """
    def __init__(self, destination, from_user_id, to_user_id, project_id, project_name, auth_role, user_message,
                 share_user_ids):
        """
        Save data for use with send method.
        :param destination: str type of message we are sending(SHARE_DESTINATION or DELIVER_DESTINATION)
        :param from_user_id: str uuid(duke-data-service) of the user who is sending the share/delivery
        :param to_user_id: str uuid(duke-data-service) of the user is receiving the share/delivery
        :param project_id: str uuid(duke-data-service) of project we are sharing
        :param project_name: str name of the project (sent for debugging purposes)
        :param auth_role: str authorization role to given to the user (determines which email to send)
        :param user_message: str user message to send with the share/delivery
        :param share_user_ids: [str] users to share the project with once ownership is transferred (only for delivery)
        """
        self.destination = destination
        self.from_user_id = from_user_id
        self.to_user_id = to_user_id
        self.project_id = project_id
        self.project_name = project_name
        self.auth_role = auth_role
        self.user_message = user_message
        self.share_user_ids = share_user_ids

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
        auth = DataServiceAuth(self.config)
        api_token = auth.get_auth()
        self.api = D4S2Api(config.d4s2_url, api_token)
        self.remote_store = remote_store
        self.print_func = print_func

    def share(self, project, to_user, force_send, auth_role, user_message):
        """
        Send mail and give user specified access to the project.
        :param project: RemoteProject project to share
        :param to_user: RemoteUser user to receive email/access
        :param auth_role: str project role eg 'project_admin' to give to the user
        :param user_message: str message to be sent with the share
        :return: str email we share the project with
        """
        if self._is_current_user(to_user):
            raise ShareWithSelfError(SHARE_WITH_SELF_MESSAGE.format("share"))
        if not to_user.email:
            self._raise_user_missing_email_exception("share")
        self.set_user_project_permission(project, to_user, auth_role)
        return self._share_project(D4S2Api.SHARE_DESTINATION, project, to_user, force_send, auth_role, user_message)

    def set_user_project_permission(self, project, user, auth_role):
        """
        Give user access permissions for a project.
        :param project: RemoteProject project to update permissions on
        :param user: RemoteUser user to receive permissions
        :param auth_role: str project role eg 'project_admin'
        """
        self.remote_store.set_user_project_permission(project, user, auth_role)

    def deliver(self, project, new_project_name, to_user, share_users, force_send, path_filter, user_message):
        """
        Remove access to project_name for to_user, copy to new_project_name if not None,
        send message to service to email user so they can have access.
        :param project: RemoteProject pre-existing project to be delivered
        :param new_project_name: str name of non-existing project to copy project_name to, if None we don't copy
        :param to_user: RemoteUser user we are handing over the project to
        :param share_users: [RemoteUser] who will have project shared with them once to_user accepts the project
        :param force_send: boolean enables resending of email for existing projects
        :param path_filter: PathFilter: filters what files are shared
        :param user_message: str message to be sent with the share
        :return: str email we sent deliver to
        """
        if self._is_current_user(to_user):
            raise ShareWithSelfError(SHARE_WITH_SELF_MESSAGE.format("deliver"))
        if not to_user.email:
            self._raise_user_missing_email_exception("deliver")
        self.remove_user_permission(project, to_user)
        if new_project_name:
            project = self._copy_project(project, new_project_name, path_filter)
        return self._share_project(D4S2Api.DELIVER_DESTINATION, project, to_user,
                                   force_send, user_message=user_message, share_users=share_users)

    def remove_user_permission(self, project, user):
        """
        Take away user's access to project.
        :param project: RemoteProject project to remove permissions on
        :param user: RemoteUser user who should no longer have access
        """
        self.remote_store.revoke_user_project_permission(project, user)

    def _share_project(self, destination, project, to_user, force_send, auth_role='', user_message='',
                       share_users=None):
        """
        Send message to remote service to email/share project with to_user.
        :param destination: str which type of sharing we are doing (SHARE_DESTINATION or DELIVER_DESTINATION)
        :param project: RemoteProject project we are sharing
        :param to_user: RemoteUser user we are sharing with
        :param auth_role: str project role eg 'project_admin' email is customized based on this setting.
        :param user_message: str message to be sent with the share
        :param share_users: [RemoteUser] users to have this project shared with after delivery (delivery only)
        :return: the email the user should receive a message on soon
        """
        from_user = self.remote_store.get_current_user()
        share_user_ids = None
        if share_users:
            share_user_ids = [share_user.id for share_user in share_users]
        item = D4S2Item(destination=destination,
                        from_user_id=from_user.id,
                        to_user_id=to_user.id,
                        project_id=project.id,
                        project_name=project.name,
                        auth_role=auth_role,
                        user_message=user_message,
                        share_user_ids=share_user_ids)
        item.send(self.api, force_send)
        return to_user.email

    def _copy_project(self, project, new_project_name, path_filter):
        """
        Copy pre-existing project with name project_name to non-existing project new_project_name.
        :param project: remotestore.RemoteProject project to copy from
        :param new_project_name: str project to copy to
        :param path_filter: PathFilter: filters what files are shared
        :return: RemoteProject new project we copied data to
        """
        temp_directory = tempfile.mkdtemp()
        new_project_name_or_id = ProjectNameOrId.create_from_name(new_project_name)
        remote_project = self.remote_store.fetch_remote_project(new_project_name_or_id)
        if remote_project:
            raise ValueError("A project with name '{}' already exists.".format(new_project_name))
        activity = CopyActivity(self.remote_store.data_service, project, new_project_name)
        self._download_project(activity, project, temp_directory, path_filter)
        self._upload_project(activity, new_project_name, temp_directory)
        activity.finished()
        shutil.rmtree(temp_directory)
        return self.remote_store.fetch_remote_project(new_project_name_or_id, must_exist=True)

    def _download_project(self, activity, project, temp_directory, path_filter):
        """
        Download the project with project_name to temp_directory.
        :param activity: CopyActivity: info about the copy activity are downloading for
        :param project: remotestore.RemoteProject project to download
        :param temp_directory: str path to directory we can download into
        :param path_filter: PathFilter: filters what files are shared
        """
        self.print_func("Downloading a copy of '{}'.".format(project.name))
        project_download = ProjectDownload(self.remote_store, project, temp_directory, path_filter,
                                           file_download_pre_processor=DownloadedFileRelations(activity))
        project_download.run()

    def _upload_project(self, activity, project_name, temp_directory):
        """
        Upload the contents of temp_directory into project_name
        :param activity: CopyActivity: info about the copy activity are uploading for
        :param project_name: str project name we will upload files to
        :param temp_directory: str path to directory who's files we will upload
        """
        self.print_func("Uploading to '{}'.".format(project_name))
        items_to_send = [os.path.join(temp_directory, item) for item in os.listdir(os.path.abspath(temp_directory))]
        project_name_or_id = ProjectNameOrId.create_from_name(project_name)
        project_upload = ProjectUpload(self.config, project_name_or_id, items_to_send,
                                       file_upload_post_processor=UploadedFileRelations(activity))
        project_upload.run()

    def _is_current_user(self, some_user):
        """
        Is the specified user the current user?
        :param some_user: RemoteUser user we want to check against the current user
        :return: boolean: True if the current user is the passed in user
        """
        current_user = self.remote_store.get_current_user()
        return current_user.id == some_user.id

    @staticmethod
    def _raise_user_missing_email_exception(cmd):
        msg = USER_WITHOUT_EMAIL_MESSAGE.format(cmd, cmd)
        raise UserMissingEmailError(msg)


class CopyActivity(object):
    def __init__(self, data_service, project, new_project_name):
        """
        Create an activity for our copy operation so users can trace back where the copied files came from.
        :param data_service: DataServiceApi: service used to create the activity
        :param project: RemoteProject project name we will download files from
        :param new_project_name: str project name we will upload files into
        """
        self.data_service = data_service
        self.name = "DukeDSClient copying project: {}".format(project.name)
        self.desc = "Copying {} to project {} using DukeDSClient{}".format(project.name, new_project_name,
                                                                           get_internal_version_str())
        self.started = self._current_timestamp_str()
        result = data_service.create_activity(self.name, self.desc, started_on=self.started)
        self.id = result.json()['id']
        self.remote_path_to_file_version_id = {}

    def finished(self):
        """
        Mark the activity as finished
        """
        self.data_service.update_activity(self.id, self.name, self.desc,
                                          started_on=self.started,
                                          ended_on=self._current_timestamp_str())

    @staticmethod
    def _current_timestamp_str():
        return datetime.datetime.now(pytz.utc).isoformat()


class DownloadedFileRelations(object):
    """
    Contains run method that will be called via project download file pre-processor.
    """
    def __init__(self, activity):
        """
        :param activity: CopyActivity: info about the activity associated with the files we are downloading
        """
        self.activity = activity

    def run(self, data_service, project_file):
        """
        Attach a remote file to activity with used relationship.
        :param data_service: DataServiceApi: service used to attach relationship
        :param project_file: ProjectFile: contains details about a file we will attach
        """
        remote_path = project_file.path
        file_dict = data_service.get_file(project_file.id).json()
        file_version_id = file_dict['current_version']['id']
        data_service.create_used_relation(self.activity.id, KindType.file_str, file_version_id)
        self.activity.remote_path_to_file_version_id[remote_path] = file_version_id


class UploadedFileRelations(object):
    """
    Contains run method that will be called via project upload file post-processor.
    """
    def __init__(self, activity):
        """
        :param activity: CopyActivity: info about the activity associated with the files we are uploading
        """
        self.activity = activity

    def run(self, data_service, file_details):
        """
        Attach a remote file to activity with was generated by relationship.
        :param data_service: DataServiceApi: service used to attach relationship
        :param file_details: dict: response from DukeDS POST to /files/ containing current_version id
        """
        file_version_id = file_details['current_version']['id']
        data_service.create_was_generated_by_relation(self.activity.id, KindType.file_str, file_version_id)
        used_entity_id = self._lookup_used_entity_id(file_details)
        data_service.create_was_derived_from_relation(used_entity_id, KindType.file_str,
                                                      file_version_id, KindType.file_str)

    def _lookup_used_entity_id(self, file_details):
        """
        Return the file_version_id associated with the path from file_details.
        The file_version_id is looked up from a dictionary in the activity.
        :param file_details: dict: response from DukeDS POST to /files/
        :return: str: file_version_id uuid
        """
        # Since this uses the response from POST to /files/ this will include the ancestors and not be
        # effected by exclude_response_fields that were used when listing the project
        name_parts = [ancestor['name'] for ancestor in file_details['ancestors']
                      if ancestor['kind'] == KindType.folder_str]
        name_parts.append(file_details['name'])
        remote_path = os.sep.join(name_parts)
        return self.activity.remote_path_to_file_version_id[remote_path]
