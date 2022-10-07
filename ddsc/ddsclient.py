""" Runs the appropriate command for a user based on arguments. """
from __future__ import print_function, unicode_literals
import sys
import datetime
import time
from ddsc.core.d4s2 import D4S2Project, D4S2Error
from ddsc.core.remotestore import RemoteStore, RemoteAuthRole, ProjectNameOrId
from ddsc.core.localstore import LocalProject
from ddsc.core.upload import ProjectUpload
from ddsc.core.projectuploader import ProjectUploadDryRun
from ddsc.core.consistency import ProjectChecker, DSHashMismatchError
from ddsc.cmdparser import CommandParser, format_destination_path, replace_invalid_path_chars
from ddsc.core.util import ProjectDetailsList, verify_terminal_encoding, boolean_input_prompt, \
    read_argument_file_contents
from ddsc.core.pathfilter import PathFilter
from ddsc.versioncheck import check_version, VersionException, get_internal_version_str
from ddsc.config import create_config
from ddsc.core.download import ProjectFileDownloader
from ddsc.exceptions import DDSUserException
from ddsc.sdk.client import Client
from ddsc.azure.commands import AzureListCommand, AzureUploadCommand, AzureAddUserCommand, AzureRemoveUserCommand, \
    AzureDownloadCommand, AzureShareCommand, AzureDeliverCommand, AzureDeleteCommand, AzureListAuthRolesCommand, \
    AzureMoveCommand, AzureInfoCommand, AzureCheckCommand


NO_PROJECTS_FOUND_MESSAGE = 'No projects found.'
INVALID_DELIVERY_RECIPIENT_MSG = 'Delivery recipient cannot be a share user. Remove recipient from --share-users and try again.'
TWO_SECONDS = 2
AZURE_BACKING_STORAGE = "azure"
LEGACY_BACKING_STORAGE = "dds"


class DDSClient(object):
    """
    Runs various commands based on arguments.
    """
    def __init__(self, backing_storage=LEGACY_BACKING_STORAGE):
        self.show_error_stack_trace = False
        self.config = None
        self.backing_storage = backing_storage

    def run_command(self, args):
        """
        Create a parser and have it parse the args then run the appropriate command.
        :param args: [str] command line args
        """
        self.config = create_config()
        parser = self._create_parser()
        parser.run_command(args)

    def _create_parser(self):
        """
        Create a parser hooking up the command methods below to be run when chosen.
        :return: CommandParser parser with commands attached.
        """
        parser = CommandParser(get_internal_version_str(), self.use_azure_commands())
        parser.register_list_command(self.list_command)
        parser.register_upload_command(self.upload_command)
        parser.register_add_user_command(self.add_user_command)
        parser.register_remove_user_command(self.remove_user_command)
        parser.register_download_command(self.download_command)
        parser.register_share_command(self.share_command)
        parser.register_deliver_command(self.deliver_command)
        parser.register_delete_command(self.delete_command)
        parser.register_list_auth_roles_command(self.list_auth_roles_command)
        parser.register_move_command(self.move_command)
        parser.register_info_command(self.info_command)
        parser.register_check_command(self.check_command)
        return parser

    def use_azure_commands(self):
        return self.backing_storage == AZURE_BACKING_STORAGE

    def list_command(self, args):
        command_constructor = ListCommand
        if self.use_azure_commands():
            command_constructor = AzureListCommand
        return self._run_command(command_constructor, args)

    def upload_command(self, args):
        command_constructor = UploadCommand
        if self.use_azure_commands():
            command_constructor = AzureUploadCommand
        return self._run_command(command_constructor, args)

    def add_user_command(self, args):
        command_constructor = AddUserCommand
        if self.use_azure_commands():
            command_constructor = AzureAddUserCommand
        return self._run_command(command_constructor, args)

    def remove_user_command(self, args):
        command_constructor = RemoveUserCommand
        if self.use_azure_commands():
            command_constructor = AzureRemoveUserCommand
        return self._run_command(command_constructor, args)

    def download_command(self, args):
        command_constructor = DownloadCommand
        if self.use_azure_commands():
            command_constructor = AzureDownloadCommand
        return self._run_command(command_constructor, args)

    def share_command(self, args):
        command_constructor = ShareCommand
        if self.use_azure_commands():
            command_constructor = AzureShareCommand
        return self._run_command(command_constructor, args)

    def deliver_command(self, args):
        command_constructor = DeliverCommand
        if self.use_azure_commands():
            command_constructor = AzureDeliverCommand
        return self._run_command(command_constructor, args)

    def delete_command(self, args):
        command_constructor = DeleteCommand
        if self.use_azure_commands():
            command_constructor = AzureDeleteCommand
        return self._run_command(command_constructor, args)

    def list_auth_roles_command(self, args):
        command_constructor = ListAuthRolesCommand
        if self.use_azure_commands():
            command_constructor = AzureListAuthRolesCommand
        return self._run_command(command_constructor, args)

    def move_command(self, args):
        command_constructor = MoveCommand
        if self.use_azure_commands():
            command_constructor = AzureMoveCommand
        return self._run_command(command_constructor, args)

    def info_command(self, args):
        command_constructor = InfoCommand
        if self.use_azure_commands():
            command_constructor = AzureInfoCommand
        return self._run_command(command_constructor, args)

    def check_command(self, args):
        command_constructor = CheckCommand
        if self.use_azure_commands():
            command_constructor = AzureCheckCommand
        return self._run_command(command_constructor, args)

    def _check_pypi_version(self):
        """
        When the version is out of date or we have trouble retrieving it print a error to stderr and pause.
        """
        try:
            check_version()
        except VersionException as err:
            print(str(err), file=sys.stderr)
            time.sleep(TWO_SECONDS)

    def _create_config(self, args):
        return create_config(allow_insecure_config_file=args.allow_insecure_config_file)

    def _run_command(self, command_constructor, args):
        """
        Run command_constructor and call run(args) on the resulting object
        :param command_constructor: class of an object that implements run(args)
        :param args: object arguments for specific command created by CommandParser
        """
        verify_terminal_encoding(sys.stdout.encoding)
        self._check_pypi_version()
        config = self._create_config(args)
        self.show_error_stack_trace = config.debug_mode
        command = command_constructor(config)
        command.run(args)
        command.cleanup()


class BaseCommand(object):
    """
    Setup remote store and save config
    """
    def __init__(self, config):
        """
        Pass in the config containing remote_store/url so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.config = config

    @staticmethod
    def create_project_name_or_id_from_args(args):
        if args.project_name:
            return ProjectNameOrId.create_from_name(args.project_name)
        else:
            return ProjectNameOrId.create_from_project_id(args.project_id)

    def fetch_project(self, args, must_exist=True, include_children=False):
        project_name_or_id = self.create_project_name_or_id_from_args(args)
        if include_children:
            print("Fetching list of files for project {}.".format(project_name_or_id.value))
        project = self.remote_store.fetch_remote_project(project_name_or_id,
                                                         must_exist=must_exist,
                                                         include_children=include_children)
        if include_children:
            print("Done fetching list of files.")
        return project

    def make_user_list(self, emails, usernames):
        """
        Given a list of emails and usernames fetch DukeDS user info.
        Parameters that are None will be skipped.
        :param emails: [str]: list of emails (can be null)
        :param usernames:  [str]: list of usernames(netid)
        :return: [RemoteUser]: details about any users referenced the two parameters
        """
        to_users = []
        if emails:
            for email in emails:
                user = self.remote_store.get_or_register_user_by_email(email)
                to_users.append(user)
        if usernames:
            for username in usernames:
                user = self.remote_store.get_or_register_user_by_username(username)
                to_users.append(user)
        return to_users

    def cleanup(self):
        self.remote_store.close()


class ClientCommand(object):
    def __init__(self, config):
        self.client = Client(config)
        self.config = config

    def get_project_by_name_or_id(self, args):
        if args.project_name:
            return self.client.get_project_by_name(args.project_name)
        else:
            return self.client.get_project_by_id(args.project_id)

    def cleanup(self):
        self.client.close()


class UploadCommand(BaseCommand):
    """
    Uploads a folder to a remote project.
    """
    def __init__(self, config):
        """
        Pass in the config containing remote_store/url so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(UploadCommand, self).__init__(config)

    def run(self, args):
        """
        Upload contents of folders to a project with project_name on remote store.
        If follow_symlinks we will traverse symlinked directories.
        If content is already on remote site it will not be sent.
        :param args: Namespace arguments parsed from the command line.
        """
        project_name_or_id = self.create_project_name_or_id_from_args(args)
        folders = args.folders                  # list of local files/folders to upload into the project
        follow_symlinks = args.follow_symlinks  # should we follow symlinks when traversing folders
        dry_run = args.dry_run                  # do not upload anything, instead print out what you would upload
        check_file_consistency = args.check     # should we check download URLs after uploading

        # Find files and folders to upload
        local_project = LocalProject(followsymlinks=follow_symlinks, file_exclude_regex=self.config.file_exclude_regex)
        local_project.add_paths(folders)
        local_items_count = local_project.count_local_items()
        print(local_items_count.to_str(prefix="Checking"))

        # Fetch remote project (if there is one) and update local_project with details from remote project
        remote_project = self.remote_store.fetch_remote_project(project_name_or_id)
        local_project.update_remote_ids(remote_project)
        items_to_send_count = local_project.count_items_to_send(self.config.upload_bytes_per_chunk)
        print(items_to_send_count.to_str(local_items_count=local_items_count, prefix="Synchronizing"))

        if dry_run:
            # Check hashes to see what needs to be uploaded
            dry_run = ProjectUploadDryRun(local_project)
            print(dry_run.get_report())
        else:
            # Upload files and folders
            project_upload = ProjectUpload(self.config, project_name_or_id, local_project, items_to_send_count)
            project_upload.run()

            # Show user results of upload
            upload_report = project_upload.get_upload_report()
            print(upload_report.summary())
            print()
            if upload_report.sent_data:
                print('\n')
                print(upload_report.get_content())
                print('\n')
            print(project_upload.get_url_msg())
            project_upload.cleanup()

            # check for consistency unless user passes --no-check flag
            if check_file_consistency:
                self.wait_for_consistency(local_project.remote_id)

    def wait_for_consistency(self, project_id):
        client = Client(self.config)
        project = client.get_project_by_id(project_id)
        checker = ProjectChecker(self.config, project)
        try:
            checker.wait_for_consistency()
        except DSHashMismatchError:
            checker.print_bad_uploads_table()
            sys.exit(1)
        finally:
            client.close()


class DownloadCommand(ClientCommand):
    """
    Downloads the content from a remote project into a folder.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(DownloadCommand, self).__init__(config)
        self.config = config

    def run(self, args):
        """
        Download a project based on passed in args.
        :param args: Namespace arguments parsed from the command line.
        """
        project = self.get_project_by_name_or_id(args)
        folder = args.folder                # path to a folder to download data into
        # Default to project name with spaces replaced with '_' if not specified
        if not folder:
            folder = replace_invalid_path_chars(project.name.replace(' ', '_'))
        path_filter = None
        if args.include_paths or args.exclude_paths:
            path_filter = PathFilter(args.include_paths, args.exclude_paths)
        destination_path = format_destination_path(folder)
        downloader = ProjectFileDownloader(self.config, destination_path, project, path_filter=path_filter)
        downloader.run()


class AddUserCommand(BaseCommand):
    """
    Adds a user to a pre-existing remote project.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(AddUserCommand, self).__init__(config)

    def run(self, args):
        """
        Give the user with user_full_name the auth_role permissions on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        email = args.email                  # email of person to give permissions, will be None if username is specified
        username = args.username            # username of person to give permissions, will be None if email is specified
        auth_role = args.auth_role          # type of permission(project_admin)
        project = self.fetch_project(args, must_exist=True, include_children=False)
        user = self.remote_store.lookup_or_register_user_by_email_or_username(email, username)
        self.remote_store.set_user_project_permission(project, user, auth_role)
        print(u'Gave user {} {} permissions for project {}.'.format(user.full_name, auth_role, project.name))


class RemoveUserCommand(BaseCommand):
    """
    Removes a user from a pre-existing remote project.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(RemoveUserCommand, self).__init__(config)

    def run(self, args):
        """
        Remove permissions from the user with user_full_name or email on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        email = args.email                # email of person to remove permissions from (None if username specified)
        username = args.username          # username of person to remove permissions from (None if email is specified)
        project = self.fetch_project(args, must_exist=True, include_children=False)
        user = self.remote_store.lookup_or_register_user_by_email_or_username(email, username)
        self.remote_store.revoke_user_project_permission(project, user)
        print(u'Removed permissions from user {} for project {}.'.format(user.full_name, project.name))


class ShareCommand(BaseCommand):
    """
    Gives user project level permission and sends an email to that user.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(ShareCommand, self).__init__(config)
        self.service = D4S2Project(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Gives user permission based on auth_role arg and sends email to that user.
        :param args Namespace arguments parsed from the command line
        """
        email = args.email                  # email of person to send email to
        username = args.username            # username of person to send email to, will be None if email is specified
        force_send = args.resend            # is this a resend so we should force sending
        auth_role = args.auth_role          # authorization role(project permissions) to give to the user
        msg_file = args.msg_file            # message file who's contents will be sent with the share
        message = read_argument_file_contents(msg_file)
        print("Sharing project.")
        to_user = self.remote_store.lookup_or_register_user_by_email_or_username(email, username)
        try:
            project = self.fetch_project(args, must_exist=True, include_children=False)
            dest_email = self.service.share(project, to_user, force_send, auth_role, message)
            print("Share email message sent to " + dest_email)
        except D4S2Error as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise


class DeliverCommand(BaseCommand):
    """
    Transfers project to another user once they accept it via the D4S2 service.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(DeliverCommand, self).__init__(config)
        self.service = D4S2Project(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Begins process that will transfer the project to another user.
        Send delivery message to D4S2 service specifying a project and a user.
        When user accepts delivery they receive access and we lose admin privileges.
        :param args Namespace arguments parsed from the command line
        """
        email = args.email                  # email of person to deliver to, will be None if username is specified
        username = args.username            # username of person to deliver to, will be None if email is specified
        copy_project = args.copy_project    # should we deliver a copy of the project
        force_send = args.resend            # is this a resend so we should force sending
        msg_file = args.msg_file            # message file who's contents will be sent with the delivery
        share_usernames = args.share_usernames  # usernames who will have this project shared once it is accepted
        share_emails = args.share_emails    # emails of users who will have this project shared once it is accepted
        message = read_argument_file_contents(msg_file)
        project = self.fetch_project(args, must_exist=True, include_children=False)
        share_users = self.make_user_list(share_emails, share_usernames)
        print("Delivering project.")
        new_project_name = None
        if copy_project:
            new_project_name = self.get_new_project_name(project.name)
        to_user = self.remote_store.lookup_or_register_user_by_email_or_username(email, username)
        if to_user.id in [share_user.id for share_user in share_users]:
            raise DDSUserException(INVALID_DELIVERY_RECIPIENT_MSG)
        try:
            path_filter = PathFilter(args.include_paths, args.exclude_paths)
            dest_email = self.service.deliver(project, new_project_name, to_user, share_users,
                                              force_send, path_filter, message)
            print("Delivery email message sent to " + dest_email)
        except D4S2Error as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise

    def get_new_project_name(self, project_name):
        """
        Return a unique project name for the copy.
        :param project_name: str: name of project we will copy
        :return: str
        """
        timestamp_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        return "{} {}".format(project_name, timestamp_str)


class ListCommand(BaseCommand):
    """
    Print out a list of project names one line at a time or details about a single project.
    Names are escaped so the output can be used with the delete command.
    """
    def __init__(self, config):
        """
        Pass in the config for which data service and user to list data for.
        :param config: Config global configuration for use with this command.
        """
        super(ListCommand, self).__init__(config)

    def run(self, args):
        """
        Lists project names.
        :param args Namespace arguments parsed from the command line
        """
        long_format = args.long_format
        # project_name and auth_role args are mutually exclusive
        if args.project_name or args.project_id:
            project = self.fetch_project(args, must_exist=True, include_children=True)
            self.print_project_details(project, long_format)
        else:
            self.print_project_list_details(args.auth_role, long_format)

    @staticmethod
    def print_project_details(project, long_format):
        details_list = ProjectDetailsList(long_format)
        details_list.walk_project(project)
        for info in details_list.details:
            print(info)

    def print_project_list_details(self, filter_auth_role, long_format):
        """
        Prints project names to stdout for all projects or just those with the specified auth_role
        :param filter_auth_role: str: optional auth_role to filter project list
        """
        if filter_auth_role:
            projects_details = self.remote_store.get_projects_with_auth_role(auth_role=filter_auth_role)
        else:
            projects_details = self.remote_store.get_projects_details()
        if projects_details:
            for projects_detail in projects_details:
                print(self.get_project_info_line(projects_detail, long_format))
        else:
            print(NO_PROJECTS_FOUND_MESSAGE)

    @staticmethod
    def get_project_info_line(project_dict, long_format):
        project_name = project_dict['name']
        project_id = project_dict['id']
        if long_format:
            return '{}\t{}'.format(project_id, project_name)
        return project_name


class DeleteCommand(ClientCommand):
    """
    Delete a single project from the duke-data-service.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(DeleteCommand, self).__init__(config)

    def run(self, args):
        """
        Deletes a single project specified by project_name in args.
        :param args Namespace arguments parsed from the command line
        """
        project = self.get_project_by_name_or_id(args)
        delete_target = project
        delete_target_name = project.name
        if args.remote_path:
            delete_target = project.get_child_for_path(args.remote_path)
            delete_target_name = "{} path {}".format(delete_target_name, args.remote_path)
        if not args.force:
            delete_prompt = "Are you sure you wish to delete {} (y/n)?".format(delete_target_name)
            if not boolean_input_prompt(delete_prompt):
                return
        delete_target.delete()


class ListAuthRolesCommand(BaseCommand):
    """
    List available auth roles for a project. Intentionally excludes system-type auth roles.
    System-type auth roles are accepted by DukeDS add_user API endpoint but are non-functional.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(ListAuthRolesCommand, self).__init__(config)

    def run(self, args):
        """
        Prints out non deprecated project-type auth roles.
        :param args Namespace arguments parsed from the command line
        """
        auth_roles = self.remote_store.get_active_auth_roles(RemoteAuthRole.PROJECT_CONTEXT)
        if auth_roles:
            for auth_role in auth_roles:
                print(auth_role.id, "-", auth_role.description)
        else:
            print("No authorization roles found.")


class MoveCommand(ClientCommand):
    """
    Move a file/folder within a single project in the duke-data-service.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        super(MoveCommand, self).__init__(config)

    def run(self, args):
        """
        Deletes a single project specified by project_name in args.
        :param args Namespace arguments parsed from the command line
        """
        project = self.get_project_by_name_or_id(args)
        project.move_file_or_folder(args.source_remote_path, args.target_remote_path)


class InfoCommand(ClientCommand):
    """
    Prints information about a project.
    """
    def run(self, args):
        project = self.get_project_by_name_or_id(args)
        summary = project.get_summary()
        print()
        print("Name: {}".format(project.name))
        print("ID: {}".format(project.id))
        print("URL: {}".format(project.portal_url()))
        print("Size: {}".format(summary))
        print()


class CheckCommand(ClientCommand):
    """
    Checks that a project is consistent.
    """
    def run(self, args):
        project = self.get_project_by_name_or_id(args)
        checker = ProjectChecker(self.config, project)
        if args.wait:
            try:
                checker.wait_for_consistency()
            except DSHashMismatchError:
                checker.print_bad_uploads_table()
                sys.exit(1)
        else:
            print("Checking files from project {}.".format(project.name))
            if checker.files_are_ok():
                print("Project {} is consistent.".format(project.name))
            else:
                checker.print_bad_uploads_table()
                sys.exit(1)
