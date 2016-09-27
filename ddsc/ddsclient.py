""" Runs the appropriate command for a user based on arguments. """
from __future__ import print_function
import sys
import datetime
import pipes
from ddsc.core.d4s2 import D4S2Project, D4S2Error
from ddsc.core.remotestore import RemoteStore, RemoteAuthRole
from ddsc.core.upload import ProjectUpload
from ddsc.cmdparser import CommandParser, path_does_not_exist_or_is_empty, replace_invalid_path_chars
from ddsc.core.download import ProjectDownload
from ddsc.core.util import ProjectFilenameList, verify_terminal_encoding
from ddsc.core.pathfilter import PathFilter

NO_PROJECTS_FOUND_MESSAGE = 'No projects found.'


class DDSClient(object):
    """
    Runs various commands based on arguments.
    """
    def __init__(self, config):
        """
        Pass in the configuration for the data service.
        :param config: ddsc.config.Config configuration object to be used with the DataServiceApi.
        """
        self.config = config

    def run_command(self, args):
        """
        Create a parser and have it parse the args then run the appropriate command.
        :param args: [str] command line args
        """
        parser = self._create_parser()
        parser.run_command(args)

    def _create_parser(self):
        """
        Create a parser hooking up the command methods below to be run when chosen.
        :return: CommandParser parser with commands attached.
        """
        parser = CommandParser()
        parser.register_list_command(self._setup_run_command(ListCommand))
        parser.register_upload_command(self._setup_run_command(UploadCommand))
        parser.register_add_user_command(self._setup_run_command(AddUserCommand))
        parser.register_remove_user_command(self._setup_run_command(RemoveUserCommand))
        parser.register_download_command(self._setup_run_command(DownloadCommand))
        parser.register_share_command(self._setup_run_command(ShareCommand))
        parser.register_deliver_command(self._setup_run_command(DeliverCommand))
        parser.register_delete_command(self._setup_run_command(DeleteCommand))
        parser.register_list_auth_roles_command(self._setup_run_command(ListAuthRolesCommand))
        return parser

    def _setup_run_command(self, command_constructor):
        """
        Create f(args) to run that will create the specified object and call run when invoked.
        The reason for this complexity is deferring the creation of expensive objects until
        we have decided to run a command. For instance setting up the data service api if we are just running -h.
        :param command_constructor: class specifies object to create and pass args to(eventually).
        :return: func function that will let the command created by command_constructor run with arguments.
        """
        return lambda args: self._run_command(command_constructor, args)

    def _run_command(self, command_constructor, args):
        """
        Run command_constructor and call run(args) on the resulting object
        :param command_constructor: class of an object that implements run(args)
        :param args: object arguments for specific command created by CommandParser
        """
        verify_terminal_encoding(sys.stdout.encoding)
        command = command_constructor(self.config)
        command.run(args)


class UploadCommand(object):
    """
    Uploads a folder to a remote project.
    """
    def __init__(self, config):
        """
        Pass in the config containing remote_store/url so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.config = config

    def run(self, args):
        """
        Upload contents of folders to a project with project_name on remote store.
        If follow_symlinks we will traverse symlinked directories.
        If content is already on remote site it will not be sent.
        :param args: Namespace arguments parsed from the command line.
        """
        project_name = args.project_name        # name of the remote project to create/upload to
        folders = args.folders                  # list of local files/folders to upload into the project
        follow_symlinks = args.follow_symlinks  # should we follow symlinks when traversing folders

        project_upload = ProjectUpload(self.config, project_name, folders, follow_symlinks=follow_symlinks)
        print(project_upload.get_differences_summary())
        if project_upload.needs_to_upload():
            project_upload.run()
            print('\n')
            print(project_upload.get_upload_report())
            print('\n')
        print(project_upload.get_url_msg())


class DownloadCommand(object):
    """
    Downloads the content from a remote project into a folder.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Download a project based on passed in args.
        :param args: Namespace arguments parsed from the command line.
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        folder = args.folder                # path to a folder to download data into
        # Default to project name with spaces replaced with '_' if not specified
        if not folder:
            fixed_path = replace_invalid_path_chars(project_name.replace(' ', '_'))
            folder = path_does_not_exist_or_is_empty(fixed_path)
        path_filter = PathFilter(args.include_paths, args.exclude_paths)
        project_download = ProjectDownload(self.remote_store, project_name, folder, path_filter)
        project_download.run()


class AddUserCommand(object):
    """
    Adds a user to a pre-existing remote project.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Give the user with user_full_name the auth_role permissions on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to give permissions, will be None if username is specified
        username = args.username            # username of person to give permissions, will be None if email is specified
        auth_role = args.auth_role          # type of permission(project_admin)
        project = self.remote_store.fetch_remote_project(project_name, must_exist=True, include_children=False)
        user = self.remote_store.lookup_user_by_email_or_username(email, username)
        self.remote_store.set_user_project_permission(project, user, auth_role)
        print(u'Gave user {} {} permissions for {}.'.format(user.full_name, auth_role, project_name))


class RemoveUserCommand(object):
    """
    Removes a user from a pre-existing remote project.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Remove permissions from the user with user_full_name or email on the remote project with project_name.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name  # name of the pre-existing project to set permissions on
        email = args.email                # email of person to remove permissions from (None if username specified)
        username = args.username          # username of person to remove permissions from (None if email is specified)
        project = self.remote_store.fetch_remote_project(project_name, must_exist=True, include_children=False)
        user = self.remote_store.lookup_user_by_email_or_username(email, username)
        self.remote_store.revoke_user_project_permission(project, user)
        print(u'Removed permissions from user {} for project {}.'.format(user.full_name, project_name))


class ShareCommand(object):
    """
    Gives user project level permission and sends an email to that user.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.service = D4S2Project(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Gives user permission based on auth_role arg and sends email to that user.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to send email to
        username = args.username            # username of person to send email to, will be None if email is specified
        force_send = args.resend            # is this a resend so we should force sending
        auth_role = args.auth_role          # authorization role(project permissions) to give to the user
        to_user = self.remote_store.lookup_user_by_email_or_username(email, username)
        try:
            dest_email = self.service.share(project_name, to_user, force_send, auth_role)
            print("Share email message sent to " + dest_email)
        except D4S2Error as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise


class DeliverCommand(object):
    """
    Transfers project to another user once they accept it via the D4S2 service.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)
        self.service = D4S2Project(config, self.remote_store, print_func=print)

    def run(self, args):
        """
        Begins process that will transfer the project to another user.
        Send delivery message to D4S2 service specifying a project and a user.
        When user accepts delivery they receive access and we lose admin privileges.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name    # name of the pre-existing project to set permissions on
        email = args.email                  # email of person to deliver to, will be None if username is specified
        username = args.username            # username of person to deliver to, will be None if email is specified
        skip_copy_project = args.skip_copy_project  # should we skip the copy step
        force_send = args.resend            # is this a resend so we should force sending
        new_project_name = None
        if not skip_copy_project:
            new_project_name = self.get_new_project_name(project_name)
        to_user = self.remote_store.lookup_user_by_email_or_username(email, username)
        try:
            path_filter = PathFilter(args.include_paths, args.exclude_paths)
            dest_email = self.service.deliver(project_name, new_project_name, to_user, force_send, path_filter)
            print("Delivery email message sent to " + dest_email)
        except D4S2Error as ex:
            if ex.warning:
                print(ex.message)
            else:
                raise

    def get_new_project_name(self, project_name):
        """
        Return a unique project name for the copy.
        :param project_name:
        :return:
        """
        timestamp_str = datetime.datetime.utcnow().strftime('%Y-%m-%d %H:%M')
        return "{} {}".format(project_name, timestamp_str)


class ListCommand(object):
    """
    Print out a list of project names one line at a time or details about a single project.
    Names are escaped so the output can be used with the delete command.
    """
    def __init__(self, config):
        """
        Pass in the config for which data service and user to list data for.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Lists project names.
        :param args Namespace arguments parsed from the command line
        """
        if args.project_name:
            project = self.remote_store.fetch_remote_project(args.project_name, must_exist=True)
            self.print_project_details(project)
        else:
            self.print_project_names()

    def print_project_details(self, project):
        filename_list = ProjectFilenameList()
        filename_list.walk_project(project)
        for info in filename_list.details:
            print(info)

    def print_project_names(self):
        names = self.remote_store.get_project_names()
        if names:
            for name in names:
                print(pipes.quote(name))
        else:
            print(NO_PROJECTS_FOUND_MESSAGE)


class DeleteCommand(object):
    """
    Delete a single project from the duke-data-service.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

    def run(self, args):
        """
        Deletes a single project specified by project_name in args.
        :param args Namespace arguments parsed from the command line
        """
        project_name = args.project_name
        project = self.remote_store.fetch_remote_project(project_name, must_exist=False)
        if not project:
            raise ValueError("No project named '{}' found.\n".format(project_name))
        else:
            if not args.force:
                delete_prompt = "Are you sure you wish to delete {} (y/n)?".format(project_name)
                if not boolean_input_prompt(delete_prompt):
                    return
            self.remote_store.delete_project_by_name(args.project_name)


class ListAuthRolesCommand(object):
    """
    List available auth roles for a project. Intentionally excludes system-type auth roles.
    System-type auth roles are accepted by DukeDS add_user API endpoint but are non-functional.
    """
    def __init__(self, config):
        """
        Pass in the config who can create a remote_store so we can access the remote data.
        :param config: Config global configuration for use with this command.
        """
        self.remote_store = RemoteStore(config)

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


def boolean_input_prompt(message):
    if sys.version_info >= (3, 0, 0):
        result = input(message)
    else:
        result = raw_input(message)
    result = result.upper()
    return result == "Y" or result == "YES" or result == "T" or result == "TRUE"



