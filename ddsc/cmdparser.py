"""
Command line parser for the application.
"""
import os
import sys
import argparse


INVALID_PATH_CHARS = (':', '/', '\\')


def replace_invalid_path_chars(path):
    """
    Converts bad path characters to '_'.
    :param path: str path to fix
    :return: str fixed path
    """
    for bad_char in INVALID_PATH_CHARS:
        path = path.replace(bad_char, '_')
    return path


def to_unicode(s):
    """
    Convert a command line string to utf8 unicode.
    :param s: string to convert to unicode
    :return: unicode string for argument
    """
    if sys.version_info >= (3,0,0):
        return str(s)
    else:
        if type(s) != unicode:
            return unicode(s, 'utf8')
        return s


def add_project_name_arg(arg_parser, required=True, help_text="Name of the remote project to manage."):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("-p",
                           metavar='ProjectName',
                           type=to_unicode,
                           dest='project_name',
                           help=help_text,
                           required=required)


def _paths_must_exists(path):
    """
    Raises error if path doesn't exist.
    :param path: str path to check
    :return: str same path passed in
    """
    path = to_unicode(path)
    if not os.path.exists(path):
     raise argparse.ArgumentTypeError("{} is not a valid file/folder.".format(path))
    return path


def path_does_not_exist_or_is_empty(path):
    """
    Raises error if the directory the path exists and contains any files.
    :param path: str path to check
    :return: str path
    """
    if os.path.exists(path):
        if os.listdir(path):
            raise argparse.ArgumentTypeError("{} already exists and is not an empty directory.".format(path))
    path = to_unicode(path)
    return _path_has_ok_chars(path)


def _path_has_ok_chars(path):
    """
    Validate path for invalid characters.
    :param path: str possible filesystem path
    :return: path if it was ok otherwise raises error
    """
    basename = os.path.basename(path)
    if any([bad_char in basename for bad_char in INVALID_PATH_CHARS]):
            raise argparse.ArgumentTypeError("{} contains invalid characters for a directory.".format(path))
    return path


def _add_folders_positional_arg(arg_parser):
    """
    Adds folders and/or filenames parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("folders",
                           metavar='Folders',
                           nargs="+",
                           help="Names of the files and/or folders to upload to the remote project.",
                           type=_paths_must_exists)


def _add_folder_positional_arg(arg_parser):
    """
    Adds folders and/or filenames parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("folder",
                           metavar='Folder',
                           help="Name of the folder to download the project contents into. "
                                "If not specified it will use the name of the project with spaces translated to '_'. "
                                "This folder must be empty or not exist(will be created).",
                           type=path_does_not_exist_or_is_empty,
                           nargs='?')


def _add_follow_symlinks_arg(arg_parser):
    """
    Adds optional follow_symlinks parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--follow_symlinks",
                            help="Follow symbolic links(experimental).",
                            action='store_true',
                            dest='follow_symlinks')


def add_user_arg(arg_parser):
    """
    Adds username parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--user",
                            metavar='Username',
                            type=to_unicode,
                            dest='username',
                            help="Username(NetID) to update permissions on. "
                                 "You must specify either --email or this flag.")


def add_email_arg(arg_parser):
    """
    Adds user_email parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--email",
                            metavar='UserEmail',
                            type=to_unicode,
                            dest='email',
                            help="Email of the person you want to update permissions on."
                                 " You must specify either --user or this flag.")


def _add_auth_role_arg(arg_parser, default_permissions):
    """
    Adds optional auth_role parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param default_permissions: default value to use for this argument
    """
    help_text = "Specifies which project permissions to give to the user. Example: 'project_admin'. "
    help_text += "See command list_auth_roles for AuthRole values."
    arg_parser.add_argument("--auth_role",
                            metavar='AuthRole',
                            type=to_unicode,
                            dest='auth_role',
                            help=help_text,
                            default=default_permissions)


def _add_copy_project_arg(arg_parser):
    """
    Adds optional copy_project parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--skip_copy_project",
                            help="Should we just send the deliver email and skip copying the project.",
                            action='store_true',
                            default=False,
                            dest='skip_copy_project')


def _add_resend_arg(arg_parser, resend_help):
    """
    Adds resend parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param type_str
    """
    arg_parser.add_argument("--resend",
                            action='store_true',
                            default=False,
                            dest='resend',
                            help=resend_help)


def _add_force_arg(arg_parser, help_text):
    """
    Adds optional force parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    :param help_text: str label displayed in usage
    """
    arg_parser.add_argument("--force",
                            help=help_text,
                            action='store_true',
                            dest='force')


def _add_include_arg(arg_parser):
    """
    Adds optional repeatable include parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--include",
                            metavar='Path',
                            action='append',
                            type=to_unicode,
                            dest='include_paths',
                            help="Specifies a single path to include. This argument can be repeated.",
                            default=[])


def _add_exclude_arg(arg_parser):
    """
    Adds optional repeatable exclude parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--exclude",
                            metavar='Path',
                            action='append',
                            type=to_unicode,
                            dest='exclude_paths',
                            help="Specifies a single path to exclude. This argument can be repeated.",
                            default=[])


class CommandParser(object):
    """
    Root command line parser. Supports the following commands: upload and add_user.
    You must register external functions to called for the various commands.
    Commands must be registered to appear in help.
    """
    def __init__(self):
        self.parser = argparse.ArgumentParser()
        self.subparsers = self.parser.add_subparsers()
        self.upload_func = None
        self.add_user_func = None
        self.download_func = None

    def register_upload_command(self, upload_func):
        """
        Add the upload command to the parser and call upload_func(project_name, folders, follow_symlinks) when chosen.
        :param upload_func: func Called when this option is chosen: upload_func(project_name, folders, follow_symlinks).
        """
        description = "Uploads local files and folders to a remote host."
        upload_parser = self.subparsers.add_parser('upload', description=description)
        add_project_name_arg(upload_parser, help_text="Name of the project to upload files/folders to.")
        _add_folders_positional_arg(upload_parser)
        _add_follow_symlinks_arg(upload_parser)
        upload_parser.set_defaults(func=upload_func)

    def register_add_user_command(self, add_user_func):
        """
        Add the add_user command to the parser and call add_user_func(project_name, user_full_name, auth_role)
        when chosen.
        :param add_user_func: func Called when this option is chosen: upload_func(project_name, user_full_name, auth_role).
        """
        description = "Gives user permission to access a remote project."
        add_user_parser = self.subparsers.add_parser('add_user', description=description)
        add_project_name_arg(add_user_parser, help_text="Name of the project to add a user to.")
        user_or_email = add_user_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_auth_role_arg(add_user_parser, default_permissions='project_admin')
        add_user_parser.set_defaults(func=add_user_func)

    def register_remove_user_command(self, remove_user_func):
        """
        Add the remove_user command to the parser and call remove_user_func(project_name, user_full_name) when chosen.
        :param remove_user_func: func Called when this option is chosen: remove_user_func(project_name, user_full_name).
        """
        description = "Removes user permission to access a remote project."
        remove_user_parser = self.subparsers.add_parser('remove_user', description=description)
        add_project_name_arg(remove_user_parser, help_text="Name of the project to remove a user from.")
        user_or_email = remove_user_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        remove_user_parser.set_defaults(func=remove_user_func)

    def register_download_command(self, download_func):
        """
        Add 'download' command for downloading a project to a non-existing or empty directory.
        :param download_func: function to run when user choses this option
        """
        description = "Download the contents of a remote remote project to a local folder."
        download_parser = self.subparsers.add_parser('download', description=description)
        add_project_name_arg(download_parser, help_text="Name of the project to download.")
        _add_folder_positional_arg(download_parser)
        include_or_exclude = download_parser.add_mutually_exclusive_group(required=False)
        _add_include_arg(include_or_exclude)
        _add_exclude_arg(include_or_exclude)
        download_parser.set_defaults(func=download_func)

    def register_share_command(self, share_func):
        """
        Add 'share' command for adding view only project permissions and sending email via another service.
        :param share_func: function to run when user choses this option
        """
        description = "Share a project with another user with specified  permissions. " \
                      "Sends the other user an email message via D4S2 service. " \
                      "If not specified this command gives user download permissions."
        share_parser = self.subparsers.add_parser('share', description=description)
        add_project_name_arg(share_parser)
        user_or_email = share_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_auth_role_arg(share_parser, default_permissions='file_downloader')
        _add_resend_arg(share_parser, "Resend share")
        share_parser.set_defaults(func=share_func)

    def register_deliver_command(self, deliver_func):
        """
        Add 'deliver' command for transferring a project to another user.,
        :param deliver_func: function to run when user choses this option
        """
        description = "Initiate delivery of a project to another user. Removes other user's current permissions. " \
                      "Makes a copy of the project. Send message to D4S2 service to send email and allow " \
                      "access to the copy of the project once user acknowledges receiving the data."
        deliver_parser = self.subparsers.add_parser('deliver', description=description)
        add_project_name_arg(deliver_parser)
        user_or_email = deliver_parser.add_mutually_exclusive_group(required=True)
        add_user_arg(user_or_email)
        add_email_arg(user_or_email)
        _add_copy_project_arg(deliver_parser)
        _add_resend_arg(deliver_parser, "Resend delivery")
        include_or_exclude = deliver_parser.add_mutually_exclusive_group(required=False)
        _add_include_arg(include_or_exclude)
        _add_exclude_arg(include_or_exclude)
        deliver_parser.set_defaults(func=deliver_func)

    def register_list_command(self, list_func):
        """
        Add 'list' command to get a list of projects or details about one project.
        :param list_func: function: run when user choses this option.
        """
        description = "Show a list of project names or folders/files of a single project."
        list_parser = self.subparsers.add_parser('list', description=description)
        add_project_name_arg(list_parser, required=False, help_text="Name of the project to show details for.")
        list_parser.set_defaults(func=list_func)

    def register_delete_command(self, delete_func):
        """
        Add 'delete' command delete a project from the remote store.
        :param delete_func: function: run when user choses this option.
        """
        description = "Permanently delete a project."
        delete_parser = self.subparsers.add_parser('delete', description=description)
        add_project_name_arg(delete_parser, help_text="Name of the project to delete.")
        _add_force_arg(delete_parser, "Do not prompt before deleting.")
        delete_parser.set_defaults(func=delete_func)

    def register_list_auth_roles_command(self, list_auth_roles_func):
        """
        Add 'list_auth_roles' command to list project authorization roles that can be used with add_user.
        :param list_auth_roles_func: function: run when user choses this option.
        """
        description = "List authorization roles for use with add_user command."
        list_auth_roles_parser = self.subparsers.add_parser('list_auth_roles', description=description)
        list_auth_roles_parser.set_defaults(func=list_auth_roles_func)

    def run_command(self, args):
        """
        Parse command line arguments and run function registered for the appropriate command.
        :param args: [str] command line arguments
        """
        parsed_args = self.parser.parse_args(args)
        if hasattr(parsed_args, 'func'):
            parsed_args.func(parsed_args)
        else:
            self.parser.print_help()

