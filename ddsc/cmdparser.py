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
        return unicode(s, 'utf8')

def _add_project_name_arg(arg_parser):
    """
    Adds project_name parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("-p",
                           metavar='ProjectName',
                           type=to_unicode,
                           dest='project_name',
                           help="Name of the remote project to manage.",
                           required=True)


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


def _add_user_arg(arg_parser):
    """
    Adds username parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--user",
                            metavar='Username',
                            type=to_unicode,
                            dest='username',
                            help="Username(NetID) to give permissions. "
                                 "You must specify either --email or this flag.")


def _add_email_arg(arg_parser):
    """
    Adds user_email parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--email",
                            metavar='UserEmail',
                            type=to_unicode,
                            dest='email',
                            help="Email of the person you want to give permission."
                                 " You must specify either --user or this flag.")

def _add_auth_role_arg(arg_parser):
    """
    Adds optional auth_role parameter to a parser.
    :param arg_parser: ArgumentParser parser to add this argument to.
    """
    arg_parser.add_argument("--auth_role",
                            metavar='AuthRole',
                            type=to_unicode,
                            dest='auth_role',
                            help="Specifies authorization role for the user ('project_admin').",
                            default='project_admin')


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
        _add_project_name_arg(upload_parser)
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
        _add_project_name_arg(add_user_parser)
        user_or_email = add_user_parser.add_mutually_exclusive_group(required=True)
        _add_user_arg(user_or_email)
        _add_email_arg(user_or_email)
        _add_auth_role_arg(add_user_parser)
        add_user_parser.set_defaults(func=add_user_func)

    def register_download_command(self, download_func):
        description = "Download the contents of a remote remote project to a local folder."
        download_parser = self.subparsers.add_parser('download', description=description)
        _add_project_name_arg(download_parser)
        _add_folder_positional_arg(download_parser)
        download_parser.set_defaults(func=download_func)

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