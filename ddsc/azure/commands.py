from ddsc.exceptions import DDSUserException
from ddsc.azure.api import create_azure_api
from ddsc.cmdparser import replace_invalid_path_chars
from ddsc.core.util import boolean_input_prompt, read_argument_file_contents

USER_EMAIL_NOT_SUPPORTED_MSG = "Error: The -e/--email flag is not supported with the Azure backend.\n"
PROJECT_ID_NOT_SUPPORTED_MSG = "Error: The -i/--id flag is not supported with the Azure backend.\n"
CHECK_COMMAND_NOT_SUPPORTED_MSG = "Error: The check command is not supported or needed for the Azure backend.\n"
SHARE_NOT_SUPPORTED_MSG = "Error: The share command is not supported for the Azure backend.\n"
COPY_NOT_SUPPORTED_FOR_AZURE_MSG = "Error: The --copy option is not supported for the Azure backend.\n"


class BaseAzureCommand(object):
    def __init__(self, config):
        self.config = config
        self._azure_api = None

    @property
    def azure_api(self):
        if not self._azure_api:
            self._azure_api = create_azure_api(self.config)
        return self._azure_api

    def cleanup(self):
        pass

    @staticmethod
    def get_netid(args):
        if args.email:
            raise DDSUserException(USER_EMAIL_NOT_SUPPORTED_MSG)
        return args.username

    def get_project(self, args):
        project = None
        if args.project_id:
            raise DDSUserException(PROJECT_ID_NOT_SUPPORTED_MSG)
        if args.project_name:
            project = self.azure_api.get_project_by_name(args.project_name)
            if not project:
                raise DDSUserException("No project found with name {}".format(args.project_name))
        return project


class AzureListCommand(BaseAzureCommand):
    def run(self, args):
        show_project = self.get_project(args)
        if show_project:
            print("Project {} Contents:".format(show_project.name))
            for azure_file in show_project.get_file_paths():
                if args.long_format:
                    print("{} (md5:{})".format(azure_file.project_path, azure_file.get_md5()))
                else:
                    print(azure_file.project_path)
        else:
            for project in self.azure_api.list_projects():
                if args.auth_role:
                    if project.auth_role == args.auth_role:
                        self.print_project(project, args.long_format)
                else:
                    self.print_project(project, args.long_format)

    @staticmethod
    def print_project(project, long_format):
        if long_format:
            print(f"{project.name}\t{project.get_url()}")
        else:
            print(project.name)


class AzureUploadCommand(BaseAzureCommand):
    def run(self, args):
        self.azure_api.upload_paths(project_name=args.project_name, paths=args.folders, dry_run=args.dry_run)


class AzureAddUserCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure_api.add_user_to_project(project=project, netid=netid, auth_role=args.auth_role)


class AzureRemoveUserCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure_api.remove_user_from_project(project=project, netid=netid)


class AzureDownloadCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        destination = args.folder
        if not destination:
            destination = replace_invalid_path_chars(project.name.replace(' ', '_'))
        self.azure_api.download_paths(
            project_name=project.name,
            include_paths=args.include_paths,
            exclude_paths=args.exclude_paths,
            destination=destination,
            dry_run=args.dry_run
        )


class AzureShareCommand(BaseAzureCommand):
    def run(self, args):
        raise DDSUserException(SHARE_NOT_SUPPORTED_MSG)


class AzureDeliverCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        if args.copy_project:
            raise DDSUserException(COPY_NOT_SUPPORTED_FOR_AZURE_MSG)
        user_message = read_argument_file_contents(args.msg_file)
        self.azure_api.deliver(project=project, netid=netid, resend=args.resend, user_message=user_message,
                               share_usernames=args.share_usernames)


class AzureDeleteCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        delete_target_name = project.name
        if args.remote_path:
            delete_target_name = "{} path {}".format(project.name, args.remote_path)
        if not args.force:
            delete_prompt = "Are you sure you wish to delete {} (y/n)?".format(delete_target_name)
            if not boolean_input_prompt(delete_prompt):
                return
        if args.remote_path:
            self.azure_api.delete_remote_path(project, args.remote_path)
        else:
            self.azure_api.delete_project(project)


class AzureListAuthRolesCommand(BaseAzureCommand):
    def run(self, args):
        for auth_role in self.azure_api.get_auth_roles():
            print(auth_role.id, "-", auth_role.description)


class AzureMoveCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        self.azure_api.move(project=project,
                            source_remote_path=args.source_remote_path,
                            target_remote_path=args.target_remote_path)


class AzureInfoCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        print()
        print("Name:", project.name)
        print("URL:", project.get_url())
        print("Size:", project.get_size_str())
        print()


class AzureCheckCommand(BaseAzureCommand):
    def run(self, args):
        raise DDSUserException(CHECK_COMMAND_NOT_SUPPORTED_MSG)
