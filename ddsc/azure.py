from ddsc.exceptions import DDSUserException
from ddsc.sdk.azure import Azure
from ddsc.cmdparser import replace_invalid_path_chars
from ddsc.core.util import boolean_input_prompt

USER_EMAIL_NOT_SUPPORTED_MSG = "Error: The -e/--email flag is not supported with the Azure backend.\n"
PROJECT_ID_NOT_SUPPORTED_MSG = "Error: The -i/--id flag is not supported with the Azure backend.\n"
CHECK_COMMAND_NOT_SUPPORTED_MSG = "Error: The check command is not supported or needed for the Azure backend.\n"


class BaseAzureCommand(object):
    def __init__(self, config):
        self.config = config
        self._azure = None

    @property
    def azure(self):
        if not self._azure:
            self._azure = Azure(self.config)
        return self._azure

    def cleanup(self):
        pass

    @staticmethod
    def get_netid(args):
        if args.email:
            raise DDSUserException(USER_EMAIL_NOT_SUPPORTED_MSG)
        return args.username

    def get_project(self, args):
        if args.project_id:
            raise DDSUserException(PROJECT_ID_NOT_SUPPORTED_MSG)
        if args.project_name:
            return self.azure.get_project(project_name=args.project_name)
        return None


class AzureListCommand(BaseAzureCommand):
    def run(self, args):
        show_project = self.get_project(args)
        if show_project:
            print("Project {} Contents:".format(show_project.name))
            for azure_file in self.azure.get_files(show_project):
                if args.long_format:
                    print("{} (md5:{})".format(azure_file.name, azure_file.md5))
                else:
                    print(azure_file.name)
        else:
            for project in self.azure.get_projects():
                if args.auth_role:
                    if project.auth_role == args.auth_role:
                        print(project.name)
                else:
                    print(project.name)


class AzureUploadCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        self.azure.upload_files(project=project, paths=args.folders,
                                follow_symlinks=args.follow_symlinks, dry_run=args.dry_run)


class AzureAddUserCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure.add_user(project=project, netid=netid, auth_role=args.auth_role)


class AzureRemoveUserCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure.remove_user(project=project, netid=netid)


class AzureDownloadCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        destination = args.folder
        if not destination:
            destination = replace_invalid_path_chars(project.name.replace(' ', '_'))
        self.azure.download_files(
            project=project,
            include_paths=args.include_paths,
            exclude_paths=args.exclude_paths,
            destination=destination)


class AzureShareCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure.share(project=project, netid=netid, auth_role=args.auth_role)


class AzureDeliverCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        netid = self.get_netid(args)
        self.azure.deliver(project=project, netid=netid, copy_project=args.copy_project, resend=args.resend,
                           msg_file=args.msg_file, share_usernames=args.share_usernames)


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
        self.azure.delete(project=project, remote_path=args.remote_path)


class AzureListAuthRolesCommand(BaseAzureCommand):
    def run(self, args):
        for auth_role in self.azure.get_auth_roles():
            print(auth_role.id, "-", auth_role.description)


class AzureMoveCommand(BaseAzureCommand):
    def run(self, args):
        project = self.get_project(args)
        self.azure.move(project=project,
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
