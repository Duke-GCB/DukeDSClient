import os
import shutil
import platform
import subprocess
from ddsc.azure.exceptions import DDSAzureSetupException, DDSAzCopyException

INSTALL_AZCOPY_INSTRUCTIONS = """
ERROR: The azcopy command line program was not found.

This program must be installed to upload and download files.
Please install azcopy using the following instructions:
https://docs.microsoft.com/en-us/azure/storage/common/storage-use-azcopy-v10#download-azcopy
For Mac or Linux place the azcopy in ~/bin or somewhere within $PATH.

"""


def create_azcopy_executable_path():
    system = platform.system()
    if system == 'Windows':
        home_dir = os.environ.get('USERPROFILE')
        if not home_dir:
            raise DDSAzureSetupException('Windows USERPROFILE has not been setup.')
        return os.path.join(home_dir, r'.azcopy\azcopy.exe')
    elif system in ('Linux', 'Darwin'):
        return os.path.expanduser(os.path.join('~', 'bin/azcopy'))
    raise DDSAzureSetupException(f"Platform {system} has not been implemented.")


def find_azcopy_executable_path():
    path = shutil.which("azcopy")
    if path:
        return path
    expected_path = create_azcopy_executable_path()
    if os.path.exists(expected_path):
        return expected_path

    return None


def group_by_dirname(paths):
    dirname_to_files = {}
    for path in paths:
        path_dirname = os.path.dirname(path)
        files = dirname_to_files.get(path_dirname, [])
        files.append(path)
        dirname_to_files[path_dirname] = files
    return dirname_to_files


class AzCopy(object):
    PUT_MD5_ARG = "--put-md5"
    CHECK_MD5_ARG = "--check-md5"
    CHECK_MD5_ARG_VALUE = "FailIfDifferentOrMissing"
    NOT_RECURSIVE_ARG = "--recursive=false"
    INCLUDE_PATTERN_ARG = "--include-pattern"
    DRY_RUN_ARG = "--dry-run"
    EXCLUDE_REGEX_ARG = "--exclude-regex"
    INCLUDE_REGEX_ARG = "--include-regex"

    def __init__(self, azcopy_executable, print_cmds=True):
        self.azcopy_executable = azcopy_executable
        self.print_cmds = print_cmds

    def _run_azcopy(self, source, destination, extra_args=[], dry_run=False, include_patterns=[]):
        if not self.azcopy_executable:
            raise DDSAzureSetupException(INSTALL_AZCOPY_INSTRUCTIONS)
        cmd = [self.azcopy_executable, "sync", source, destination]
        cmd.extend(extra_args)
        if include_patterns:
            cmd.append(self.INCLUDE_PATTERN_ARG)
            cmd.append(';'.join(include_patterns))
        if dry_run:
            cmd.append(self.DRY_RUN_ARG)
        if self.print_cmds:
            print('Running', ' '.join(cmd))
        completed_process = subprocess.run(cmd)
        if completed_process.returncode != 0:
            raise DDSAzCopyException(f"ERROR: azcopy failed with exit code {completed_process.returncode}")

    def upload_files(self, source_parent_dir, source_filenames, destination, dry_run=False):
        self._run_azcopy(
            source=source_parent_dir,
            include_patterns=source_filenames,
            destination=destination,
            dry_run=dry_run,
            extra_args=[self.PUT_MD5_ARG, self.NOT_RECURSIVE_ARG]
        )

    def upload_directory(self, source, destination, dry_run=False):
        self._run_azcopy(
            source=source,
            destination=destination,
            dry_run=dry_run,
            extra_args=[self.PUT_MD5_ARG]
        )

    def download_directory(self, source, destination, include_paths, exclude_paths, dry_run=False):
        extra_args = [self.CHECK_MD5_ARG, self.CHECK_MD5_ARG_VALUE]
        if include_paths:
            extra_args.append(self.INCLUDE_REGEX_ARG)
            extra_args.append(';'.join(include_paths))
        if exclude_paths:
            extra_args.append(self.EXCLUDE_REGEX_ARG)
            extra_args.append(';'.join(exclude_paths))
        os.makedirs(destination, exist_ok=True)
        self._run_azcopy(
            source=source,
            destination=destination,
            dry_run=dry_run,
            extra_args=extra_args
        )


def create_azcopy():
    return AzCopy(find_azcopy_executable_path())
