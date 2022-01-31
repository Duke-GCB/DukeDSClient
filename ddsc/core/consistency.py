import time
from ddsc.core.ddsapi import DSResourceNotConsistentError, DSHashMismatchError, DataServiceError
from tabulate import tabulate


class UploadDetails(object):
    def __init__(self, dds_file, remote_path):
        self.dds_file = dds_file
        self.remote_path = remote_path
        self.dds_upload = dds_file.get_upload()
        self.status = self.dds_upload.status

    def inconsistent(self):
        return not self.status.is_consistent

    def had_error(self):
        return self.status.error_on is not None

    def is_bad(self):
        return self.inconsistent() or self.had_error()

    def name(self):
        return self.dds_file.name

    def status_str(self):
        if self.inconsistent():
            return 'Inconsistent'
        elif self.had_error():
            return 'Error'
        return 'Ok'

    def file_id(self):
        return self.dds_file.id

    def message(self):
        if self.inconsistent():
            return 'started upload at {}'.format(self.status.initiated_on)
        elif self.had_error():
            return self.status.error_message
        return ''


class ProjectChecker(object):

    def __init__(self, config, project):
        self.config = config
        self.project = project

    def files_are_ok(self):
        try:
            self._try_fetch_project_files()
            return True
        except (DSResourceNotConsistentError, DSHashMismatchError):
            return False
        except DataServiceError as e:
            if e.status_code == 400:
                return False
            raise

    def _try_fetch_project_files(self):
        # exhaust the project files generator to fetch urls for all files
        for _, _ in self.project.get_project_files_generator(self.config.page_size):
            pass

    def get_bad_uploads(self):
        results = []
        for remote_path, dds_file in self.project.get_path_to_files().items():
            upload_details = UploadDetails(dds_file, remote_path)
            if upload_details.is_bad():
                results.append(upload_details)
        return results

    def get_bad_uploads_table_data(self):
        headers = ["File", "Status", "Message", "FileID", "RemotePath"]
        data = []
        for upload_details in self.get_bad_uploads():
            data.append([
                upload_details.name(),
                upload_details.status_str(),
                upload_details.message(),
                upload_details.file_id(),
                upload_details.remote_path
            ])
        return headers, data

    def delete_bad_uploads(self):
        for bad_upload in self.get_bad_uploads():
            bad_upload.dds_file.delete()

    def print_bad_uploads_table(self):
        print("ERROR: Project {} is not in a consistent state.\n".format(self.project.name))
        print("Please wait while file uploads are checked.\nThis process can take quite a while.")
        headers, data = self.get_bad_uploads_table_data()
        print(tabulate(data, headers=headers))
        print("\nNOTE: Inconsistent files should resolve in a few minutes after starting.")
        print("\nAn inconsistent file can be deleted by running:\n ddsclient delete -p <ProjectName> --path <RemotePath>")
        print()

    def wait_for_consistency(self, wait_sec=5):
        while True:
            try:
                print("Checking files for project {}.".format(self.project.name))
                self._try_fetch_project_files()
                # if we are able to fetch all files project is in a consistent state
                print("Project {} is consistent.".format(self.project.name))
                return True
            except DSResourceNotConsistentError:
                print("Project not consistent yet. Waiting.")
                time.sleep(wait_sec)
            except DSHashMismatchError:
                raise
