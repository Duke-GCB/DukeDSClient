from localstore import LocalContent, LocalOnlyCounter, UploadReport
from remotestore import RemoteContentFetch, RemoteContentSender
from util import ProgressPrinter
from ddsapi import DataServiceApi


class DDSClient(object):
    def __init__(self, config):
        self.config = config
        self.data_service = DataServiceApi(config.auth, config.url)
        self.local_content = None

    def run(self):
        self._load_local_content()
        self._fetch_remote_content()
        self._compare_local_with_remote()
        self._send_different_local_content()
        self._print_report()

    def _load_local_content(self):
        self.local_content = LocalContent()
        self.local_content.add_paths(self.config.folders)

    def _fetch_remote_content(self):
        remote_fetch = RemoteContentFetch(self.data_service)
        self.remote_project = remote_fetch.fetch_remote_project(self.config.project_name, self.config.folders)

    def _compare_local_with_remote(self):
        self.local_content.update_remote_ids(self.remote_project)

    def _send_different_local_content(self):
        counter = LocalOnlyCounter()
        self.local_content.accept(counter)
        print 'Uploading {}.'.format(counter.result_str())
        progress_printer = ProgressPrinter(counter.total_items())
        sender = RemoteContentSender(self.data_service, self.local_content.remote_id, self.config.project_name, progress_printer)
        self.local_content.accept(sender)
        progress_printer.finished()

    def _print_report(self):
        report = UploadReport(self.config.project_name)
        self.local_content.accept(report)
        print(report)



