import sys
import requests
import os
import multiprocessing
import time
import queue
from ddsc.core.download import FileHashStatus, SWIFT_EXPIRED_STATUS_CODE, S3_EXPIRED_STATUS_CODE
from ddsc.core.remotestore import DOWNLOAD_FILE_CHUNK_SIZE
from ddsc.config import GET_PAGE_SIZE_DEFAULT
from ddsc.core.ddsapi import DDS_TOTAL_HEADER


class FileDownloader(object):
    def __init__(self, dest_directory, project, num_workers):
        self.dest_directory = dest_directory
        self.dds_connection = project.dds_connection
        self.project = project
        self.num_workers = num_workers
        self.page_size = GET_PAGE_SIZE_DEFAULT
        self.async_download_results = []
        self.message_queue = multiprocessing.Manager().Queue()
        self.files_downloaded = 0
        self.files_to_download = None
        self.file_download_state = {}
        self.download_status_list = []

    def set_files_to_download(self, num_files):
        self.files_to_download = num_files
        self.show_progress_bar()

    def run(self):
        with multiprocessing.Pool(self.num_workers) as pool:
            for project_file in self._get_project_files():
                self._download_file(pool, project_file)
                while self._work_queue_is_full():
                    self._wait_for_and_retry_failed_downloads(pool)
            while self._work_queue_is_not_empty():
                self._wait_for_and_retry_failed_downloads(pool)
        self._print_downloaded_files_status()

    def _print_downloaded_files_status(self):
        print("Verifying contents of {} downloaded files using file hashes.".format(self.files_to_download))
        all_good = True
        for download_status in self.download_status_list:
            if not download_status.has_a_valid_hash():
                all_good = False
            print(download_status.get_status_line())
        if all_good:
            print("All downloaded files have been verified successfully.")
        else:
            raise ValueError("ERROR: Downloaded file(s) do not match the expected hashes.")

    def _get_project_files(self):
        project_files_generator = self.project.get_project_files_generator(self.page_size)
        for project_file, headers in project_files_generator:
            if self.files_to_download is None:
                self.files_to_download = int(headers.get(DDS_TOTAL_HEADER))
            yield project_file

    def _download_file(self, pool, project_file):
        output_path = project_file.get_local_path(self.dest_directory)
        #print("downloading {}".format(output_path))
        os.makedirs(os.path.dirname(output_path), exist_ok=True)
        download_file_dict = self._make_download_file_dict(project_file, output_path)
        async_result = pool.apply_async(download_file, (self.message_queue, download_file_dict,))
        self.async_download_results.append(async_result)

    def _work_queue_is_full(self):
        return len(self.async_download_results) >= self.num_workers

    def _work_queue_is_not_empty(self):
        return len(self.async_download_results) > 0

    def _wait_for_and_retry_failed_downloads(self, pool):
        download_results = self._pop_ready_download_results()
        if download_results:
            self._process_download_results(pool, download_results)
        else:
            try:
                file_id, bytes_downloaded, file_size = self.message_queue.get_nowait()
                # print(file_id, bytes_downloaded, file_size)
                self.file_download_state[file_id] = (bytes_downloaded, file_size)
                self.show_progress_bar()
            except queue.Empty:
                pass
            time.sleep(0)  # Pause to give up CPU since nothing is ready

    def show_progress_bar(self):
        downloaded_files, download_percent = self.get_downloaded_files_and_percent()
        sys.stdout.write("\rDownloaded {:.0f}% - {} of {} files".format(download_percent, downloaded_files,
                                                                        self.files_to_download))

    def get_downloaded_files_and_percent(self):
        parts_per_file = 100
        downloaded_files = 0
        parts_to_download = float(self.files_to_download * parts_per_file)
        parts_downloaded = 0
        for file_id, download_info in self.file_download_state.items():
            bytes_downloaded, file_size = download_info
            if bytes_downloaded == file_size:
                downloaded_files += 1
                parts_downloaded += parts_per_file
            else:
                parts_downloaded += int(parts_per_file * float(bytes_downloaded / file_size))
        return downloaded_files, float(parts_downloaded/parts_to_download) * 100

    def _pop_ready_download_results(self):
        ready_results = []
        for async_result in self._get_ready_async_results():
            result = async_result.get()
            # retrieve the value from the async result
            ready_results.append(result)
            # remove the async result from the list to watch
            self.async_download_results.remove(async_result)
        return ready_results

    def _get_ready_async_results(self):
        ready_results = []
        for async_result in self.async_download_results:
            if async_result.ready():
                ready_results.append(async_result)
        return ready_results

    def _process_download_results(self, pool, download_results):
        for download_file_dict in download_results:
            if download_file_dict['state'] == 'retry':
                # Refresh url in download_file_dict
                file_id = download_file_dict['file_id']
                print("Retrying {}".format(download_file_dict['output_path']))
                file_download = self.dds_connection.get_file_download(file_id)
                download_file_dict['url'] = file_download.host + file_download.url
                # Re-run download process
                async_result = pool.apply_async(download_file, (self.message_queue, download_file_dict,))
                self.async_download_results.append(async_result)
            elif download_file_dict['state'] == 'error':
                raise ValueError(download_file_dict['msg'])
            else:
                self.files_downloaded += 1
                self.download_status_list.append(download_file_dict['status'])

    @staticmethod
    def _make_download_file_dict(project_file, output_path):
        return {
            'state': 'new',
            'file_id': project_file.id,
            'size': project_file.size,
            'hashes': project_file.hashes,
            'output_path': output_path,
            'url': project_file.file_url['host'] + project_file.file_url['url'],
        }


def download_file(message_queue, download_file_dict):
    file_id = download_file_dict['file_id']
    size = int(download_file_dict['size'])
    hashes = download_file_dict['hashes']
    output_path = download_file_dict['output_path']
    url = download_file_dict['url']
    response = requests.get(url, stream=True)
    try:
        written_size = 0
        response.raise_for_status()
        with open(output_path, "wb") as outfile:
            for chunk in response.iter_content(chunk_size=DOWNLOAD_FILE_CHUNK_SIZE):
                if chunk:  # filter out keep-alive new chunks
                    outfile.write(chunk)
                    written_size += len(chunk)
                    message_queue.put((file_id, written_size, size))
        if written_size != size:
            return download_file_result(state='retry', download_file_dict=download_file_dict)
        message_queue.put((file_id, written_size, size))
        file_hash_status = FileHashStatus.determine_for_hashes(hashes, output_path)
        return download_file_result(state='ok', download_file_dict=download_file_dict, status=file_hash_status)
    except requests.exceptions.HTTPError as error:
        if response.status_code == SWIFT_EXPIRED_STATUS_CODE or response.status_code == S3_EXPIRED_STATUS_CODE:
            return download_file_result(state='retry', download_file_dict=download_file_dict)
        return download_file_result(state='error', download_file_dict=download_file_dict, msg=str(error))


def download_file_result(state, download_file_dict, msg=None, status=None):
    result = dict(download_file_dict)
    result['state'] = state
    if msg:
        result['msg'] = msg
    if status:
        result['status'] = status
    return result
