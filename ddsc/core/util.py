import sys
import os
import platform
import stat
import time
from ddsc.exceptions import DDSUserException

TERMINAL_ENCODING_NOT_UTF_ERROR = """
ERROR: DukeDSClient requires UTF terminal encoding.

Follow this guide for adjusting your terminal encoding:
  https://github.com/Duke-GCB/DukeDSClient/blob/master/docs/UnicodeTerminalSetup.md

"""

CONFIG_FILE_PERMISSIONS_ERROR = """
ERROR: Your config file ~/.ddsclient permissions can allow other users to see your secret key.
Please remove group and other permissions for your DukeDSClient configuration file.
To do so run the following command:
chmod 600 ~/.ddsclient

"""

REMOTE_PATH_SEP = '/'


class KindType(object):
    """
    The types of items that are part of a project. Strings are from the duke-data-service.
    """
    file_str = 'dds-file'
    folder_str = 'dds-folder'
    project_str = 'dds-project'

    @staticmethod
    def is_file(item):
        return item.kind == KindType.file_str

    @staticmethod
    def is_folder(item):
        return item.kind == KindType.folder_str

    @staticmethod
    def is_project(item):
        return item.kind == KindType.project_str


class NoOpProgressPrinter(object):
    def transferring_item(self, item, increment_amt=1, override_msg_verb=None):
        pass

    def increment_progress(self, increment_amt=1):
        pass

    def finished(self):
        pass


class ProgressPrinter(object):
    """
    Prints a progress bar(percentage) to the terminal, expects to have sending_item and finished called.
    Replaces the same line again and again as progress changes.
    """
    def __init__(self, total, msg_verb):
        """
        Setup printer expecting to have sending_item called total times.
        :param total: int the number of items we are expecting, used to determine progress
        """
        self.total = total
        self.cnt = 0
        self.max_width = 0
        self.waiting = False
        self.msg_verb = msg_verb
        self.progress_bar = ProgressBar()
        self.transferred_bytes = 0
        self.total_bytes = 0

    def transferring_item(self, item, increment_amt=1, override_msg_verb=None, transferred_bytes=0):
        """
        Update progress that item is about to be transferred.
        :param item: LocalFile, LocalFolder, or LocalContent(project) that is about to be sent.
        :param increment_amt: int amount to increase our count(how much progress have we made)
        :param override_msg_verb: str: overrides msg_verb specified in constructor
        """
        self.increment_progress(increment_amt)
        percent_done = int(float(self.cnt) / float(self.total) * 100.0)
        if KindType.is_project(item):
            details = 'project'
        else:
            details = os.path.basename(item.path)
        msg_verb = self.msg_verb
        if override_msg_verb:
            msg_verb = override_msg_verb
        self.transferred_bytes += transferred_bytes
        self.progress_bar.update(percent_done, self.transferred_bytes, '{} {}'.format(msg_verb, details))
        self.progress_bar.show()

    def increment_progress(self, amt=1):
        self.cnt += amt

    def finished(self):
        """
        Must be called to print final progress label.
        """
        self.progress_bar.set_state(ProgressBar.STATE_DONE)
        self.progress_bar.show()

    def show_warning(self, message):
        """
        Shows warnings to the user.
        :param message: str: Message to display
        """
        print(message)

    def start_waiting(self):
        """
        Show waiting progress bar until done_waiting is called.
        Only has an effect if we are in waiting state.
        """
        if not self.waiting:
            self.waiting = True
            wait_msg = "Waiting for project to become ready for {}".format(self.msg_verb)
            self.progress_bar.show_waiting(wait_msg)

    def done_waiting(self):
        """
        Show running progress bar (only has an effect if we are in waiting state).
        """
        if self.waiting:
            self.waiting = False
            self.progress_bar.show_running()


class ProgressBar(object):
    STATE_RUNNING = 'running'
    STATE_WAITING = 'waiting'
    STATE_DONE = 'done'

    def __init__(self):
        self.max_width = 0
        self.percent_done = 0
        self.current_item_details = ''
        self.line = ''
        self.state = self.STATE_RUNNING
        self.wait_msg = 'Waiting'
        self.transferred_bytes = 0
        self.start_time = time.time()

    def update(self, percent_done, transferred_bytes, details):
        self.percent_done = percent_done
        self.current_item_details = details
        self.transferred_bytes = transferred_bytes

    def set_state(self, state):
        self.state = state

    def _get_line(self):
        speed = transfer_speed_str(current_time=time.time(), start_time=self.start_time,
                                   transferred_bytes=self.transferred_bytes)
        if self.state == self.STATE_DONE:
            return 'Done: 100%{}'.format(speed)
        details = self.current_item_details
        if self.state == self.STATE_WAITING:
            details = self.wait_msg
        return 'Progress: {}%{} - {}'.format(self.percent_done, speed, details)

    def show(self):
        line = self._get_line()
        sys.stdout.write(self.format_line(line))
        sys.stdout.flush()
        self.max_width = max(len(line), self.max_width)

    def format_line(self, line):
        justified_line = line.ljust(self.max_width)
        formatted_line = '\r{}'.format(justified_line)
        if self.state == self.STATE_DONE:
            formatted_line += '\n'
        return formatted_line

    def show_running(self):
        """
        Show running progress bar
        """
        self.set_state(ProgressBar.STATE_RUNNING)
        self.show()

    def show_waiting(self, wait_msg):
        """
        Show waiting progress bar until done_waiting is called.
        Only has an effect if we are in waiting state.
        :param wait_msg: str: message describing what we are waiting for
        """
        self.wait_msg = wait_msg
        self.set_state(ProgressBar.STATE_WAITING)
        self.show()


class ProjectWalker(object):
    """
    Generic tool for visiting all the nodes in a project.
    For use with RemoteProject and LocalProject
    """
    @staticmethod
    def walk_project(project, visitor):
        """
        Visit all nodes in the project tree(project, folders, files).
        :param project: LocalProject project we want to visit all children of.
        :param visitor: object must implement visit_project, visit_folder, visit_file
        """
        ProjectWalker._visit_content(project, None, visitor)

    @staticmethod
    def _visit_content(item, parent, visitor):
        """
        Recursively visit nodes in the project tree.
        :param item: LocalContent/LocalFolder/LocalFile we are traversing down from
        :param parent: LocalContent/LocalFolder parent or None
        :param visitor: object visiting the tree
        """
        if KindType.is_project(item):
            visitor.visit_project(item)
        elif KindType.is_folder(item):
            visitor.visit_folder(item, parent)
        else:
            visitor.visit_file(item, parent)
        if not KindType.is_file(item):
            for child in item.children:
                ProjectWalker._visit_content(child, item, visitor)


class FilteredProject(object):
    """
    Adds ability to filter items when a visitor is visiting a project.
    """
    def __init__(self, filter_func, visitor):
        """
        Setup to let visitor walk a project filtering out items based on a function.
        :param filter_func: function(item): returns True to let visitor see the item
        :param visitor: object: object with visit_project,visit_folder,visit_file methods
        """
        self.filter_func = filter_func
        self.visitor = visitor

    def walk_project(self, project):
        """
        Go through all nodes(RemoteProject,RemoteFolder,RemoteFile) in project and send them to visitor if filter allows.
        :param project: RemoteProject: project we will walk
        """
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        if self.filter_func(item):
            self.visitor.visit_project(item)

    def visit_folder(self, item, parent):
        if self.filter_func(item):
            self.visitor.visit_folder(item, parent)

    def visit_file(self, item, parent):
        if self.filter_func(item):
            self.visitor.visit_file(item, parent)


class ProjectDetailsList(object):
    """
    Walks a project and saves the project name and filenames to [str] filenames property.
    """
    def __init__(self, long_format):
        self.long_format = long_format
        self.details = []
        self.id_to_path = {}

    def walk_project(self, project):
        """
        Walks a project and saves the project name and filenames to [str] filenames property.
        :param project: LocalProject project we will read details from.
        """
        # This method will call visit_project, visit_folder, and visit_file below as it walks the project tree.
        ProjectWalker.walk_project(project, self)

    def visit_project(self, item):
        if self.long_format:
            self.details.append("{} - Project {} Contents:".format(item.id, item.name))
        else:
            self.details.append("Project {} Contents:".format(item.name))

    def visit_folder(self, item, parent):
        name = self.get_name(item, parent)
        self.id_to_path[item.id] = name
        if self.long_format:
            self.details.append('{}\t{}'.format(item.id, name))
        else:
            self.details.append(name)

    def visit_file(self, item, parent):
        name = self.get_name(item, parent)
        if self.long_format:
            self.details.append('{}\t{}\t({}:{})'.format(item.id, name, item.hash_alg, item.file_hash))
        else:
            self.details.append(name)

    def get_name(self, item, parent):
        if parent:
            if parent.kind == KindType.project_str:
                return '{}{}'.format(REMOTE_PATH_SEP, item.name)
            parent_name = self.id_to_path.get(parent.id)
            if parent_name:
                return "{}{}{}".format(parent_name, REMOTE_PATH_SEP, item.name)
        return item.name


class ProgressQueue(object):
    """
    Sends tuples over queue for amount processed or an error with a message.
    """
    ERROR = 'error'
    PROCESSED = 'processed'
    START_WAITING = 'start_waiting'
    DONE_WAITING = 'done_waiting'

    def __init__(self, queue):
        self.queue = queue

    def error(self, error_msg):
        self.queue.put((ProgressQueue.ERROR, error_msg))

    def processed(self, amt):
        self.queue.put((ProgressQueue.PROCESSED, amt))

    def start_waiting(self):
        self.queue.put((ProgressQueue.START_WAITING, None))

    def done_waiting(self):
        self.queue.put((ProgressQueue.DONE_WAITING, None))

    def get(self):
        """
        Get the next tuple added to the queue.
        :return: (str, value): where str is either ERROR or PROCESSED and value is the message or processed int amount.
        """
        return self.queue.get()


def wait_for_processes(processes, size, progress_queue, watcher, item):
    """
    Watch progress queue for errors or progress.
    Cleanup processes on error or success.
    :param processes: [Process]: processes we are waiting to finish downloading a file
    :param size: int: how many values we expect to be processed by processes
    :param progress_queue: ProgressQueue: queue which will receive tuples of progress or error
    :param watcher: ProgressPrinter: we notify of our progress:
    :param item: object: RemoteFile/LocalFile we are transferring.
    """
    while size > 0:
        progress_type, value = progress_queue.get()
        if progress_type == ProgressQueue.PROCESSED:
            chunk_size, transferred_bytes = value
            watcher.transferring_item(item, increment_amt=chunk_size, transferred_bytes=transferred_bytes)
            size -= chunk_size
        elif progress_type == ProgressQueue.START_WAITING:
            watcher.start_waiting()
        elif progress_type == ProgressQueue.DONE_WAITING:
            watcher.done_waiting()
        else:
            error_message = value
            for process in processes:
                process.terminate()
            raise DDSUserException(error_message)
    for process in processes:
        process.join()


def verify_terminal_encoding(encoding):
    """
    Raises ValueError with error message when terminal encoding is not Unicode(contains UTF ignoring case).
    :param encoding: str: encoding we want to check
    """
    if encoding and not ("UTF" in encoding.upper()):
        raise DDSUserException(TERMINAL_ENCODING_NOT_UTF_ERROR)


def verify_file_private(filename):
    """
    Raises ValueError the file permissions allow group/other
    On windows this never raises due to the implementation of stat.
    """
    if platform.system().upper() != 'WINDOWS':
        filename = os.path.expanduser(filename)
        if os.path.exists(filename):
            file_stat = os.stat(filename)
            if mode_allows_group_or_other(file_stat.st_mode):
                raise DDSUserException(CONFIG_FILE_PERMISSIONS_ERROR)


def mode_allows_group_or_other(st_mode):
    """
    Returns True if st_mode bitset has group or other permissions
    :param st_mode: int: bit set from a file
    :return: bool: true when group or other has some permissions
    """
    return (st_mode & stat.S_IRWXO or st_mode & stat.S_IRWXG) != 0


class RemotePath(object):
    @staticmethod
    def add_leading_slash(path):
        return '{}{}'.format(REMOTE_PATH_SEP, path)

    @staticmethod
    def strip_leading_slash(path):
        return path.lstrip(REMOTE_PATH_SEP)

    @staticmethod
    def split(remote_path):
        remote_path_no_leading_slash = RemotePath.strip_leading_slash(remote_path)
        return remote_path_no_leading_slash.split(REMOTE_PATH_SEP)


def humanize_bytes(num_bytes):
    """
    Convert a number of bytes to human version
    :param num_bytes: int: bytes to be converted to
    :return: str
    """
    val = num_bytes
    suffix = "B"
    factor = 1000
    if val >= factor:
        val = val / factor
        suffix = "KB"
    if val >= factor:
        val = val / factor
        suffix = "MB"
    if val >= factor:
        val = val / factor
        suffix = "GB"
    val = "{:0.1f} {}".format(val, suffix)
    return val.replace(".0", "")


def plural_fmt(name, cnt):
    """
    pluralize name if necessary and combine with cnt
    :param name: str name of the item type
    :param cnt: int number items of this type
    :return: str name and cnt joined
    """
    if cnt == 1:
        return '{} {}'.format(cnt, name)
    else:
        return '{} {}s'.format(cnt, name)


def join_with_commas_and_and(items):
    if not items:
        return ""
    head_items = items[:-1]
    last_item = items[-1]
    head_items_str = ', '.join(head_items)
    if head_items_str:
        return ' and '.join([head_items_str, last_item])
    else:
        return last_item


def transfer_speed_str(current_time, start_time, transferred_bytes):
    """
    Return transfer speed str based
    :param current_time: float: current time
    :param start_time: float: starting time
    :param transferred_bytes: int: bytes transferred
    :return: str: end user str
    """
    elapsed_seconds = current_time - start_time
    if elapsed_seconds > 0 and transferred_bytes > 0:
        bytes_per_second = float(transferred_bytes) / (elapsed_seconds + 0.5)
        return '@ {}/s'.format(humanize_bytes(bytes_per_second))
    return ''


def boolean_input_prompt(message):
    if sys.version_info >= (3, 0, 0):
        result = input(message)
    else:
        result = input(message)
    result = result.upper()
    return result == "Y" or result == "YES" or result == "T" or result == "TRUE"


def read_argument_file_contents(infile):
    """
    return the contents of a file or "" if infile is None.
    If the infile is STDIN displays a message to tell user how to quit entering data.
    :param infile: file handle to read from
    :return: str: contents of the file
    """
    if infile:
        if infile == sys.stdin:
            print("Enter message and press CTRL-d when done:")
        return infile.read()
    return ""
