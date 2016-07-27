import sys

TERMINAL_ENCODING_NOT_UTF_ERROR="""
ERROR: DukeDSClient requires UTF terminal encoding.

Follow this guide for adjusting your terminal encoding:
  https://github.com/Duke-GCB/DukeDSClient/blob/master/docs/UnicodeTerminalSetup.md

"""


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
        self.msg_verb = msg_verb

    def transferring_item(self, item, increment_amt=1):
        """
        Update progress that item is about to be transferred.
        :param item: LocalFile, LocalFolder, or LocalContent(project) that is about to be sent.
        :param increment_amt: int amount to increase our count(how much progress have we made)
        """
        percent_done = int(float(self.cnt)/float(self.total) * 100.0)
        name = ''
        if KindType.is_project(item):
            name = 'project'
        else:
            name = item.path
        # left justify message so we cover up the previous one
        message = u'\rProgress: {}% - {} {}'.format(percent_done, self.msg_verb, name)
        self.max_width = max(len(message), self.max_width)
        sys.stdout.write(message.ljust(self.max_width))
        sys.stdout.flush()
        self.cnt += increment_amt

    def finished(self):
        """
        Must be called to print final progress label.
        """
        sys.stdout.write('\rDone: 100%'.ljust(self.max_width) + '\n')
        sys.stdout.flush()

    def show_warning(self, message):
        """
        Shows warnings to the user.
        :param message: str: Message to display
        """
        print(message)


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


class ProjectFilenameList(object):
    """
    Walks a project and saves the project name and filenames to [str] filenames property.
    """
    def __init__(self):
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
        self.details.append("Project {} Contents:".format(item.name))

    def visit_folder(self, item, parent):
        name = self.get_name(item, parent)
        self.id_to_path[item.id] = name
        self.details.append(name)

    def visit_file(self, item, parent):
        name = self.get_name(item, parent)
        self.details.append(name)

    def get_name(self, item, parent):
        if parent:
            parent_name = self.id_to_path.get(parent.id)
            if parent_name:
                return "{}/{}".format(parent_name, item.name)
        return item.name


class ProgressQueue(object):
    """
    Sends tuples over queue for amount processed or an error with a message.
    """
    ERROR = 'error'
    PROCESSED = 'processed'

    def __init__(self, queue):
        self.queue = queue

    def error(self, error_msg):
        self.queue.put((ProgressQueue.ERROR, error_msg))

    def processed(self, amt):
        self.queue.put((ProgressQueue.PROCESSED, amt))

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
            chunk_size = value
            watcher.transferring_item(item, increment_amt=chunk_size)
            size -= chunk_size
        else:
            error_message = value
            for process in processes:
                process.terminate()
            raise ValueError(error_message)
    for process in processes:
        process.join()


def verify_terminal_encoding(encoding):
    """
    Raises ValueError with error message when terminal encoding is not Unicode(contains UTF).
    :param encoding: str: encoding we want to check
    """
    encoding = encoding or ''
    if not ("UTF" in encoding):
        raise ValueError(TERMINAL_ENCODING_NOT_UTF_ERROR)
