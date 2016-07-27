"""
Classes for filtering a list of paths based on either a list of paths to include and paths to include.
"""

import os
from ddsc.core.util import FilteredProject


class PathFilter(object):
    """
    Represents a list of path filters that allows a path to be checked via include_path method.
    By default all paths return True. Specify the list to include or exclude by calling set_include_paths or
    set_exclude_paths. Returns which paths have been seen via get_unused_paths method.
    """
    def __init__(self, include_paths, exclude_paths):
        """
        Creates a path filter based on either include_paths or exclude_paths if both are filled in raises error.
        :param include_paths: [str]: list of paths that should be included
        :param exclude_paths: [str]: list of paths that should be excluded
        """
        if include_paths and exclude_paths:
            raise ValueError("Programming Error: Should not specify both include_path and exclude_paths")

        path_filter = IncludeAll()
        if include_paths:
            path_filter = IncludeFilter(include_paths)
        elif exclude_paths:
            path_filter = ExcludeFilter(exclude_paths)

        self.filter = path_filter
        self.seen_paths = set()

    def include_path(self, path):
        """
        Should this path be included based on the include_paths or exclude_paths.
        Keeps track of paths seen to allow finding unused filters.
        :param path: str: remote path to be filtered
        :return: bool: True if we should include the path
        """
        self.seen_paths.add(path)
        return self.filter.include(path)

    def reset_seen_paths(self):
        """
        Clear list of paths seen via include_path method.
        """
        self.seen_paths = set()

    def get_unused_paths(self):
        """
        Returns which include_paths or exclude_paths that were not used via include_path method.
        :return: [str] list of filtering paths that were not used.
        """
        return [path for path in self.filter.paths if path not in self.seen_paths]


class PathFilterUtil(object):
    """
    Utility methods used in building path filtering objects.
    """
    @staticmethod
    def is_child(child_path, parent_path):
        """
        Is parent_path a parent(or grandparent) directory of child_path.
        :param child_path: str: remote file path
        :param parent_path: str: remote file path
        :return: bool: True when parent_path is child_path's parent
        """
        parent_dir = os.path.join(parent_path, '')
        child_dir = os.path.join(child_path, '')
        return child_dir.startswith(parent_dir)

    @staticmethod
    def parent_child_paths(path, some_path):
        """
        Is path a parent of some_path or some_path is a parent of path.
        :param path: str: remote file path
        :param some_path: str: remote file path
        :return: bool: True when they are parents
        """
        return PathFilterUtil.is_child(path, some_path) or PathFilterUtil.is_child(some_path, path)

    @staticmethod
    def strip_trailing_slash(paths):
        """
        Remove trailing slash from a list of paths
        :param paths: [str]: paths to fix
        :return: [str]: stripped paths
        """
        return [path.rstrip(os.sep) for path in paths]


class IncludeAll(object):
    """
    Default filter that includes every path.
    """
    def __init__(self):
        self.paths = []  # filters must have a paths property for get_unused_paths

    """
    Path filter that will include all paths.
    """
    def include(self, some_path):
        return True


class IncludeFilter(object):
    """
    Path filter that will include paths that are parent/children/equal to include_paths.
    """
    def __init__(self, paths):
        self.paths = PathFilterUtil.strip_trailing_slash(paths)

    def include(self, some_path):
        if some_path in self.paths:
            return True
        for path in self.paths:
            if PathFilterUtil.parent_child_paths(path, some_path):
                return True
        return False


class ExcludeFilter(object):
    """
    Path filter that will exclude paths that are children/equal to include_paths.
    """
    def __init__(self, paths):
        self.paths = PathFilterUtil.strip_trailing_slash(paths)

    def include(self, some_path):
        if some_path in self.paths:
            return False
        for path in self.paths:
            if PathFilterUtil.is_child(some_path, path):
                return False
        return True


class PathFilteredProject(object):
    """
    Lets visitor visit only nodes in project that are acceptable to a path_filter.
    """
    def __init__(self, path_filter, visitor):
        """
        Setup to allow visitor to visit files/folders from a project that pass path_filter.
        :param path_filter: PathFilter: determines which items are sent to visitor
        :param visitor: object: has methods to visit_project,visit_folder,visit_file that will be called from run method
        """
        self.path_filter = path_filter
        self.visitor = visitor
        self.skipped_folder_paths = set()
        self.filtered_project = FilteredProject(self.include, visitor)

    def run(self, project):
        """
        Walk project calling visit_* methods for visitor for items.
        :param project: RemoteProject: project to visit folders/files of.
        """
        self.filtered_project.walk_project(project)

    def include(self, item):
        """
        Method that determines which items the visitor sees.
        :param item: RemoteProject/RemoteFolder/RemoteItem: item to have it's remote_path checked
        :return: bool: True if the item is to be included
        """
        return self.path_filter.include_path(item.remote_path)



