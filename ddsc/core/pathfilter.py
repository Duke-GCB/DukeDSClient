import os
from ddsc.core.util import FilteredProject


class PathFilter(object):
    def __init__(self):
        self.filter = IncludeAll()
        self.seen_paths = set()

    @staticmethod
    def create(include_paths, exclude_paths):
        path_filter = PathFilter()
        if include_paths and exclude_paths:
            raise ValueError("Programming Error: Should not specify both include_path and exclude_paths")
        if include_paths:
            path_filter.set_include_paths(include_paths)
        elif exclude_paths:
            path_filter.set_exclude_paths(exclude_paths)
        return path_filter

    @staticmethod
    def strip_trailing_slash(paths):
        return [path.rstrip('/') for path in paths]

    def set_include_paths(self, paths):
        self.filter = IncludeFilter(PathFilter.strip_trailing_slash(paths))

    def set_exclude_paths(self, paths):
        self.filter = ExcludeFilter(PathFilter.strip_trailing_slash(paths))

    def include_path(self, path):
        self.seen_paths.add(path)
        return self.filter.include(path)

    def reset_seen_paths(self):
        self.seen_paths = set()

    def get_unused_paths(self):
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


class IncludeAll(object):
    def __init__(self):
        self.paths = []
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
        self.paths = paths

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
        self.paths = paths

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
        self.path_filter = path_filter
        self.visitor = visitor
        self.skipped_folder_paths = set()
        self.filtered_project = FilteredProject(self.include, visitor)

    def run(self, project):
        self.filtered_project.walk_project(project)

    def include(self, item):
        return self.path_filter.include_path(item.remote_path)



