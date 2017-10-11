import os
import re
import fnmatch


DDS_IGNORE_FILENAME = '.ddsignore'


class FileFilter(object):
    """
    Provides a function for filtering files based on a regex.
    """
    def __init__(self, file_exclude_regex):
        """
        Set exclusion regex to be used when filtering.
        Pass empty string to include everything.
        :param file_exclude_regex: str: regex that matches files we want to exclude
        """
        if file_exclude_regex:
            self.exclude_regex = re.compile(file_exclude_regex)
        else:
            self.exclude_regex = None

    def include(self, filename, is_file):
        """
        Determines if a file should be included in a project for uploading.
        If file_exclude_regex is empty it will include everything.
        :param filename: str: filename to match it should not include directory
        :param is_file: bool: is this a file if not this will always return true
        :return: boolean: True if we should include the file.
        """
        if self.exclude_regex and is_file:
            if self.exclude_regex.match(filename):
                return False
            return True
        else:
            return True


class FilenamePatternList(object):
    """
    Contains a list of Unix shell-style wildcard patterns to exclude filenames.
    """
    def __init__(self):
        self.regex_list = []

    def add_filename_pattern(self, dir_name, pattern):
        """
        Adds a Unix shell-style wildcard pattern underneath the specified directory
        :param dir_name: str: directory that contains the pattern
        :param pattern: str: Unix shell-style wildcard pattern
        """
        full_pattern = '{}{}{}'.format(dir_name, os.sep, pattern)
        filename_regex = fnmatch.translate(full_pattern)
        self.regex_list.append(re.compile(filename_regex))

    def include(self, path):
        """
        Returns False if any pattern matches the path
        :param path: str: filename path to test
        :return: boolean: True if we should include this path
        """
        for regex_item in self.regex_list:
            if regex_item.match(path):
                return False
        return True


class IgnoreFilePatterns(object):
    """
    Determines if folders/files should be included based on .ddsignore files and config exclude filenames
    """
    def __init__(self, file_filter):
        self.file_filter = file_filter
        self.pattern_list = FilenamePatternList()

    def load_directory(self, top_path, followlinks):
        """
        Traverse top_path directory and save patterns in any .ddsignore files found.
        :param top_path: str: directory name we should traverse looking for ignore files
        :param followlinks: boolean: should we traverse symbolic links
        """
        for dir_name, child_dirs, child_files in os.walk(top_path, followlinks=followlinks):
            for child_filename in child_files:
                if child_filename == DDS_IGNORE_FILENAME:
                    pattern_lines = self._read_non_empty_lines(dir_name, child_filename)
                    self.add_patterns(dir_name, pattern_lines)

    def add_patterns(self, dir_name, pattern_lines):
        """
        Add patterns the should apply below dir_name
        :param dir_name: str: directory that contained the patterns
        :param pattern_lines: [str]: array of patterns
        """
        for pattern_line in pattern_lines:
            self.pattern_list.add_filename_pattern(dir_name, pattern_line)

    @staticmethod
    def _read_non_empty_lines(dir_name, child_filename):
        path = '{}{}{}'.format(dir_name, os.sep, child_filename)
        with open(path, 'r') as infile:
            lines = infile.read().split('\n')
            return [line for line in lines if line]

    def include(self, path, is_file):
        """
        Returns False if any pattern matches the path
        :param path: str: filename path to test
        :return: boolean: True if we should include this path
        """
        return self.pattern_list.include(path) and self.file_filter.include(os.path.basename(path), is_file)
