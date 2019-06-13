import os
from ddsc.core.util import KindType


class MoveUtil(object):
    def __init__(self, project, source_remote_path, target_remote_path):
        self.project = project
        self.source_remote_path = source_remote_path
        self.target_remote_path = target_remote_path

    def run(self):
        source = self.project.get_child_for_path(self.source_remote_path)
        new_parent = self.get_new_parent()
        new_name = self.get_new_name()
        if new_parent:
            source = source.change_parent(new_parent)
        if new_name:
            source = source.rename(new_name)
        return source

    def get_new_parent(self):
        target = self.project.try_get_item_for_path(self.target_remote_path)
        if target:
            if self.is_folder_or_project(target):
                return target
            else:
                raise ValueError("Cannot move to existing file {}.".format(self.target_remote_path))
        else:
            source_parent_remote_path = os.path.dirname(self.source_remote_path)
            target_parent_remote_path = os.path.dirname(self.target_remote_path)
            if source_parent_remote_path != target_parent_remote_path:
                target_parent = self.project.try_get_item_for_path(target_parent_remote_path)
                if target_parent:
                    if self.is_folder_or_project(target_parent):
                        return target_parent
                    else:
                        raise ValueError("Target parent {} is a file.".format(target_parent_remote_path))
                else:
                    raise ValueError("Target parent directory {} does not exist.".format(target_parent_remote_path))
        return None

    def get_new_name(self):
        source_remote_basename = os.path.basename(self.source_remote_path)
        target_remote_basename = os.path.basename(self.target_remote_path)
        if source_remote_basename != target_remote_basename:
            return target_remote_basename
        else:
            return None

    @staticmethod
    def is_folder_or_project(item):
        return KindType.is_folder(item) or KindType.is_project(item)
