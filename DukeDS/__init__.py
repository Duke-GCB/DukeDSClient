from ddsc.sdk.dukeds import DukeDS
list_projects = DukeDS.list_projects
create_project = DukeDS.create_project
delete_project = DukeDS.delete_project
list_files = DukeDS.list_files
download_file = DukeDS.download_file
upload_file = DukeDS.upload_file
delete_file = DukeDS.delete_file
move_file_or_folder = DukeDS.move_file_or_folder
can_deliver_to_user_with_email = DukeDS.can_deliver_to_user_with_email
can_deliver_to_user_with_username = DukeDS.can_deliver_to_user_with_username

__all__ = ['list_projects', 'create_project', 'delete_project',
           'list_files', 'download_file', 'upload_file', 'delete_file',
           'move_file_or_folder',
           'can_deliver_to_user_with_email', 'can_deliver_to_user_with_username']
