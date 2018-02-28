from ddsc.sdk.client import Client
#from ddsc.core.projectdownloader import ProjectDownload

#pd = ProjectDownload(project, '/tmp/taco', None, None)
#pd.make_directories()
##for file_url in project.get_file_urls():
#   print file_url.get_remote_path(), file_url.get_remote_parent_path()

from ddsc.core.projectdownloader import ProjectDownload, DownloadSettings
project_id = '3719153b-725d-43f7-b19b-dbff200eb794'
client = Client()
project = client.get_project_by_id(project_id)
ds = DownloadSettings(client.dds_connection.config, '/tmp/tacos')
pd = ProjectDownload(ds, project)
pd.run()

