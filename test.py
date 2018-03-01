from ddsc.sdk.client import Client
from ddsc.core.projectdownloader import ProjectDownload, DownloadSettings

project_id = '3719153b-725d-43f7-b19b-dbff200eb794'
client = Client()
project = client.get_project_by_id(project_id)
ds = DownloadSettings(client, '/tmp/tacos')
pd = ProjectDownload(ds, project)
pd.run()

