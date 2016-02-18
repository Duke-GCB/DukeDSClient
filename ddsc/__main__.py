"""Duke data service folder upload project ."""

import sys
import ddsapi
import uploadtool


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    config = uploadtool.Configuration(args)
    data_service = ddsapi.DataServiceApi(config.auth, config.url)
    tool = uploadtool.ProjectUploadTool(data_service, config.project_name, config.project_desc)
    tool.upload(config.folder)

if __name__ == '__main__':
    main()
