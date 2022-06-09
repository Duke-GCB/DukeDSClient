"""Duke data service command line project management utility."""
import sys
from ddsc.exceptions import DDSUserException
from ddsc.ddsclient import DDSClient, AZURE_BACKING_STORAGE
from ddsc.config import create_config


class AzureDDSClient(DDSClient):
    def __init__(self):
        super().__init__(backing_storage=AZURE_BACKING_STORAGE)

    def _create_config(self, args):
        azure_container_name = None
        if "azure_container_name" in args:
            azure_container_name = args.azure_container_name
        return create_config(
            allow_insecure_config_file=args.allow_insecure_config_file,
            azure_container_name=azure_container_name
        )


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    client = AzureDDSClient()
    try:
        client.run_command(args)
    except DDSUserException as ex:
        if client.show_error_stack_trace:
            raise
        else:
            error_message = '\n{}\n'.format(str(ex))
            sys.stderr.write(error_message)
            sys.exit(2)


if __name__ == '__main__':
    main()
