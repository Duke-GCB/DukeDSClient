"""Duke data service command line project management utility."""
import sys
from ddsc.config import create_config
from ddsc.ddsclient import DDSClient

def print_exception_and_exit(ex):
    """
    Print out error message and exit
    :param ex: exception that was raised
    """
    sys.stderr.write(str(ex))
    sys.exit(2)


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    try:
        config = create_config()
    except ValueError as ex:
        print_exception_and_exit(ex)
    try:
        client = DDSClient(config)
        client.run_command(args)
    except Exception as ex:
        if config.debug_mode:
            raise
        else:
            print_exception_and_exit(ex)


if __name__ == '__main__':
    main()
