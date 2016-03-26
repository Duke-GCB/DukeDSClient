"""Duke data service command line project management utility."""

import sys

from ddsc.config import create_config
from ddsc.ddsclient import DDSClient


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    config = create_config()
    try:
        client = DDSClient(config)
        client.run_command(args)
    except Exception as ex:
        if config.debug_mode:
            raise
        else:
            sys.stderr.write(str(ex))
            sys.exit(2)

if __name__ == '__main__':
    main()
