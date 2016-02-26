"""Duke data service folder upload project."""

import sys
from ddsc.ddsclient import DDSClient, Config


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    config = Config()
    client = DDSClient(config)
    client.run_command(args)

if __name__ == '__main__':
    main()
