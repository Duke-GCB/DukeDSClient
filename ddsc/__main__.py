"""Duke data service folder upload project."""

import sys
from util import Configuration
from ddsclient import DDSClient


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    config = Configuration(args)
    client = DDSClient(config)
    client.run()

if __name__ == '__main__':
    main()
