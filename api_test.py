#!/usr/bin/env python
"""
Allows for generic command line testing of the ddsapi.

For example to get a list of project names:

python api_test.py get_projects | grep description

Run without any arguments to get a list of what arguments are possible:

python api_test.py

"""
import sys
import yaml
import inspect
from ddsc.config import create_config
from ddsc.core.remotestore import RemoteStore


def convertNoneStr(arg):
    if arg == 'None':
        return None
    return arg


def main(args=None):
    if args is None:
        args = sys.argv[1:]
    args = [convertNoneStr(arg) for arg in args]
    config = create_config()
    remote_store = RemoteStore(config)
    ds = remote_store.data_service

    if not args:
        print ("Data service methods:")
        for method_name in get_public_methods(ds):
            method = getattr(ds, method_name)
            args = [arg for arg in inspect.getargspec(method).args if arg != 'self']
            print('{} {}'.format(method_name, ' '.join(args)))
        print("")
    else:
        f = getattr(ds, args[0])
        ret = f.__call__(*args[1:])
        result = ret.json()
        print(yaml.safe_dump(result))


def get_public_methods(obj):
    return [method for method in dir(obj) if not method.startswith("_") and callable(getattr(obj, method))]

if __name__ == '__main__':
    main()
