from setuptools import setup
import sys
import time

PYTHON_VERSION_WARNING = """WARNING: It appears your version of Python does not meet the required minimum of 2.7.9. Some Linux distributions, including Red Hat Enterprise Linux, backport fixes to previous versions, and hence for those lower versions, too, may satisfy the requirements.

"""

if sys.version_info < (2, 7, 9):
    sys.stderr.write(PYTHON_VERSION_WARNING)
    time.sleep(5)

setup(name='DukeDSClient',
        version='0.2.10',
        description='Command line tool(ddsclient) to upload/manage projects on the duke-data-service.',
        url='https://github.com/Duke-GCB/DukeDSClient',
        keywords='duke dds dukedataservice',
        author='John Bradley',
        license='MIT',
        packages=['ddsc','ddsc.core'],
        install_requires=[
          'requests',
          'PyYAML',
        ],
        test_suite='nose.collector',
        tests_require=['nose'],
        entry_points={
            'console_scripts': [
                'ddsclient = ddsc.__main__:main'
            ]
        },
        classifiers=[
            'Development Status :: 3 - Alpha',
            'Intended Audience :: Developers',
            'Topic :: Utilities',
            'License :: OSI Approved :: MIT License',
            'Programming Language :: Python :: 2.7',
            'Programming Language :: Python :: 3.3',
            'Programming Language :: Python :: 3.4',
            'Programming Language :: Python :: 3.5',
        ],
    )

