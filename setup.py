from setuptools import setup


setup(name='DukeDSClient',
        version='4.0.0',
        description='Command line tool(ddsclient) to upload/manage projects on the duke-data-service.',
        url='https://github.com/Duke-GCB/DukeDSClient',
        keywords='duke dds dukedataservice',
        author='John Bradley',
        license='MIT',
        packages=['ddsc', 'ddsc.core', 'ddsc.sdk', 'ddsc.azure', 'DukeDS'],
        install_requires=[
          'requests>=2.20.0',
          'PyYAML>=5.1',
          'pytz',
          'future',
          'six',
          'tenacity==6.2.0',
          'tabulate',
          'azure-identity',
          'azure-mgmt-storage',
          'azure-storage-file-datalake',
          'msgraph-core==0.2.2',
        ],
        test_suite='nose.collector',
        tests_require=['nose', 'mock'],
        entry_points={
            'console_scripts': [
                'ddsclient = ddsc.__main__:main',
                'ddd = ddsc.azure.__main__:main'
            ]
        },
        classifiers=[
            'Development Status :: 5 - Production/Stable',
            'Intended Audience :: Developers',
            'Topic :: Utilities',
            'License :: OSI Approved :: MIT License'
        ],
        python_requires='>=3.7',
    )

