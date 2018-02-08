from setuptools import setup


setup(name='DukeDSClient',
        version='1.0.1',
        description='Command line tool(ddsclient) to upload/manage projects on the duke-data-service.',
        url='https://github.com/Duke-GCB/DukeDSClient',
        keywords='duke dds dukedataservice',
        author='John Bradley',
        license='MIT',
        packages=['ddsc', 'ddsc.core', 'ddsc.sdk', 'DukeDS'],
        install_requires=[
          'requests',
          'PyYAML',
          'pytz',
          'future',
          'six',
        ],
        test_suite='nose.collector',
        tests_require=['nose', 'mock'],
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

