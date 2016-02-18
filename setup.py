from setuptools import setup

setup(name='ddsclient',
        version='0.1.0',
        packages=['ddsc'],
        install_requires=[
          'requests',
        ],
        entry_points={
            'console_scripts': [
                'ddsclient = ddsc.__main__:main'
            ]
        },
    )

