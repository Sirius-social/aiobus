import os
from setuptools import setup, find_packages

HERE = os.path.abspath(os.path.dirname(__file__))

# The text of the README file
with open(os.path.join(HERE, "README.md")) as fid:
    README = fid.read()


setup(
    name="aiobus",
    version="1.0.1",
    description="An event-bus application layer, supports redis",
    long_description=README,
    long_description_content_type="text/markdown",
    url="https://github.com/Sirius-social/aiobus",
    author="Networks Synergy",
    author_email="support@socialsirius.com",
    license="Apache License",
    maintainer=', '.join(('Pavel Minenkov <minikspb@gmail.com>',)),
    classifiers=[
        "License :: OSI Approved :: Apache Software License",
        'Intended Audience :: Developers',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: POSIX',
        'Operating System :: MacOS :: MacOS X',
        'Operating System :: Microsoft :: Windows',
    ],
    packages=[],
    python_requires='>=3.5',
    include_package_data=True,
    install_requires=[
        'aioredis>=1.3.1'
    ]
)
