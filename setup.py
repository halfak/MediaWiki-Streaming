import os

from setuptools import find_packages, setup

import mwstreaming


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

def requirements(fname):
    return [line.strip()
            for line in open(os.path.join(os.path.dirname(__file__), fname))]

setup(
    name = "mwstreaming",
    version = "0.5.4",
    author = "Aaron Halfaker",
    author_email = "ahalfaker@wikimedia.org",
    description = "A collection of scripts and utilities to support the " +
                  "stream-processing of MediaWiki data.",
    license = "MIT",
    url = "https://github.com/halfak/MediaWiki-Streaming",
    packages=find_packages(),
    entry_points = {
        'console_scripts': [
            'mwstream=mwstreaming.mwstream:main'
        ],
    },
    long_description = read('README.rst'),
    install_requires = ['docopt', 'deltas', 'yamlconf', 'mediawiki-utilities',
                        'jsonschema', 'stopit'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "Development Status :: 3 - Alpha",
        "License :: OSI Approved :: MIT License",
        "Intended Audience :: Science/Research",
        "Intended Audience :: System Administrators",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Utilities",
        "Topic :: Scientific/Engineering"
    ]
)
