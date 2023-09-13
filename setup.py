#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.md') as readme_file:
    readme = readme_file.read()

with open('HISTORY.md') as history_file:
    history = history_file.read()

description = "A simple utility which will parse Instabase Flow Logs"

requirements = ['Jinja2==3.1.2',
                'numpy==1.25.2',
                'openpyxl==3.1.2',
                'pandas==2.1.0']

setup_requirements = []

test_requirements = []

setup(
    author="Josh Bronikowski",
    author_email='josh.bronikowski@instabase.com',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Programming Language :: Python :: 3.9',
        'Programming Language :: Python :: 3.10',
    ],
    description=description,
    entry_points={
        'console_scripts': [
            'ibflowparser=flowparser.parser:main',
        ],
    },
    install_requires=requirements,
    license="MIT license",
    long_description=readme + '\n\n' + history,
    include_package_data=True,
    keywords='ibflowparser',
    name='ibflowparser',
    packages=find_packages(),
    setup_requires=setup_requirements,
    test_suite='tests',
    tests_require=test_requirements,
    url='https://github.com/instabase/flow-parser',
    version='0.0.2',
    zip_safe=False,
)