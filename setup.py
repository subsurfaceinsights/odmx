#!/usr/bin/env python3

"""
Setup file for the SSI data subpackage in ODMX's python library.
"""

from setuptools import setup


with open('requirements.txt') as f:
    requirements = f.read().splitlines()

setup(
    name='odmx',
    version='1.1.0',
    author='Erek Alper, Doug Johnson, Roelof Versteeg, Rebecca Rubinstein',
    author_email='erek.alper@subsurfaceinsights.com, dougvj@gmail.com, roelof.versteeg@subsurfaceinsights.com, rebecca.rubinstein@subsurfaceinsights.com',
    description='Python library for working with the ODMX data model',
    long_description=open('README.md').read(),
    packages=['odmx', 'odmx.datasources', 'odmx.support'],
    package_data={'' : ['**/*.json', '**/*.sql']},
    include_package_data=True,
    zip_safe=False,
    install_requires=requirements,
    classifiers=[
        'Programming Language :: Python',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.10',
    ],
)
