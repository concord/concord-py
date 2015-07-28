#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call


reqs=[
    'thrift==0.9.2',
    'argparse==1.2.1',
    'six==1.9.0',
    'wsgiref==0.1.2',
    'zope.interface==4.1.2'
]

setup(version='0.2.2',
      name='concord-py',
      description='python concord command line tools',
      scripts=[],
      author='concord systems',
      author_email='hi@concord.io',
      packages=find_packages(),
      url='http://concord.io',
      install_requires=reqs,
      test_suite="tests",
)
