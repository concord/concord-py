#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call

reqs_file = os.path.join(os.path.dirname(os.path.realpath(__file__))
                   , "requirements.txt")

install_reqs = parse_requirements(reqs_file, session=pip.download.PipSession())


# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]

setup(version='0.3.6',
      name='concord-py',
      description='concord.io python client library',
      scripts=[],
      author='concord systems',
      author_email='hi@concord.io',
      packages=find_packages(),
      url='http://concord.io',
      install_requires=reqs,
      test_suite="tests",
)
