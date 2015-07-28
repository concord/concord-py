#!/usr/bin/env python
import os
import pip
from setuptools import setup, find_packages
from pip.req import parse_requirements
from subprocess import call


install_reqs = parse_requirements("requirements.txt",
                                  session=pip.download.PipSession())
# reqs is a list of requirement
# e.g. ['django==1.5.1', 'mezzanine==1.4.6']
reqs = [str(ir.req) for ir in install_reqs]


setup(version='0.2.1',
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
