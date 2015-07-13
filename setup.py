from setuptools import setup, find_packages  # Always prefer setuptools over distutils
from codecs import open  # To use a consistent encoding
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'DESCRIPTION.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='concord',
    version='0.0.4',
    description='concord client library for python',
    long_description=long_description,
    url='http://concord.io',
    author='Cole Brown',
    author_email='cole@concord.io',
    license='MIT',
    classifiers=[
        'Development Status :: 4 - Alpha',
        'Intended Audience :: Developers',
        'Topic :: System :: Networking',
        'License :: OSI Approved :: MIT License',
        'Programming Language :: Python :: 2.7',
    ],
    keywords='distributed stream-processing streaming computation systems',
    #packages=find_packages(exclude=['rbonut.thrift']),
    packages=['concord', 'concord.internal', 'concord.internal.thrift'],
    #install_requires=['thrift'],
)

