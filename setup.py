from setuptools import setup
import os 


def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()


setup(
    name='cloudfs_fdw',
    version='1.0.0',
    author='Ernst-Georg Schmid',
    author_email='pgchem@tuschehund.de',
    packages=['cloudfs_fdw'],
    url='https://github.com/ergo70/cloudfs_fdw',
    license='LICENSE.txt',
    description='A foreign data wrapper for accessing csv and JSON files on cloud filesystems.',
    install_requires=["csv","smart_open","ijson","multicorn"],
)
