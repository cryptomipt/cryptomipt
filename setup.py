from setuptools import setup, find_packages
from os.path import join, dirname

setup(
    name='cryptomipt',
    version='0.1.1',
    packages=['cryptomipt'],
    long_description=open(join(dirname(__file__), 'README.md')).read(),
)