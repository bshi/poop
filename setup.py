#!/bin/env python
from distutils.core import setup

LONG_DESCRIPTION = """This package provides a lightweight framework for writing
hadoop tasks in Python.
"""

setup(
    name='python-poop',
    version='0.1',
    description='Python Hadoop utility',
    long_description=LONG_DESCRIPTION,
    author='Bo Shi',
    author_email='bs1984@gmail.com',
    url='http://www.deadpuck.net/poop-api/',
    license="MIT",
    py_modules = ['poop'],
)
