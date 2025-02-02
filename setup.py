# setup.py (minimal)
import setuptools

setuptools.setup(
    name='minimal_test',
    version='0.1',
    install_requires=['apache-beam[gcp]', 'geopy'],
    packages=setuptools.find_packages()
)





"""
#!/usr/bin/python
from setuptools import find_packages
from setuptools import setup

setup(
    name='Coding-Challenge',
    version='1.0',
    install_requires=[
        'apache-beam[gcp]',
        'geopy',
    ],
    packages=find_packages(exclude=['notebooks']),
    py_modules=['config'],
    include_package_data=True,
    description='Coding Challenge'
)
"""