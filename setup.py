#!/usr/bin/env python
# -*- coding: utf-8 -*-

from setuptools import setup, find_packages


NAME = "pyflume"
PACKAGE = "pyflumes"
DESCRIPTION = "A Log-collector Tool."
AUTHOR = "Allen Jiang"
AUTHOR_EMAIL = "allenjiang@digiwin.biz"
VERSION = __import__(PACKAGE).__version__


setup(
    name=NAME,
    version=VERSION,
    scripts=['pyflume'],
    description=DESCRIPTION,
    author=AUTHOR,
    author_email=AUTHOR_EMAIL,
    license="BSD",
    packages=find_packages(exclude=["tests.*", "tests"]),
    include_package_data=True,
    platforms="any",
    install_requires=[
    ],
    classifiers=[
        "Environment :: Web Environment",
        "Intended Audience :: Developers",
        "License :: OSI Approved :: BSD License",
        "Operating System :: OS Independent",
        "Programming Language :: Python",
        "Framework :: Flask",
    ],
    zip_safe=False,
)


