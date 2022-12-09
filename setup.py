# SPDX-FileCopyrightText: 2022 Karlsruhe Institute of Technology - KIT
#
# SPDX-License-Identifier: MIT

from distutils.util import convert_path

from setuptools import find_packages, setup

with open("README.md", "r") as fh:
    long_description = fh.read()

setup(
    name="pycasts2s",
    version=0.1,
    author="Christof Lorenz",
    author_email="Christof.Lorenz@kit.edu",
    description="",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="TBA",
    packages=find_packages(exclude=("tests", "docs")),
    python_requires=">=3.10",
    install_requires=[
        "pandas",
        "dask",
        "xarray",
        "numpy",
        "netCDF4",
        "scipy",
        "pytest",
        "surpyval",
        "formulaic",
    ],
    license_files=("LICENSE.md", "LICENSES/GPL-3.0-or-later.txt"),
    entry_points={
        "console_scripts": ["mqtt_publish=mqtt_pipeline.fake_publisher:publish", "mqtt_subscribe=mqtt_pipeline.fake_subscriber:subscribe"],
    },
)
