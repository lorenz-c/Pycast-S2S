import os

from setuptools import find_packages, setup

with open("requirements.txt", "r") as f:
    INSTALL_REQUIRES = [rq for rq in f.read().split("\n") if rq != ""]

def read(fname):
    return open(os.path.join(os.path.dirname(__file__), fname)).read()

setup(
    name='Forecast_processing',
    version='0.1',
    authors='Christof Lorenz',
    author_email='christof.lorenz@kit.edu',
    description='Post-processing routines for hydrometeorological subseasonal-to-seasonal forecasts',
    url='https://gitlab.imk-ifu.kit.edu/borkenhagen-c/bias-correction-seas5-py',
    license = "MIT",
    keywords='seasonal forecasts',
    packages=find_packages(exclude=['test']),
    include_package_data=True,
    long_description=read('README.md'),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Topic :: Scientific/Engineering :: Atmospheric Science",
        "License :: OSI Approved :: BSD License",
	    'Programming Language :: Python :: 3.10',
        'Programming Language :: Python :: 3.11',
    ],
    install_requires=INSTALL_REQUIRES,
)
