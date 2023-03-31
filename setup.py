from setuptools import setup
import os

with open("README.md", "r") as fh:
    long_description = fh.read()

library_folder = os.path.dirname(os.path.realpath(__file__))

requirementPath = f'{library_folder}/requirements.txt'
install_requires = []
if os.path.isfile(requirementPath):
    with open(requirementPath) as f:
        install_requires = f.read().splitlines()


setup(
    name='aqeea',
    version='0.1-b',
    author="Jean-Marie Lepioufle",
    author_email="jm@jeanmarie.eu",
    packages=[
        'aqeea'],
    license='MIT + Copyright Jean-Marie Lepioufle',
    description='Access to air quality data from the European Environment Agency.',
    long_description = long_description,
    url="https://github.com/j-m-l-e-u/aqeea_pkg",
    python_requires='>=3.8',
    install_requires=install_requires,
    extras_require={
        "examples": ["ipython", "jupyter", "os", "osmnx"],
    })
