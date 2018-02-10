"""
Setup
"""

from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='sulekha_holtwinters',  # Required
    version='1.4.0',  # Required
    description='A Holt Winters implementation Python project',  # Required
    long_description=long_description,  # Optional
    url='https://github.com/sulekhaaloorravi-python/sulekha_holtwinters',  # Optional
    author='Sulekha Aloorravi',  # Optional
    author_email='sulekha.aloorravi@gmail.com',  # Optional
    classifiers=[  # Optional
        'Development Status :: 3 - Alpha',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Researc',
        'Topic :: Scientific/Engineering :: Mathematics'

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7'
    ],


    keywords='Holt Winters Forecasting Experiment',  # Optional

    packages=find_packages(exclude=['']),  # Required

    install_requires=['plotly'],  # Optional

    package_data={  # Optional
        'sulekha_holtwinters': ['data'],
    }
)
