from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

with open(path.join(here, 'README.rst'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='aioredlock',

    version='0.1.0',

    description='Asyncio implemetation of Redis distributed locks',
    long_description=long_description,

    url='https://github.com/joanvila/aioredlock',

    author='Joan Vilà Cuñat',
    author_email='vila.joan94@gmail.com',

    license='MIT',

    classifiers=[
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 4 - Beta',

        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries :: Python Modules',

        'License :: OSI Approved :: MIT License',

        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
    ],

    keywords='redis redlock distributed locks asyncio',

    packages=find_packages(),

    install_requires=['aioredis']
)
