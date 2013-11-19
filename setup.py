try:
    from setuptools import setup
except ImportError:
    from distutils.core import setup

setup(
    name='hiverunner',
    version='1.0.1',
    author='Belly',
    author_email='hiverunner@bellycard.com',
    packages=['hiverunner',],
    scripts=['bin/hiverunner',],
    url='https://github.com/bellycard/hiverunner',
    license='Apache License 2.0',
    description='Hive Runner is a python script that pulls saved queries from Beeswax, runs the queries on Hive, and stores the results in Memcache.',
    long_description=open('README.txt').read(),
    install_requires=[
        "MySQL-python==1.2.4",
        "hiver==0.2.1",
        "python-memcached==1.53",
        "simplejson==3.3.0",
        "thrift==0.9.1",
        "wsgiref==0.1.2",
    ],
)
