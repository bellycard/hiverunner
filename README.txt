===========
Hive Runner
===========

Hive Runner is a python script that pulls saved queries from Beeswax, runs the
queries on Hive, and stores the results in Memcache.

Using Hive Runner
=================

Requirements
------------

* Python 2.7
* Cloudera Beeswax - Beeswax must be using a MySQL Database for storage.
* HiveServer - You must be running HiveServer version 1. Note that Cloudera’s
  Hadoop distribution only ships with version 2. You can easily install version
  1 using Cloudera’s package repositories.
* Memcached - You must have Memcached running somewhere.
* Pip - Pip is used for Python package dependency.

Installation
------------

* Optionally, create a VirtualEnv: ``virtualenv environment-name``
* Optionally, use your VirtualEnv: ``source environment-name/bin/activate``
* Install Hive Runner via pip: ``pip install hiverunner``

Usage
-----

Hive Runner has flexible parameters. Available options can be seen by running
``hiverunner --help``. The most important parameters to include when running
Hive Runner from the command line are connection settings.

For example, to run all queries in Beeswax prepended with ``_hourly`` and
caching the results in memcache::

    hiverunner --hourly \
    --mysql-host mysql01.example.com \
    --mysql-database beeswax \
    --mysql-user hue \
    --mysql-password secret \
    --hive-host hive01.example.com \
    --memcache-host cache01.example.com

You can run the same command for all queries prepended with ``_weekly`` simply
by changing the ``hourly`` parameter to ``weekly``::

    hiverunner --weekly \
    --mysql-host mysql01.example.com \
    --mysql-database beeswax \
    --mysql-user hue \
    --mysql-password secret \
    --hive-host hive01.example.com \
    --memcache-host cache01.example.com

If you find that you need to run custom named queries or only a single query the
``custom`` parameter makes this easy. Simply provide the name of the query that
must be run.

For example, to run a single query regardless of the prepended time-focused
demarcation::

    hiverunner --custom _daily_custom_query \
    --mysql-host mysql01.example.com \
    --mysql-database beeswax \
    --mysql-user hue \
    --mysql-password secret \
    --hive-host hive01.example.com \
    --memcache-host cache01.example.com

This format makes it easy to schedule cron jobs.

More Information
================

Hive Runner is open source software and available at
https://github.com/bellycard/hiverunner. Bug reports, feature requests, and
contributions are welcome!

Contributors
============

AJ Self <aj@bellycard.com>

Kevin Reedy <kevin@bellycard.com>

License
=======

Apache License, Version 2.0

http://www.apache.org/licenses/LICENSE-2.0
