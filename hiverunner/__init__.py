#!/usr/bin/env python

# Copyright 2013 Belly, Inc.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at

#   http://www.apache.org/licenses/LICENSE-2.0

# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import argparse
import logging
import memcache
from multiprocessing import Pool
import urllib
import urllib2

import hiver
import MySQLdb as db
import simplejson as json

def push_to_memcache(name, data, address):
    """Push data to the api, which is backed by memcache"""
    logging.info("Pushing results for query %s to memcache" % (name))
    try:
        mc = memcache.Client([address], debug=0)
        mc.set(name, data)
        mc.disconnect_all()
    except Exception as e:
        logging.error("Failed to push results for '%s'" % (name))
        logging.info("Params for failed push '%s': %s" % (name, params))


def hive_worker(params):
    """Connect to hive and run query"""
    # Parse Parameters
    name = params[0]
    data = json.loads(params[1])
    options = params[2]

    # data is a json stored in the beeswax_savedquery table; it contains {"query": {"query": "SELECT ..."}}
    query = data["query"]["query"]

    logging.info("Starting worker for query " + name)

    try:
        # Connect to hive
        client = hiver.connect(options['hive-host'], options['hive-port'])

        # Execute queries from config
        client.execute(options['hive-initial-commands'])

        # Execute main query
        client.execute(query)

        # Get schema
        schema = client.getSchema()
        column_names = [column.name for column in schema.fieldSchemas]

        # First row should be column_names
        output = [column_names]
    except hiver.hive_service.ttypes.HiveServerException as e:
        logging.error("Failed to run hive query " + name + ":" + e.message)
        return

    # Rest of the rows are data
    for row in client.fetchAll():
        output.append(row.split("\t"))

    memcache_address = "%s:%d" % (options['memcache-host'], options['memcache-port']) 
    push_to_memcache(name, json.dumps(output), memcache_address)


def fetch_saved_queries(search_text, db_params):
    """Fetch saved queries from the beeswax database"""
    logging.info("Getting saved queries like '" + search_text + "'")

    connection = db.connect(
        host=db_params['host'],
        port=db_params['port'],
        user=db_params['user'],
        passwd=db_params['password'],
        db=db_params['database']
    )
    cursor = connection.cursor()
    # Query to get saved hive queries out of beeswax
    # The result is a JSON containing the HiveQL Query and meta information
    cursor.execute("SELECT name, data FROM beeswax_savedquery WHERE name COLLATE latin1_general_cs LIKE '" + search_text + "' AND name NOT LIKE '% %' AND is_trashed = 0")
    return list(list(x) for x in cursor.fetchall())

def main():
    # Parse arguments
    parser = argparse.ArgumentParser(description="Run scheduled hive queries, which start with _hourly, _daily, and _weekly")
    parser.add_argument("--verbose", help="Be verbose", action="store_true")
    parser.add_argument("--hourly", help="Run hourly queries", action="store_true")
    parser.add_argument("--daily", help="Run daily queries", action="store_true")
    parser.add_argument("--weekly", help="Run weekly queries", action="store_true")
    parser.add_argument("--custom", help="Run queries with custom like string")
    parser.add_argument("--memcache-host", help="Memcache host, defaults to 127.0.0.1", default="127.0.0.1")
    parser.add_argument("--memcache-port", help="Memcache port, defaults to 11211", type=int, default=11211)
    parser.add_argument("--mysql-host", help="MySQL Host", required=True)
    parser.add_argument("--mysql-port", help="MySQL Port, defaults to 3306", type=int, default=3306)
    parser.add_argument("--mysql-user", help="MySQL User", required=True)
    parser.add_argument("--mysql-password", help="MySQL Password", required=True)
    parser.add_argument("--mysql-database", help="MySQL Database, defaults to beeswax", default="beeswax")
    parser.add_argument("--hive-host", help="Hive Host", required=True)
    parser.add_argument("--hive-port", help="Hive Port, defaults to 10000", type=int, default=10000)
    parser.add_argument("--hive-threads", help="Hive Threads, defaults to 8", type=int, default=8)
    parser.add_argument("--hive-initial-commands", help="Commands to run upon connection to hive, for example 'ADD JAR custom_serde.jar;'")
    args = parser.parse_args()

    # Set up logger
    log_level = logging.INFO if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format='%(asctime)s - %(levelname)s - %(message)s', datefmt="%Y-%m-%d %H:%M:%S")

    # Gather database parameters
    db_params = {
        "host": args.mysql_host,
        "port": args.mysql_port,
        "user": args.mysql_user,
        "password": args.mysql_password,
        "database": args.mysql_database
    }

    # Gather hive queries
    queries = []
    if args.hourly:
        queries.extend(fetch_saved_queries("_hourly%", db_params))
    if args.daily:
        queries.extend(fetch_saved_queries("_daily%", db_params))
    if args.weekly:
        queries.extend(fetch_saved_queries("_weekly%", db_params))
    if args.custom:
        queries.extend(fetch_saved_queries(db.escape_string(args.custom), db_params))

    # Gather hive and memcache parameters
    params = {
        "hive-host": args.hive_host,
        "hive-port": args.hive_port,
        "hive-initial-commands": args.hive_initial_commands,
        "memcache-host": args.memcache_host,
        "memcache-port": args.memcache_port
    }

    # queries is list of lists. In order to properly pass paramm, assign the params dict to the end of each list
    for index, query in enumerate(queries):
        queries[index].append(params)

    # Work on the queries in parallel
    logging.info("Spawning " + str(args.hive_threads) + " workers")
    pool = Pool(processes=args.hive_threads)
    pool.map(hive_worker, queries)


if __name__ == '__main__':
    main()
