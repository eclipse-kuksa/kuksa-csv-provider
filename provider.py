#! /usr/bin/env python3

########################################################################
# Copyright (c) 2023-2026 Contributors to the Eclipse Foundation
#
# See the NOTICE file(s) distributed with this work for additional
# information regarding copyright ownership.
#
# This program and the accompanying materials are made available under the
# terms of the Apache License 2.0 which is available at
# http://www.apache.org/licenses/LICENSE-2.0
#
# SPDX-License-Identifier: Apache-2.0
########################################################################
'''A provider accepting VSS-signals from a CSV-file
 to write these signals into an Kuksa.val data broker'''

import asyncio
import csv
import argparse
import logging
import os
import sys
from pathlib import Path
from urllib.parse import urlparse

from kuksa_client.v2.aio import KuksaClient
from kuksa_client.v2 import KuksaError


def init_argparse() -> argparse.ArgumentParser:
    '''This inits the argument parser for the CSV-provider.'''
    parser = argparse.ArgumentParser(
        description="This provider writes the content of a csv file to a kuksa.val databroker",
    )
    environment = os.environ
    # DEPRECATED: the -a/-p options are kept for backwards compatibility only.
    # Use the positional server URI (grpc://host:port) instead. They are combined
    # into such an URI by resolve_server_uri() when still used.
    parser.add_argument("server", nargs="?", default=None,
                        help="URI of the kuksa.val databroker to connect to, e.g. grpc://127.0.0.1:55555"
                        " or grpcs://localhost:55555 for a TLS connection."
                        " The default value is grpc://127.0.0.1:55555")
    # DEPRECATED: use the positional server URI instead of -a/-p.
    parser.add_argument("-a", "--address", default=None,
                        help="[DEPRECATED] This indicates the address of the kuksa.val databroker"
                        " to connect to. Use the positional server URI instead,"
                        " e.g. grpc://127.0.0.1:55555")
    # DEPRECATED: use the positional server URI instead of -a/-p.
    parser.add_argument("-p", "--port", default=None, type=int,
                        help="[DEPRECATED] This indicates the port of the kuksa.val databroker"
                        " to connect to. Use the positional server URI instead,"
                        " e.g. grpc://127.0.0.1:55555")
    parser.add_argument("-f", "--file", default=environment.get("PROVIDER_SIGNALS_FILE",
                                                                "signals.csv"),
                        help="This indicates the csv file containing the signals to update in"
                        " the kuksa.val databroker. The default value is signals.csv.")
    parser.add_argument("-i", "--infinite", default=environment.get("PROVIDER_INFINITE"),
                        action=argparse.BooleanOptionalAction,
                        help="If the flag is set, the provider loops"
                        "the file until stopped, otherwise the file gets processed once.")
    parser.add_argument("-l", "--log", default=environment.get("PROVIDER_LOG_LEVEL", "INFO"),
                        help="This sets the logging level. The default value is WARNING.",
                        choices={"INFO", "DEBUG", "WARNING", "ERROR", "CRITICAL"})
    parser.add_argument("--cacertificate",
                        help="Specify the path to your CA.pem. Needed when connecting using a"
                        " grpcs:// URI",
                        nargs='?', default=None)
    parser.add_argument("--tls-server-name",
                        help="TLS server name, may be needed if addressing a server by IP-name",
                        nargs='?', default=None)
    return parser


def resolve_server_uri(parser: argparse.ArgumentParser, args: argparse.Namespace) -> str:
    '''Resolve the broker URI from the positional argument, keeping the deprecated
    -a/-p options working by translating them into a grpc:// URI.'''
    if args.server is not None and (args.address is not None or args.port is not None):
        parser.error("-a/--address and -p/--port are deprecated and cannot be combined"
                     " with the positional server URI. Use only the server URI,"
                     " e.g. grpc://127.0.0.1:55555")
    if args.address is not None or args.port is not None:
        address = args.address or os.environ.get("KUKSA_DATA_BROKER_ADDR", "127.0.0.1")
        port = args.port or int(os.environ.get("KUKSA_DATA_BROKER_PORT", "55555"))
        uri = "grpc://%s:%d" % (address, port)
        message = ("-a/--address and -p/--port are deprecated and will be removed in a future"
                   " release. Use the positional server URI instead, e.g. grpc://127.0.0.1:55555.")
        print("WARNING: %s" % message, file=sys.stderr)
        return uri
    if args.server is not None:
        return args.server
    return os.environ.get("KUKSA_ADDRESS", "grpc://127.0.0.1:55555")


def get_connection_details(parser: argparse.ArgumentParser, server_uri: str,
                           args: argparse.Namespace) -> tuple:
    '''Resolve host, port, root certificate and TLS server name from the server URI.'''
    parts = urlparse(server_uri)
    if parts.scheme.lower() not in ("grpc", "grpcs"):
        parser.error("Unsupported URI scheme %s. Use grpc:// or grpcs://"
                     % (parts.scheme or "(none)"))
    if parts.hostname is None:
        parser.error("No hostname or IP given in server URI")
    try:
        port = parts.port or 55555
    except ValueError:
        parser.error("Invalid port in server URI %s" % parts.netloc)
    root_certificates = Path(args.cacertificate) if args.cacertificate else None
    if parts.scheme.lower() == "grpcs" and root_certificates is None:
        parser.error("TLS cannot be used as no CA Certificate specified."
                     " Provide the --cacertificate argument")
    return parts.hostname, port, root_certificates, args.tls_server_name


async def main():
    '''the main function as entry point for the CSV-provider'''
    parser = init_argparse()
    args = parser.parse_args()
    numeric_value = getattr(logging, args.log.upper(), None)
    server_uri = resolve_server_uri(parser, args)
    host, port, root_path, tls_server_name = get_connection_details(parser, server_uri, args)
    if isinstance(numeric_value, int):
        logging.basicConfig(encoding='utf-8', level=numeric_value)
    try:
        async with KuksaClient(host, port, root_certificates=root_path,
                               tls_server_name=tls_server_name) as client:
            csvfile = open(args.file, newline='', encoding="utf-8")
            signal_reader = csv.DictReader(csvfile,
                                           delimiter=',',
                                           quotechar='|',
                                           skipinitialspace=True)
            logging.info("Starting to apply the signals read from %s.", str(csvfile.name))
            if args.infinite:
                backup = list(signal_reader)
                while True:
                    rows = backup
                    backup = list(rows)
                    await process_rows(client, rows)
            else:
                await process_rows(client, signal_reader)
    except KuksaError as error:
        logging.error("Could not connect to the KUKSA databroker at %s:"
                      "\n %s"
                      "\nMake sure the KUKSA databroker is running and reachable.", server_uri, error)


async def process_rows(client, rows):
    '''Processes a single row from the CSV-file and write the
     recorded signal to the data broker through the client.'''
    for row in rows:
        try:
            if row['field'] == "current":
                # Everything in CSV is a string, so we need to coerce the value to the correct type
                await client.set(await client.coerce_updates({row['signal']: row['value']}))
                logging.info("Update current value of %s to %s", row['signal'], row['value'])
            elif row['field'] == "target":
                # Everything in CSV is a string, so we need to coerce the value to the correct type
                await client.actuate(await client.coerce_updates({row['signal']: row['value']}))
                logging.info("Update target value of %s to %s", row['signal'], row['value'])
        except KuksaError as ex:
            logging.error("Error while updating %s\n%s", row['signal'], ex)

        try:
            await asyncio.sleep(delay=float(row['delay']))
        except ValueError:
            logging.error("Error while waiting for %s seconds after updating %s to %s."
                          " Make sure to only use numbers for the delay value.",
                          row['delay'], row['signal'], row['value'])

asyncio.run(main())
