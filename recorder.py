#! /usr/bin/env python3

########################################################################
# Copyright (c) 2023 Contributors to the Eclipse Foundation
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
'''A recording writing signals from an instance of the KUKSA.val databroker
 to a CSV-file'''
import argparse
import asyncio
import csv
import logging
import os
import sys
import time
from pathlib import Path
from urllib.parse import urlparse

from kuksa_client.v2.aio import KuksaClient
from kuksa_client.v2 import KuksaError


def init_argparse() -> argparse.ArgumentParser:
    '''This inits the argument parser for the CSV-recorder.'''
    parser = argparse.ArgumentParser(
        description="This provider writes the content of a csv file to a KUKSA.val databroker")
    # DEPRECATED: the -a/-p options are kept for backwards compatibility only.
    # Use the positional server URI (grpc://host:port) instead. They are combined
    # into such an URI by resolve_server_uri() when still used.
    parser.add_argument("server", nargs="?", default=None,
                        help="URI of the KUKSA.val databroker to connect to, e.g. grpc://127.0.0.1:55555"
                        " or grpcs://localhost:55555 for a TLS connection."
                        " The default value is grpc://127.0.0.1:55555")
    # DEPRECATED: use the positional server URI instead of -a/-p.
    parser.add_argument("-a", "--address", default=None,
                        help="[DEPRECATED] This indicates the address of the KUKSA.val databroker"
                        " to connect to. Use the positional server URI instead,"
                        " e.g. grpc://127.0.0.1:55555")
    # DEPRECATED: use the positional server URI instead of -a/-p.
    parser.add_argument("-p", "--port", default=None, type=int,
                        help="[DEPRECATED] This indicates the port of the KUKSA.val databroker"
                        " to connect to. Use the positional server URI instead,"
                        " e.g. grpc://127.0.0.1:55555")
    parser.add_argument("-f", "--file", default="signalsOut.csv", help="This indicates the csv file"
                        " to write the signals to."
                        " The default value is signals.csv.")
    parser.add_argument("-s", "--signals", help="A list of signals to"
                        " record", nargs='+', required=True)
    parser.add_argument("-d", "--with-datatype", action="store_true",
                        help="If set, the VSS datatype for each signal is also recorded.")
    parser.add_argument("-l", "--log", default="INFO", help="This sets the logging level."
                        " The default value is WARNING.",
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
        address = args.address or "127.0.0.1"
        port = args.port or 55555
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
    '''entrypoint to the CSV-recorder'''
    parser = init_argparse()
    args = parser.parse_args()
    for signal in args.signals:
        if "://" in signal:
            parser.error("%s looks like a server URI, not a signal path. Give the server URI"
                         " before the -s argument, e.g. python3 recorder.py %s -s ..."
                         % (signal, signal))
    numeric_value = getattr(logging, args.log.upper(), None)
    if isinstance(numeric_value, int):
        logging.basicConfig(encoding='utf-8', level=numeric_value)
    server_uri = resolve_server_uri(parser, args)
    host, port, root_path, tls_server_name = get_connection_details(parser, server_uri, args)
    try:
        async with KuksaClient(host, port, root_certificates=root_path,
                               tls_server_name=tls_server_name) as client:
            fieldnames = ['field', 'signal', 'value', 'delay']
            if args.with_datatype:
                fieldnames.append('datatype')
            csvfile = open(args.file, 'w', newline='', encoding="utf-8", buffering=1)
            signalwriter = csv.DictWriter(csvfile, fieldnames)
            signalwriter.writeheader()
            previous_time = time.time()
            time_gap = 0.0
            # Expand any "*" widlcards
            logging.debug("Expanding signals %s" % args.signals)
            expanded_signals = []
            for path in args.signals:
                expanded_signals.extend(await client.expand(path))
            # de-duplicate, in case wildcards overlap
            expanded_signals = list(dict.fromkeys(expanded_signals))

            logging.debug(f"Subscribing to expanded signals: {expanded_signals}")

            async for updates in client.subscribe(expanded_signals):
                logging.debug("Received updates: %s" % updates)
                for path, dp in updates.items():
                    if dp.value is None:
                        logging.warning("Received update with no value for signal %s", path)
                        continue
                    current_time = time.time()
                    time_gap = current_time - previous_time
                    previous_time = current_time
                    row = {'field': 'current',
                           'signal': path,
                           'value': dp.value,
                           'delay': time_gap}
                    if args.with_datatype:
                        row['datatype'] = (await client.get_metadata(path)).data_type.name

                    logging.debug("Writing row: %s" % row)
                    signalwriter.writerow(row)
    except KuksaError as error:
        logging.error("Error communicating with KUKSA databroker: %s:", error)

asyncio.run(main())
