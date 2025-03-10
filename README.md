# KUKSA CSV Provider

![kuksa.val Logo](./doc/img/logo.png)

The aim of this script is to provide and replay VSS data into a [KUKSA Databroker](https://github.com/eclipse-kuksa/kuksa-databroker).
Therefore, it takes a CSV-file, containting pre-defined sequences of VSS signals including timing delays, and provides it to the KUKSA Databroker.

## Usage

The provider requires an installation of Python in version 3 and can be executed with the following commands:

```console
pip install -r requirements.txt
python3 provider.py
```

This assumes a running KUKSA Databroker at `127.0.0.1:55555` and a file named `signals.csv` containing the signals to apply. See the section [Arguments](#arguments) for more details on possible arguments and default values.

The provider uses the [KUKSA Python SDK](https://github.com/eclipse-kuksa/kuksa-python-sdk), which you need to install on your system, e.g., by applying the [requirement.txt](requirements.txt) with pip.

## Arguments

You can start the provider with the following arguments on a command line:

| short argument | long argument | environment variable | description | default value |
|---- | ---- | ---- |----- | ----|
|-f| --file | PROVIDER_SIGNALS_FILE | This indicates the CSV-file containing the signals to update in the KUKSA Databroker. | signals.csv |
| -a | --address | KUKSA_DATA_BROKER_ADDR | This indicates the address of KUKSA Databroker to connect to. | 127.0.0.1 |
| -p | --port | KUKSA_DATA_BROKER_PORT | This indicates the port of the KUKSA Databroker to connect to. | 55555 |
| -i | --infinite | PROVIDER_INFINITE | If the flag is set, the provider loops over the file until stopped, otherwise the file gets processed once. | not present/False |
| -l | --log | PROVIDER_LOG_LEVEL | This sets the logging level. Possible values are: DEBUG, INFO, DEBUG, WARNING, ERROR, CRITICAL | WARNING |
|    | --cacertificate | - | Path to root CA. If defined the client will attempt to use a secure connection and identify the server using this certificate. | None |
|    | --tls-server-name | - | TLS server name, may be needed if addressing a server by IP-name. | None |

## CSV File

An example CSV-files is available in [signals.csv](signals.csv) where an example line is:

```csv
current,Vehicle.Speed,48,1
```

The delimiter for the CSV-file is the ',' sign. The first line is interpreted as header and not as data.

Each line in the csv file consists of the following elements:

| header | description | example |
| -- | -----------| --|
| field | indicates whether to update the current value (`current`) or the target value (`target`) for the signal. | current |
| signal | the name of the signal to update | Vehicle.Speed |
| value | the new value | 48 |
| delay | Indicates the time in seconds which the provider shall wait after processing this signal. This way one can emulate the change of signals over time. | 0 |

## TLS

If connecting to a KUKSA Databroker that require a secure connection you must specify which root certificate to
use to identify the Server by the `--cacertificate` argument. If your (test) setup use the KUKSA example
certificates you must give [CA.pem](https://github.com/eclipse/kuksa.val/blob/master/kuksa_certificates/CA.pem)
as root CA. The server name must match the name in the certificate provided by KUKSA.val databroker.
Due to a limitation in the gRPC client, if connecting by IP-address you may need to give a name listed in the certificate
by the `--tls-server-name` argument. The example server certificate lists the names `Server` and `localhost`,
so one of those names needs to be specified if connecting to `127.0.0.1`. An example is shown below:

```console
python provider.py --cacertificate /home/user/kuksa.val/kuksa_certificates/CA.pem --tls-server-name Server
```

## Limitations

* CSV Provider does not support authentication, i.e. it is impossible to communicate with a Databroker that require authentication!

## Recorder

One way to generate a CSV-file for the CSV-provider is to record it from an KUKSA Databroker. This way one can reproduce an interaction of different providers with the KUKSA Databroker. The script in `csv_provider/recorder.py` allows this recording.
An example call, only recording the vehicle speed and width would be:

```console
pip install -r requirements.txt
python3 recorder.py -s Vehicle.Speed Vehicle.Width
```

The recorder supports the following parameters:

| short argument | long argument | description | default value |
|---- | ---- | ----- | ----|
|-f| --file | This indicates the filename to which to write the VSS-signals. | signalsOut.csv |
|-s| --signals | A list of signals to record. | There is no default value, but the argument is required.|
| -a | --address | This indicates the address of KUKSA Databroker to connect to. | 127.0.0.1 |
| -p | --port | This indicates the port of the KUKSA Databroker to connect to. | 55555 |
| -l | --log | This sets the logging level. Possible values are: DEBUG, INFO, WARNING, ERROR, CRITICAL | INFO |

## Container

CSV-provider is also available as container

```console
docker run -it --rm --net=host ghcr.io/eclipse-kuksa/kuksa-csv-provider/csv-provider:main
```

If the ghcr registry is not easily accessible to you, e.g. if you are a China mainland user, starting from release 0.4.4 we  also made the container images available at quay.io:

```console
docker run -it --rm --net=host quay.io/eclipse-kuksa/csv-provider:main
```

## Pre-commit set up

This repository is set up to use [pre-commit](https://pre-commit.com/) hooks.
Use `pip install pre-commit` to install pre-commit.
After you clone the project, run `pre-commit install` to install pre-commit into your git hooks.
Pre-commit will now run on every commit.
Every time you clone a project using pre-commit running pre-commit install should always be the first thing you do.
