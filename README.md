# immu-py [![License](https://img.shields.io/github/license/codenotary/immudb4j)](LICENSE)

[![Slack](https://img.shields.io/badge/join%20slack-%23immutability-brightgreen.svg)](https://slack.vchain.us/)
[![Discuss at immudb@googlegroups.com](https://img.shields.io/badge/discuss-immudb%40googlegroups.com-blue.svg)](https://groups.google.com/group/immudb)

## Official [immudb] client for Python.

[immudb]: https://grpc.io/

## Contents

- [Introduction](#introduction)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Supported Versions](#supported-versions)
- [Quickstart](#quickstart)
- [Step by step guide](#step-by-step-guide)
- [Creating a Client](#creating-a-client)

## Introduction

immu-py implements a [grpc] immudb client. A minimalist API is exposed for applications while cryptographic
verifications and state update protocol implementation are fully implemented by this client.
Latest validated immudb state may be kept in the local filesystem when using default `rootService`,
please read [immudb research paper] for details of how immutability is ensured by [immudb].

[grpc]: https://grpc.io/
[immudb research paper]: https://immudb.io/
[immudb]: https://immudb.io/

## Prerequisites

immu-py assumes there is an existing instance of the immudb server up and running. Running `immudb` is quite simple, please refer to the
following link for downloading and running it: https://immudb.io/docs/quickstart.html

## Installation

Install the package using pip:

```shell
    pip install git+https://github.com/codenotary/immu-py.git#egg=immu
```

 Then import the client as follows:

```python
    from immu import ImmuClient
```

Note: immu-py is currently hosted in [Github Packages].

## Supported Versions

immu-py supports the [latest immudb release].

[latest immudb release]: https://github.com/codenotary/immudb/releases/tag/v0.7.1

## Quickstart

[Hello Immutable World!] example can be found in `immudb-client-examples` repo.

[Hello Immutable World!]: https://github.com/codenotary/immudb-client-examples/tree/master/python

## Step by step guide

### Creating a Client

The following code snippets shows how to create a client.

Using default configuration:

```python
    client = ImmuClient("localhost:3322")
```

Setting `immudb` url and port:

```python

    client = ImmuClient("mycustomurl:someport")
    client = ImmuClient("10.105.20.32:8899")
```
