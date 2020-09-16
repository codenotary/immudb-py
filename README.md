# immudb-py [![License](https://img.shields.io/github/license/codenotary/immudb4j)](LICENSE)

[![Build Status](https://travis-ci.com/codenotary/immudb-py.svg?branch=master)](https://travis-ci.com/codenotary/immudb-py)
[![Coverage Status](https://coveralls.io/repos/github/codenotary/immu-py/badge.svg?branch=coverall)](https://coveralls.io/github/codenotary/immu-py?branch=coverall)
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
    pip install git+https://github.com/codenotary/immu-py.git
```

 Then import the client as follows:

```python
    from immudb.client import ImmudbClient
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
    client = ImmudbClient()
```

Setting `immudb` url and port:

```python

    client = ImmudbClient("mycustomurl:someport")
    client = ImmudbClient("10.105.20.32:8899")
```

### User sessions

Use `login` and `logout` methods to initiate and terminate user sessions:

```python
    client.login("usr1", "pwd1");

    // Interact with immudb using logged user

    client.logout();
```
### Encoding

Please note that, in order to provide maximum flexibility, all functions accept byte arrays as parameters. Therefore, unicode strings must be properly encoded.
It is possible to store structured objects, but they must be serialized (e.g., with pickle or json).

### Creating a database

Creating a new database is quite simple:

```python
    client.createDatabase(b"db1");
```

### Setting the active database

Specify the active database with:

```python
    client.useDatabase(b"db1");
```
If not specified, the default databased used is "defaultdb".

### Traditional read and write

immudb provides read and write operations that behave as a traditional
key-value store i.e. no cryptographic verification is done. This operations
may be used when validations can be post-poned:

```python
    client.set(b"k123", b"value123");
    result = client.get(b"k123");
```

### Verified or Safe read and write

immudb provides built-in cryptographic verification for any entry. The client
implements the mathematical validations while the application uses as a traditional
read or write operation:

```python
    try:
        client.safeSet(b"k123", new byte[]{1, 2, 3});
        results = client.safeGet(b"k123");
    Except VerificationException as e:
        # Do something
```

### Multi-key read and write

Transactional multi-key read and write operations are supported by immudb and immupy.
Atomic multi-key write (all entries are persisted or none):

```python
    normal_dictionary = {b"key1": b"value1", b"key2": b"value2"}
    client.setAll(normal_dictionary);
```

Atomic multi-key read (all entries are retrieved or none):

```python
    normal_dictionary = {b"key1": b"value1", b"key2": b"value2"}
    results_dictionary = client.getAll(normal_dictionary.keys())
    # Or manually
    client.get([b"key1", b"key2"])
```

### Closing the client

To programatically close the connection with immudb server use the `shutdown` operation:

```python
    client.shutdown();
```

Note: after shutdown, a new client needs to be created to establish a new connection.

## Contributing

We welcome contributions. Feel free to join the team!

To report bugs or get help, use [GitHub's issues].

[GitHub's issues]: https://github.com/codenotary/immudpy/issues
