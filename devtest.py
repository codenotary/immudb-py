#!/usr/bin/env python
from immu.client import ImmuClient
a = ImmuClient("localhost:3322")
a.login("immudb","immudb")
