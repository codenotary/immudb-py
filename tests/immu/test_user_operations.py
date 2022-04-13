# Copyright 2022 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


import immudb.constants
from immudb.grpc import schema_pb2
import string
import random
import grpc._channel
import google.protobuf.empty_pb2


def get_random_name(length):
    return ''.join(random.choice(string.ascii_lowercase) for i in range(length))


def get_random_string(length):
    return ''.join(random.choice(string.ascii_letters+string.digits) for i in range(length))


class TestUser:
    def test_users_functions(self, client):
        users = client.listUsers()
        assert type(users.userlist.users[0]) == schema_pb2.User

        user = "test_"+get_random_name(8)
        password = "Pw0:"+get_random_string(12)
        database = "defaultdb"
        permission = immudb.constants.PERMISSION_RW

        resp = client.createUser(user, password, permission, database)
        assert type(resp.reply) == google.protobuf.empty_pb2.Empty

        try:
            resp = client.createUser(user, password, permission, database)
            assert False  # it is not allowed to create a user twice
        except grpc.RpcError as e:
            assert e.details() == 'user already exists'

        user1 = "test_"+get_random_name(8)
        password = "Pw0:"+get_random_string(12)
        database = "defaultdb"
        permission = immudb.constants.PERMISSION_RW

        try:
            resp = client.createUser(user1, "12345", permission, database)
            assert False  # it is not allowed to create a trivial password
        except grpc.RpcError as e:
            pass

        try:
            resp = client.createUser(user1, "12345", permission, database)
            assert False  # it is not allowed to create a trivial password
        except grpc.RpcError as e:
            pass

        newPassword = "Pw1:"+get_random_string(12)
        resp = client.changePassword(user, newPassword, password)
        assert type(resp.reply) == google.protobuf.empty_pb2.Empty
