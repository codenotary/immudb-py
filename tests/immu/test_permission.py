# Copyright 2021 CodeNotary, Inc. All rights reserved.

# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#       http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pytest
from immudb.client import ImmudbClient

from immudb.constants import PERMISSION_RW, PERMISSION_R
from immudb.grpc.schema_pb2 import GRANT, REVOKE

from random import randint

class TestPermission:
    def test_createPermissionOnDatabase(self, client):
        permtestdb1="permtestdb{:04d}".format(randint(0, 10000))
        permtestdb2="permtestdb{:04d}".format(randint(0, 10000))
        permtestuser="permtestuser{:04d}".format(randint(0, 10000))
        
        client.databaseCreate(permtestdb1)
        client.createUser(permtestuser, "Password-2",
                          PERMISSION_RW, permtestdb1)
        client.databaseCreate(permtestdb2)
        client.changePermission(GRANT, permtestuser,
                                permtestdb2, PERMISSION_RW)
        has_permission = False
        for user in client.listUsers().userlist.users:
            if user.user == permtestuser.encode('ascii'):
                for permission in user.permissions:
                    if permission.database == permtestdb2 and permission.permission == PERMISSION_RW:
                        has_permission = True
        assert has_permission

        client.changePermission(REVOKE, permtestuser,
                                permtestdb2, PERMISSION_RW)
        has_permission = False
        for user in client.listUsers().userlist.users:
            if user.user == permtestuser.encode('ascii'):
                for permission in user.permissions:
                    if permission.database == permtestdb2 and permission.permission == PERMISSION_RW:
                        has_permission = True
        assert not has_permission
