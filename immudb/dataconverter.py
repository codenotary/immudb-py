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

import immudb.datatypesv2 as datatypesv2


def convertResponse(fromResponse, toHumanDataClass=True):
    """Converts response from GRPC to python dataclass

    Args:
        fromResponse (GRPCResponse): GRPC response from immudb
        toHumanDataClass (bool, optional): decides if final product should be converted to 'human' dataclass (final product have to override _getHumanDataClass method). Defaults to True.

    Returns:
        DataClass: corresponding dataclass type
    """
    if fromResponse.__class__.__name__ == "RepeatedCompositeContainer":
        all = []
        for item in fromResponse:
            all.append(convertResponse(item))
        return all
    schemaFrom = datatypesv2.__dict__.get(
        fromResponse.__class__.__name__, None)
    if schemaFrom:
        construct = dict()
        for field in fromResponse.ListFields():
            construct[field[0].name] = convertResponse(field[1], False)
        if toHumanDataClass:
            return schemaFrom(**construct)._getHumanDataClass()
        else:
            return schemaFrom(**construct)
    else:
        return fromResponse
