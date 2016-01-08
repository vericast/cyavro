# Copyright (c) 2015 MaxPoint Interactive, Inc.
#
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without modification, are permitted provided that the
# following conditions are met:
#
# 1. Redistributions of source code must retain the above copyright notice, this list of conditions and the following
#    disclaimer.
#
# 2. Redistributions in binary form must reproduce the above copyright notice, this list of conditions and the following
#    disclaimer in the documentation and/or other materials provided with the distribution.
#
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote
#    products derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

"""
py.test test fixtures for cyavro
"""

from __future__ import print_function, absolute_import
import numpy as np
import pandas as pd
import cyavro
import avro
import string
import random
import six
import os
import json
import traceback
import sys
import unittest
import pytest

def random_integers(size, dtype, high=10000):
    vals = np.random.randint(low=0, high=high, size=size)
    return vals.astype(dtype)


def random_floats(size, dtype):
    vals = np.random.normal(0, 10000, size=size)
    return vals.astype(dtype)


def make_unicode_alphabet():
    """Generates a list of common unicode ranges that exclude combining code points"""
    include_ranges = [
        (0x0021, 0x0021),
        (0x0023, 0x0026),
        (0x0028, 0x007E),
        (0x00A1, 0x00AC),
        (0x00AE, 0x00FF),
        (0x0100, 0x017F),
        (0x0180, 0x024F),
        (0x2C60, 0x2C7F),
        (0x16A0, 0x16F0),
        (0x0370, 0x0377),
        (0x037A, 0x037E),
        (0x0384, 0x038A),
        (0x038C, 0x038C),
    ]

    return ''.join([
        six.unichr(code_point) for current_range in include_ranges for code_point in range(current_range[0], current_range[1] + 1)
    ])

unicode_alphabet = make_unicode_alphabet()

ascii_alphabet = string.ascii_letters + string.digits + string.punctuation


def get_random_unicode(length):
    return ''.join(random.choice(unicode_alphabet) for i in range(length))


def get_random_ascii(length, ):
    return ''.join(random.choice(ascii_alphabet) for i in range(length))


def random_strings(size, maxlen=128):
    lengths = np.random.randint(0, maxlen, size)
    return np.array(list(map(get_random_ascii, lengths)), dtype=object)


# Python 2 bytes vs str.  In py3 bytes are a real type.
if six.PY2:
    random_bytes = random_strings
else:
    def random_bytes(size, maxlen=128):
        lengths = np.random.randint(0, maxlen, size)
        return np.array([str.encode(x) for x in map(get_random_ascii, lengths)], dtype=object)


def random_unicode(size, maxlen=128):
    lengths = np.random.randint(0, maxlen, size)
    return np.array(list(map(get_random_unicode, lengths)), dtype=object)


class TestBasicTypes(unittest.TestCase):

    """Fixture for nose test of all standard avro types"""

    num_records = 1000
    filename = 'test.avro'

    def setUp(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def tearDown(self):
        if os.path.exists(self.filename):
            os.unlink(self.filename)

    def generic_dataframe(self, df, avro_schema, assert_fns=None):
        """Generic test running function for arbitrary avro schemas.

        Writes a dataframe containing the records to avro.

        Reads back and compares with the original
        """
        print(avro_schema)

        cyavro.write_avro_file_from_dataframe(df, self.filename,
                                              json.dumps(avro_schema),
                                              codec='null'
                                              )

        if assert_fns is None:
            assert_fns = {}

        df_read = cyavro.read_avro_file_as_dataframe(self.filename)

        import avro.schema
        from avro.datafile import DataFileReader, DataFileWriter
        from avro.io import DatumReader, DatumWriter

        with open(self.filename, 'rb') as fo:
            reader = DataFileReader(fo, DatumReader())
            records = []
            for user in reader:
                records.append(user)
            df_reference = pd.DataFrame(records)
            reader.close()

        success = True

        for col in avro_schema["fields"]:
            colname = col['name']
            assert_fn = assert_fns.get(colname, np.testing.assert_array_equal)

            def print_fail_header(s):
                print('#' * len(s))
                print("FAIL: Column {}".format(col))
                print('#' * len(s))
                print(s)

            try:
                assert_fn(df_read[colname], df[colname])
            except AssertionError:
                print_fail_header("Failed for cyavro read comparison  {}\n".format(col))
                traceback.print_exc(file=sys.stdout)
                success = False

            try:
                assert_fn(df_reference[colname], df[colname])
            except AssertionError:
                print_fail_header("Failed for cyavro write comparison {}\n".format(col))
                traceback.print_exc(file=sys.stdout)
                success = False

        assert success

    def generic_simple(self, data, avro_type, assert_fn=np.testing.assert_array_equal, schema=None):
        df = pd.DataFrame({'a': data})

        if schema is None:
            schema = {
                "type": "record",
                "name": "something",
                "fields": [
                    dict(name="a", type=avro_type)
                ]}

        self.generic_dataframe(df, schema, assert_fns={'a': assert_fn})

    def test_int32(self):
        data = random_integers(self.num_records, 'int32')
        self.generic_simple(data, 'int')

    def test_int64(self):
        data = random_integers(self.num_records, 'int64')
        self.generic_simple(data, 'long')

    def test_float32(self):
        data = random_integers(self.num_records, 'float32')
        self.generic_simple(data, 'float', assert_fn=np.testing.assert_array_almost_equal)

    def test_float64(self):
        data = random_integers(self.num_records, 'float64')
        self.generic_simple(data, 'double', assert_fn=np.testing.assert_array_almost_equal)

    def test_string(self):
        data = random_strings(self.num_records)
        self.generic_simple(data, 'string')

    def test_unicode(self):
        data = random_unicode(self.num_records)
        self.generic_simple(data, 'string')

    def test_bytes(self):
        data = random_bytes(self.num_records)
        self.generic_simple(data, 'bytes')

    def test_boolean(self):
        data = random_integers(self.num_records, bool, 2)
        self.generic_simple(data, 'boolean')

    def test_fixed(self):
        import uuid
        data = np.array([uuid.uuid4().bytes for i in range(self.num_records)], dtype=object)
        avrotype = {"type": "fixed", "size": 16, "name": "uuid"}
        self.generic_simple(data, avrotype)

    def test_subrecord(self):
        k = random_integers(self.num_records, 'int32')
        v = random_strings(self.num_records)

        data = [{"k": k, "v": v} for k, v in zip(k, v)]

        df = pd.DataFrame({'a': data})

        schema = {
            "type": "record",
            "name": "something",
            "fields": [
                {
                    "name": "a",
                    "type": {
                        "type": "record",
                        "name": "subrecord",
                        "fields": [
                            {"name": "k", "type": "int"},
                            {"name": "v", "type": "string"},
                        ]
                    }
                }
            ]}

        self.generic_dataframe(df, schema)

    def test_map(self):

        data = [{k: v for k, v in zip(random_strings(10), random_integers(10, 'int32'))}
                for i in range(self.num_records)]

        df = pd.DataFrame({'a': data})

        schema = {
            "type": "record",
            "name": "something",
            "fields": [
                    {
                        "name": "a",
                        "type": {
                            "type": "map",
                            "values": "int"
                        }
                    }
            ]
        }

        self.generic_dataframe(df, schema)

    def test_array(self):

        data = [[v for v in random_integers(10, 'int32')] for i in range(self.num_records)]

        df = pd.DataFrame({'a': data})

        schema = {
            "type": "record",
            "name": "something",
            "fields": [
                    {
                        "name": "a",
                        "type": {
                            "type": "array",
                            "items": "int"
                        }
                    }
            ]
        }
        self.generic_dataframe(df, schema)

if __name__ == '__main__':
    unittest.main()
