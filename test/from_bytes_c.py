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
Tests the `reader_from_bytes_c` cython exposed function
"""
import os
import tempfile

import numpy as np
import pandas as pd
import pandas.util.testing as pdt

import cyavro

import pyximport; pyximport.install()
from cyavro import test_utils

import io


avroschema = """ {"type": "record",
"name": "from_bytes_test",
"fields":[
   {"name": "id", "type": "int"},
   {"name": "name", "type": "string"}
]
}
"""

def prepare_file(fpath):
    tmpdir = tempfile.gettempdir()
    fpath = os.path.join(fpath)
    writer = cyavro.AvroWriter(fpath, 'null', avroschema)

    ids = np.random.randint(100, size=10)
    ids = np.arange(10)
    names = pdt.rands_array(10, 10)
    df_write = pd.DataFrame({"id": ids, "name": names})
    df_write = cyavro.prepare_pandas_df_for_write(df_write, avroschema, copy=False)

    writer.write(df_write)
    writer.close()
    return df_write, fpath

def test_from_bytes():
    df_write, fpath = prepare_file('from_bytes_data.avro')

    reader = test_utils.get_reader(fpath)
    reader.init_buffers()
    df_read = pd.DataFrame(reader.read_chunk())

    pdt.assert_frame_equal(df_write, df_read)
    reader.close()


def test_from_bytes_python():
    df_write, fpath = prepare_file('from_bytes_data_py.avro')

    with open(fpath, 'rb') as fo:
        data = fo.read()

    reader = cyavro.AvroReader()
    reader.init_bytes(data)
    reader.init_reader()
    reader.init_buffers()
    df_read = pd.DataFrame(reader.read_chunk())

    pdt.assert_frame_equal(df_write, df_read)
    reader.close()

def test_from_byesio():
    df_write, fpath = prepare_file('from_bytes_data_py.avro')

    with open(fpath, 'rb') as fo:
        data = fo.read()
        bytesIO = io.BytesIO(data)

    df_read = cyavro.read_avro_bytesio_as_dataframe(bytesIO)
    pdt.assert_frame_equal(df_write, df_read)
