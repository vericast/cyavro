import datetime
import os
import numpy as np
import pandas as pd
import pytest
import shutil
import tempfile

dask = pytest.importorskip('dask')

from cyavro import dask_reader, write_avro_file_from_dataframe


@pytest.yield_fixture
def tempdir():
    d = tempfile.mkdtemp()
    try:
        yield d
    finally:
        shutil.rmtree(d)

N = 1000000
d = np.random.randint(0, 65000, size=N).astype('timedelta64')
t = d + np.array([datetime.datetime.now()]).astype('datetime64[ms]')
data = pd.DataFrame({'a': np.random.randint(0, 65000, size=N),
                     'b': np.random.random(size=N),
                     #'c': d,
                     #'d': t,
                     'e': ['a'] * N})


@pytest.mark.parametrize('block', ([1024, 1024**2, 1024**3]))
def test_roundtrip(tempdir, block):
    fn = os.path.join(tempdir, 'test.avro')
    write_avro_file_from_dataframe(data, fn, block_size=block)
    out = dask_reader.read_avro(fn).compute()
    pd.util.testing.assert_frame_equal(data, out)
