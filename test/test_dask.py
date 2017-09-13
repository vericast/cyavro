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
@pytest.mark.parametrize('mode', (['scan', 'seek', 'none']))
@pytest.mark.parametrize('bs', ([1024**2, 10*1024**3]))
def test_roundtrip(tempdir, block, mode, bs):
    fn = os.path.join(tempdir, 'test.avro')
    write_avro_file_from_dataframe(data, fn, block_size=block)
    out = dask_reader.read_avro(fn, block_finder=mode).compute()
    pd.util.testing.assert_frame_equal(data, out)


@pytest.mark.parametrize('block', ([1024, 1024**2, 1024**3]))
@pytest.mark.parametrize('mode', (['scan', 'seek', 'none']))
@pytest.mark.parametrize('bs', ([1024**2, 10*1024**3]))
def test_roundtrip_multifile(tempdir, block, mode, bs):
    fn1 = os.path.join(tempdir, 'test1.avro')
    write_avro_file_from_dataframe(data, fn1, block_size=block)
    fn2 = os.path.join(tempdir, 'test2.avro')
    write_avro_file_from_dataframe(data, fn2, block_size=block)
    expected = pd.concat([data, data]).reset_index(drop=True)
    fn = os.path.join(tempdir, 'test*.avro')
    out = dask_reader.read_avro(fn, block_finder=mode).compute()
    out.reset_index(drop=True, inplace=True)  # why does cyavro compute integers?
    pd.util.testing.assert_frame_equal(expected, out)
