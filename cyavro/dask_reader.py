from __future__ import unicode_literals
from collections import OrderedDict
import io
import json
import numpy as np
import pandas as pd

from dask.bytes.utils import seek_delimiter
from dask.bytes.core import get_fs_paths_myopen
from fastparquet.dataframe import empty
from dask import delayed
from dask.dataframe import from_delayed
import cyavro

MAGIC = b'Obj\x01'
SYNC_SIZE = 16


def read_long(fo):
    """variable-length, zig-zag encoding."""
    c = fo.read(1)
    if not c:
        # end of input
        return 0
    b = ord(c)
    n = b & 0x7F
    shift = 7

    while (b & 0x80) != 0:
        b = ord(fo.read(1))
        n |= (b & 0x7F) << shift
        shift += 7

    return (n >> 1) ^ -(n & 1)


def read_bytes(fo):
    """a long followed by that many bytes of data."""
    size = read_long(fo)
    return fo.read(size)


typemap = {
    'boolean': np.dtype(bool),
    'int': np.dtype('int32'),
    'long': np.dtype('int64'),
    'float': np.dtype('float32'),
    'double': np.dtype('float64')
}


def map_types(header, schema):
    types = OrderedDict()
    for entry in schema['fields']:
        # should bother with root record's name and namespace?
        if 'logicalType' in entry:
            lt = entry['logicalType']
            if lt == 'decimal':
                t = np.dtype('float64')
            elif lt.startswith('time-') or lt =='duration':
                t = np.dtype('timedelta64')
            elif lt.startswith('timestamp-') or lt == 'date':
                t = np.dtype('datetime64')
        else:
            t = typemap.get(entry['type'], np.dtype("O"))
        types[entry['name']] = t
    header['dtypes'] = types


def read_header(fo):
    """Extract an avro file's header

    fo: file-like
        This should be in bytes mode, e.g., io.BytesIO

    Returns dict representing the header
    """
    assert fo.read(len(MAGIC)) == MAGIC, 'Magic avro bytes missing'
    meta = {}
    out = {'meta': meta}
    while True:
        n_keys = read_long(fo)
        if n_keys == 0:
            break
        for i in range(n_keys):
            key = read_bytes(fo).decode('utf8')
            val = read_bytes(fo)
            if key == 'avro.schema':
                val = json.loads(val.decode('utf8'))
                map_types(out, val)
            meta[key] = val
    out['sync'] = fo.read(SYNC_SIZE)
    out['header_size'] = fo.tell()
    fo.seek(0)
    out['head_bytes'] = fo.read(out['header_size'])
    peek_first_block(fo, out)
    return out


def peek_first_block(fo, out):
    fo.seek(out['header_size'])
    out['first_block_count'] = read_long(fo)
    out['first_block_bytes'] = read_long(fo)
    out['first_block_data'] = fo.tell()
    out['blocks'] = [{'offset': out['header_size'],
                      'size': (out['first_block_bytes']
                               + out['first_block_data'] - out['header_size']),
                      'nrows': out['first_block_count']}]


def scan_blocks(fo, header, file_size):
    """Find offsets of the blocks by skipping each block's data.

    Useful where the blocks are large compared to read buffers.
    If blocks are small compared to read buffers, better off searching for the
    sync delimiter.

    Results are attached to the header dict.
    """
    if len(header['blocks']) > 1:
        # already done
        return
    if len(header['blocks']) == 0:
        peek_first_block(fo, header)
    data = header['first_block_data']
    bytes = header['first_block_bytes']
    nrows = header['first_block_count']
    while True:
        off0 = data + bytes
        if off0 + SYNC_SIZE >= file_size:
            break
        fo.seek(off0)
        assert fo.read(SYNC_SIZE) == header['sync'], "Sync failed"
        off = fo.tell()
        num = read_long(fo)
        bytes = read_long(fo)
        data = fo.tell()
        if num == 0 or bytes == 0:
            # can have zero-length blocks??
            raise ValueError("Unexpected end of input")
        header['blocks'].append({'offset': off, 'size': data - off + bytes,
                                 'nrows': num})
        nrows += num
    header['nrows'] = nrows


def read_avro(urlpath, block_finder='auto', blocksize=100000000,
              check_headers=True, **kwargs):
    """Read set of avro files into dask dataframes

    Use this only with avro schema that make sense as tabular data, i.e.,
    not deeply nested with arrays and maps.

    Parameters
    ----------
    urlpath: string
        Absolute or relative filepath, URL (may include protocols like
        ``s3://``), or globstring pointing to data.
    block_finder: auto|scan|seek|none
        Method for chunking avro files.
        - scan: read the first bytes of every block to find the size of all
            blocks and therefore the boundaries
        - seek: find the block delimiter bytestring every blocksize bytes
        - none: do not chunk files, parallelism will be only across files
        - auto: use seek if the first block is larger than 0.2*blocksize, else
            scan.
    check_headers: bool (True)
        Read the small header at the start of every file. If False, the headers
        must be exactly byte-equivalent. This is a useful optimisation,
        especially if block_finder='none'
    """
    if block_finder not in ['auto', 'scan', 'seek', 'none']:
        raise ValueError("block_finder must be in ['auto', 'scan', 'seek',"
                         " 'none'], got %s" % block_finder)
    fs, paths, myopen = get_fs_paths_myopen(urlpath, None, 'rb', None, **kwargs)
    chunks = []
    head = None
    read = delayed(read_avro_bytes)
    for path in paths:
        if head is None:
            # sample first file
            with myopen(path, 'rb') as f:
                head = read_header(f)
        size = fs.size(path)
        b_to_s = blocksize / size
        if block_finder == 'scan' or (block_finder == 'auto' and b_to_s > 0.2):
            with myopen(path, 'rb') as f:
                head['blocks'] = []
                scan_blocks(f, head, size)
                # pick blocks ~blocksize apart, append to chunks
        elif block_finder == 'none':
            chunks.append(read(path, myopen, head['header_size'], size, head,
                               head['head_bytes']))
        else:
            # block case
            loc0 = head['header_size']
            with myopen(path, 'rb') as f:
                while True:
                    f.seek(blocksize, 1)
                    if f.tell() >= size:
                        break
                    seek_delimiter(f, head['sync'])
                    loc = f.tell()
                    chunks.append(read(path, myopen, loc0, loc - loc0,
                                       head, head['head_bytes']))
                    loc0 = loc
    return from_delayed(chunks, meta=head['dtypes'],
                        divisions=[None] * (len(chunks) + 1))


def read_avro_bytes(URL, open_with, start_byte, length, header, head_bytes,
                    nrows=None):
    """Pass a specific file/bytechunk and convert to daatframe with cyavro

    Both a python dict version of the header, and the original bytes that
    define it, are required. The bytes are prepended to the data, so that the
    C avro reader can interpret them.
    """
    with open_with(URL) as f:
        f.seek(start_byte)
        data = head_bytes + f.read(length)
    if nrows is None:
        b = io.BytesIO(data)
        header['blocks'] = []
        scan_blocks(b, header, len(data))
        nrows = sum(b['nrows'] for b in header['blocks'])
    f = cyavro.AvroReader()
    f.init_bytes(data)
    df, arrs = empty(header['dtypes'].values(), nrows, cols=header['dtypes'])
    f.init_reader()
    f.init_buffers(10000)
    for i in range(0, nrows, 10000):
        d = f.read_chunk()
        for c in d:
            df.loc[i:i+10000-1, c] = d[c]
    return df
