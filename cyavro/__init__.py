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

'''
Wrapper for avro using cython wrapped avrolib

Allows faster reading and writing of avro files.
'''

from __future__ import absolute_import, print_function
from cyavro._cyavro import AvroReader, AvroWriter, prepare_pandas_df_for_write
import pandas as pd
import os as os
import glob as glob
from filelock.filelock import FileLock
import six
import json
try:
    from version import version as __version__
except ImportError:
    pass


DEFAULT_BUFFER_SIZE = 50000
SENTINEL_INT64_NULL = -9223372036854775808
SENTINEL_INT32_NULL = -2147483648


class UnixMilliAvroReader(AvroReader):
    """Helper class to allow reading hints from avro schema for timestamp fields stored as Long

    Parameters
    ----------
    filename : str
    json_avro_schema : dict
    """

    def __init__(self, filename, json_avro_schema=None):
        super(UnixMilliAvroReader, self).__init__(filename)
        if json_avro_schema is not None:
            if isinstance(json_avro_schema, dict) and ('fields' in json_avro_schema):
                self._json_avro_schema = json_avro_schema
        self._json_avro_schema = None

    @property
    def json_avro_schema(self):
        if self._json_avro_schema is None:
            # dependency on the avro python reference implementation since getting full json
            # avro schema from the c-api is elusive
            from avro.datafile import DataFileReader
            from avro.io import DatumReader
            import json
            with open(self.filename) as fo:
                with DataFileReader(fo, DatumReader()) as avf:
                    self._json_avro_schema = json.loads(avf.meta['avro.schema'])
        return self._json_avro_schema

    def read_chunk(self):
        return_dict = super(UnixMilliAvroReader, self).read_chunk()
        # Do some postprocessing, generally dealing with dates
        for f in self.json_avro_schema['fields']:
            if f.get('sql_type', None) == 'TIMESTAMP WITH TIME ZONE':
                return_dict[f['name']] = pd.to_datetime(return_dict[f['name']], utc=True, unit='ms')
        return return_dict


# Global aliases for avro reader / writer classes.  This is to allow for user to change these at runtime to
# user defined subclasses
avro_reader_cls = AvroReader
avro_writer_cls = AvroWriter


def read_avro_file_as_dataframe_iter(path_to_file, buffer_size=DEFAULT_BUFFER_SIZE):
    """Reads an avro file.

    Parameters
    ----------
    path_to_file : string
        /path/to/the/file.avro
    buffer_size : int
        Number of rows to read per block read.

    Returns
    -------
    df : yields :class:`pd.DataFrame`
        A generator yielding :class:`pd.DataFrame` objects for each `buffer_size` chunk
    """
    reader = avro_reader_cls()
    reader.init_file(path_to_file)
    try:
        reader.init_reader()
        reader.init_buffers(buffer_size)
        while True:
            df = pd.DataFrame(reader.read_chunk())
            if len(df):
                yield df
            else:
                break
    finally:
        reader.close()


def read_avro_bytesio_as_dataframe_iter(filelikeobject, buffer_size=DEFAULT_BUFFER_SIZE):
    """Reads an avro file.

    Parameters
    ----------
    path_to_file : string
        /path/to/the/file.avro
    buffer_size : int
        Number of rows to read per block read.

    Returns
    -------
    df : yields :class:`pd.DataFrame`
        A generator yielding :class:`pd.DataFrame` objects for each `buffer_size` chunk
    """
    reader = avro_reader_cls()
    data = filelikeobject.read()
    reader.init_bytes(data)
    try:
        reader.init_reader()
        reader.init_buffers(buffer_size)
        while True:
            df = pd.DataFrame(reader.read_chunk())
            if len(df):
                yield df
            else:
                break
    finally:
        reader.close()




def group_dataframe_iter(iterator, group_by):
    """splits an iterator of dataframes into group chunks.

    This is a useful utility function in conjunction with :meth:`read_avro_path_as_dataframe_iter` as it allows a one
    pass looping over presorted chunked data with keys crossing file boundaries.

    This function will *DISCARD* the index of the dataframes.

    Parameters
    ----------
    iterator : iterator of :class:`pd.DataFrame` objects.
    group_by : first argument to pass to :method:`pd.DataFrame.groupby`

    Returns
    -------
    df : yields :class:`pd.DataFrame`
        A generator yielding :class:`pd.DataFrame` objects for each group obtained by using
        ``df.groupby(group_by, sort=False)``.  This assume that the data in the dataframes is group contiguous.

    Examples
    --------
    # Example usage and behavior
    >>> df = DataFrame({'A' : ['foo', 'bar', 'foo', ],
    ...                 'B' : ['one', 'one', 'two', ],
    ...                 'C' : range(3), 'D' : range(3)})
    ...
    >>> iterator = [df.sort('A', ascending=True), df]
    >>> for x, dff in group_avro_dataframe_iter(iterator, 'A')):
    ...     print x
    ...     print dff
    bar
         A    B  C  D
    0  bar  one  1  1
    foo
         A    B  C  D
    0  foo  one  0  0
    1  foo  two  2  2
    2  foo  one  0  0
    3  foo  two  2  2
    bar
         A    B  C  D
    0  bar  one  1  1


    """
    prev_group = None
    current_group = []
    for df in iterator:
        g = df.groupby(group_by, sort=False)
        for gk, gdf in g:
            if gk == prev_group:
                current_group.append(gdf)
            else:
                yield prev_group, pd.concat(current_group, ignore_index=True)

                # Clear the current group
                del current_group[:]
                prev_group = gk
                current_group.append(gdf)
    yield prev_group, pd.concat(current_group, ignore_index=True)


def read_avro_file_as_dataframe(path_to_file, buffer_size=DEFAULT_BUFFER_SIZE):
    """Reads an avro file.

    Parameters
    ----------
    path_to_file : string
        /path/to/the/file.avro
    buffer_size : int
        Number of rows to read per block read.

    Returns
    -------
    df : `pd.DataFrame`
        A pandas DataFrame of the avro file.
    """
    it = read_avro_file_as_dataframe_iter(path_to_file, buffer_size)
    df = pd.concat([df for df in it], ignore_index=True)
    return df


def read_avro_bytesio_as_dataframe(filelikeobject, buffer_size=DEFAULT_BUFFER_SIZE):
    it = read_avro_bytesio_as_dataframe_iter(filelikeobject, buffer_size)
    df = pd.concat([df for df in it], ignore_index=True)
    return df


def get_avro_files_in_path(path, pattern="*.avro"):
    files = glob.glob(os.path.join(path, pattern))
    return files


def read_avro_path_as_dataframe_iter(path):
    """Read all the avro files in a path

    Parameters
    ----------
    path : string
        /path/to/avro/files

    Returns
    -------
    df : yields `pd.DataFrame`
        An generator yielding a `pd.DataFrame` for each file.
    """
    files = get_avro_files_in_path(path)
    for fn in files:
        yield read_avro_file_as_dataframe(fn)


def read_avro_path_as_dataframe(path):
    """Helper function to read ALL avro files in a path a return a dataframe

    Parameters
    ----------
    path : string
        /path/to/avro/files

    Returns
    -------
    df : `pd.DataFrame`
    """
    return pd.concat(iter(read_avro_path_as_dataframe_iter(path)))


def write_avro_file_from_dataframe(df, path_to_file, schema=None, codec='snappy', block_size=1048576):
    """Writes an avro file from a given dataframe..

    Parameters
    ----------
    df : `pd.DataFrame`
        The dataframe to be written to avro
    path_to_file : string
        /path/to/the/file.avro
    schema : string or dict, optional
        The avro schema to be written.  If a schema is not supplied it will be inferred.
    codec : string
        The compression scheme required {'none', 'snappy', 'deflate'}.

    Returns
    -------
    bool
        True on completion
    """
    if schema is None:
        schema = infer_avro_schema_for_dataframe(df)
    if isinstance(schema, dict):
        schema = json.dumps(schema)
    writer = avro_writer_cls(path_to_file, str(codec), str(schema), block_size)
    try:
        writer.write(df)
    finally:
        writer.close()
    return True


class AvroToHDF(object):
    """Utility namespace class that provides methods to convert Avro to HDF5 for easier use within the
    rest of the python ecosystem.

    This implementation will chunk the avro reads and write these chunks using multiple threads.
    This will not read the entire avro file into memory at once.  This allows for safe use when converting
    large avro files to HDF5.

    Structured as a class so that consumers can subclass this if they wish to override behavior

     - :meth:`avro_file_to_hdf5`
     - :meth:`convert_df_to_hdf5`
     - :meth:`read_avro_file_as_dataframe_iter`

    Examples
    --------
    >>> class SplitByLineItemHDF(AvroToHDF)
    ...
    ...     @classmethod
    ...     def convert_df_to_hdf5(cls, df, hdf_file, table_name):
    ...         for li, subdf in df.groupby('key'):
    ...             super(SplitByLineItemHDF, cls).convert_df_to_hdf5(subdf, hdf_file, '{}_{}'.format(table_name, li))
    ...
    ... SplitByLineItemHDF.avro_path_to_hdf5('.', 'container.h5', 'key', n_jobs=4)
    """

    @classmethod
    def read_avro_file_as_dataframe_iter(cls, avro_file):
        return read_avro_file_as_dataframe_iter(avro_file)

    @classmethod
    def get_avro_files_in_path(path):
        return get_avro_files_in_path(path)

    @classmethod
    def avro_file_to_hdf5(cls, avro_file, hdf_file, table_name):
        """Converts a single avro file to a single hdf5 file.

        Parameters
        ----------
        avro_file : string
            A path to the avro file to be converted.
        hdf_file : string
            Path to the output hdf file.
        table_name : string
            Name of the table to be created in the hdf file.

        Returns
        -------
        bool
            True on completion
        """
        for df in cls.read_avro_file_as_dataframe_iter(avro_file):
                cls.convert_df_to_hdf5(df, hdf_file, table_name)
        return True

    @classmethod
    def convert_df_to_hdf5(cls, df, hdf_file, table_name):
        """Converts a given dataframe to hdf5.

        Parameters
        ----------
        df : `pd.DataFrame`
        hdf_file : string
        table_name : string
        """
        lockfile = hdf_file + '.lock'
        with FileLock(lockfile, timeout=1000000):
            df.to_hdf(hdf_file, table_name, format='table', append=True)

    @classmethod
    def avro_path_to_hdf5(cls, path, hdf_file, table_name, n_jobs=1):
        """Converts avro files to a single hdf5 file.  Optionally using joblib to run this in parallel.

        Parameters
        ----------
        path : string
            A path where the avro files to read are stored.
        hdf_file : string
            Path to the output hdf file.
        table_name : string
            Name of the table to be created in the hdf file.
        n_jobs : integer, optional
            Number of worker jobs to spawn.  If > 1 this will create `n_jobs` processes using `joblib`.
            This requires `joblib` or `sklearn` to be installed.

        Returns
        -------
        bool
            True on completion
        """
        filenames = get_avro_files_in_path(path)
        hdf_file = os.path.abspath(hdf_file)
        # Construct argument list for all jobs
        args = [{
            'avro_file': filename,
            'hdf_file': hdf_file,
            'table_name': table_name
            } for filename in filenames]

        has_joblib = False
        # Import joblib from normal location or from scikit-learn
        try:
            import joblib
            has_joblib = True
        except ImportError:
            try:
                from sklearn.externals import joblib
                has_joblib = True
            except ImportError:
                pass

        if (n_jobs > 1) and has_joblib:
            px = joblib.Parallel(n_jobs=n_jobs)
            px(joblib.delayed(cls.avro_file_to_hdf5)(**a) for a in args)
            return True
        else:
            for a in args:
                cls.avro_file_to_hdf5(**a)
        return True


# Aliases for class methods to simplify the namespace.
avro_file_to_hdf5 = AvroToHDF.avro_file_to_hdf5
avro_path_to_hdf5 = AvroToHDF.avro_path_to_hdf5


def infer_avro_schema_for_dataframe(df):
    """Get the avro json schema for a given pandas DataFrame

    Parameters
    ----------
    df : `pd.DataFrame`

    Returns
    -------
    schema : str
        Inferred avro schema as a json string.
    """
    import itertools
    import numpy as np

    def _get_python_type(iterable):
        types = set(map(type, iterable))
        if len(types) == 1:
            # Still dealing with non-union types
            tp = types.pop()
            if issubclass(tp, six.string_types):
                return '"string"'
            elif issubclass(tp, bytes):
                return '"bytes"'
            elif issubclass(tp, dict):
                # get value element type
                values = (x.itervalues() for x in iterable)
                value_type = _get_python_type(list(itertools.chain.from_iterable(values)))
                return '{{"type": "map", "values": {} }}'.format(value_type)
            elif issubclass(tp, (list, tuple)):
                value_type = _get_python_type(list(itertools.chain.from_iterable(iterable)))
                return '{{"type": "array", "items": {} }}'.format(value_type)
            elif issubclass(tp, int):
                return '"long"'
            elif issubclass(tp, float):
                return '"double"'
            elif issubclass(tp, bool):
                return '"bool"'
            elif issubclass(tp, type(None)):
                return '"null"'
            else:
                raise Exception('Unexpected type found, ' + repr(types))
        else:
            typestrings = []
            for tp in types:
                sublist = filter(lambda x: type(x) == tp, iterable)
                x = _get_python_type(sublist)
                if x == '"null"':
                    typestrings.insert(0, x)
                else:
                    typestrings.append(_get_python_type(sublist))
            if len(typestrings) > 0:
                return '[{}]'.format(','.join(typestrings))
            else:
                return '"null"'

    def _get_avro_dtype(dtype, iterable):
        if dtype == np.int64:
            return '"long"'
        elif dtype == np.int32:
            return '"int"'
        elif dtype == np.float32:
            return '"float"'
        elif dtype == np.float64:
            return '"double"'
        elif dtype == np.bool:
            return '"bool"'
        elif dtype == np.dtype('datetime64[ns]'):
            return '"long"'
        elif dtype == np.object:
            # Special handling for object types
            return _get_python_type(iterable)
        else:
            raise Exception('Unhandled dtype found, ' + repr(dtype))

    dtypes = df.dtypes
    fields = []
    for name, typ in dtypes.iteritems():
        fields.append('{"name":"%(name)s", "type": %(type)s}' % {'name': name, 'type': _get_avro_dtype(typ, df[name])})

    return '''{
    "type": "record",
    "name": "AutoGen",
    "namespace": "com.maxpoint.cyavro.autogenerated",
    "fields": [
        %(fields)s
    ]}''' % {'fields': (',\n' + ' ' * 8).join(fields)}
