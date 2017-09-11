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
# 3. Neither the name of the copyright holder nor the names of its contributors may be used to endorse or promote products
#    derived from this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF
# THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

#cython: boundscheck=False
#cython: wraparound=False
#cython: profile=True
#cython: embedsignature=True

"""
This contains the cython based implementation of avro reader and writer classes.

For runtime performance this is compiled without bounds checking or wraparound.

Profile is enabled by default for easier debugging of the code with gdb.
embedsignature allows docstrings to propogate to Python.
"""

from cython.view cimport array as cvarray
from cython.operator cimport dereference as deref
from libc.stdlib cimport malloc, free
import numpy as np
cimport numpy as np
from libc.stdint cimport int32_t, int64_t
from _cavro cimport *
from six import string_types, binary_type, iteritems
from libc.string cimport memcpy

# from posix.stdio cimport *  # New on cython, not yet released
from posixstdio cimport *

# Globals
cdef int64_t PANDAS_NaT = np.datetime64('nat').astype('int64') # Not a time from pandas
# numpy / pandas do not support nullable int32/int64 dtypes.  These values are treated as avro nulls.
cdef int64_t SENTINEL_INT64_NULL = -9223372036854775808
cdef int32_t SENTINEL_INT32_NULL = -2147483648

###################################################################################################
#
#   Reader Methods and classes
#
###################################################################################################

cdef class AvroReader(object):
    """Avro Reader class for reading chunks from an avro file.

    Parameters
    ----------
    filename : str
        Input filename to be read

    Examples
    --------
    >>> rd = AvroReader('/path/to/file.avro')
    >>> rd.init_reader()
    >>> rd.init_buffers(10000)
    >>> while True:
    ...     df = rd.read_chunk()
    ...     ...
    ...     # Do something with df
    ...     ...
    ...     if len(df) == 0:
    ...         break
    ...
    >>> rd.close()

    """

    def __cinit__(self):
        self._reader = NULL
        self.fp_reader_buffer = NULL
        self.fp_reader_buffer_length = 0
        self._should_free_buffer = True

    def __init__(self):
        self.chunk_size = 10000
        self.refholder = []
        self.field_names = []
        self.field_types = []
        self.empty_file = False
        self.reader_type = avro_reader_type_unset
        self.initialized = False

    def init_file(self, str filename):
        self.filename = filename
        self.reader_type = avro_reader_type_file

    def init_bytes(self, bytes data):
        self.filedata = data
        self.filedatalength = len(data) + 1 
        self.reader_type = avro_reader_type_bytes

    def __dealloc__(self):
        # Ensure that we close the reader properly
        cdef avro_file_reader_t filereader
        if self._reader != NULL:
            try:
                filereader = self._reader
                avro_file_reader_close(filereader)
            finally:
                self._reader = NULL
        if self.fp_reader_buffer != NULL:
            free(self.fp_reader_buffer)
            self.fp_reader_buffer = NULL

    def close(self):
        cdef avro_file_reader_t filereader
        if self._reader != NULL:
            filereader = self._reader
            avro_file_reader_close(filereader)
            self._reader = NULL
        if self.fp_reader_buffer != NULL:
            free(self.fp_reader_buffer)
            self.fp_reader_buffer = NULL

    def init_reader(self):
        if self.reader_type == avro_reader_type_file:
            self.init_file_reader()
        elif self.reader_type == avro_reader_type_bytes:
            #raise Exception("DEATH")
            self.init_memory_reader()

    cdef init_memory_reader(self):
        #raise Exception("ASDASD")

        cdef char* cbytes = self.filedata
        cdef int size = len(self.filedata)
        cdef void *dest = malloc(size + 1)
        memcpy(dest, cbytes, size)
        self.fp_reader_buffer = dest
        self.fp_reader_buffer_length = size
        self.init_reader_buffer()

    cdef init_file_reader(self):
        """Initialize the file reader object.  This must be called before calling :meth:`init_buffers`
        """

        cdef avro_file_reader_t filereader
        py_byte_string = self.filename.encode('UTF-8')
        cdef char* c_filename =  py_byte_string

        cdef avro_value_t record
        cdef int rval
        cdef avro_type_t avro_type

        rval = avro_file_reader(c_filename, &filereader)
        if rval != 0:
            avro_error = avro_strerror().decode('UTF-8')
            if 'Cannot read file block count' in avro_error:
                self.empty_file = True
            else:
                raise Exception("Can't read file : {}".format(avro_error))

        self._reader = filereader
        self.initialized = True

    cdef init_reader_buffer(self):
        """Initialize the file reader object based on a File Description using
        avro_file_reader_fp
        """
        cdef FILE* cfile = fmemopen(self.fp_reader_buffer, self.fp_reader_buffer_length, "rb")

        cdef avro_file_reader_t filereader
        cdef rval = avro_file_reader_fp(cfile, "unused", 0, &filereader)

        if rval != 0:
            avro_error = avro_strerror().decode('UTF-8')
            if 'Cannot read file block count' in avro_error:
                self.empty_file = True
            else:
                raise Exception("Can't read file : {}".format(avro_error))
        self._reader = filereader
        self.initialized = True

    def init_buffers(self, size_t chunk_size=0):
        """Initialize the buffers for the reader object.  This must be called before calling :meth:`read_chunk`

        This method also initializes and parses the avro schema for the file in question.

        Parameters
        ----------
        chunk_size : size_t
            Number of records to read in chunk.  This essentially determines how much memory we are allowing the reader
            to use.

        """
        if not self.initialized:
            raise Exception("Reader not initialized")

        # If the file contains nothing: do nothing.
        if self.empty_file:
            return

        if chunk_size != 0:
            self.chunk_size = chunk_size

        cdef:
            avro_file_reader_t filereader = self._reader
            int rval
            avro_schema_t wschema
            avro_value_iface_t *iface
            avro_value_t record
            avro_value_t child
            avro_type_t avro_type
            size_t actual_size
            size_t i
            const char* map_key = NULL
            list thelist
            np.ndarray thearray

        # Retrieve the avro schema stored in the file and get the first record.
        wschema = avro_file_reader_get_writer_schema(filereader)
        iface = avro_generic_class_from_schema(wschema)
        avro_generic_value_new(iface, &record)
        rval = avro_file_reader_read_value(filereader, &record)
        avro_type = avro_value_get_type(&record)

        if avro_type != AVRO_RECORD:
            raise Exception('Non-record types are not supported.')

        # Set up read buffers for each of the avro types.  For numpy types the read buffers are ndarrays, for other
        # types they are python lists
        avro_value_get_size(&record, &actual_size)
        self.buffer_lst = [''] * actual_size

        for i in range(actual_size):
            avro_value_get_by_index(&record, i,  &child, &map_key)
            avro_type = avro_value_get_type(&child)
            using_array = True
            if avro_type in {AVRO_UNION, AVRO_FIXED, AVRO_ARRAY, AVRO_MAP, AVRO_STRING, AVRO_BYTES,
                             AVRO_RECORD, AVRO_NULL}:
                thelist = [None] * self.chunk_size
                self.refholder.append(thelist)
                using_array = False
            elif avro_type == AVRO_INT32:
                thearray = np.empty(shape=(self.chunk_size,), dtype=np.int32)
            elif avro_type == AVRO_INT64:
                thearray = np.empty(shape=(self.chunk_size,), dtype=np.int64)
            elif avro_type == AVRO_FLOAT:
                thearray = np.empty(shape=(self.chunk_size,), dtype=np.float32)
            elif avro_type == AVRO_DOUBLE:
                thearray = np.empty(shape=(self.chunk_size,), dtype=np.float64)
            elif avro_type in {AVRO_BOOLEAN, AVRO_ENUM}:
                thearray = np.empty(shape=(self.chunk_size,), dtype=np.int32)
            else:
                raise Exception('Unexpected type ({})'.format(avro_type))

            self.field_names.append(map_key.decode('utf8'))
            self.field_types.append(avro_type)

            # For faster behavior for numpy compatible types we use ndarrays instead of python lists
            if using_array:
                self.refholder.append(thearray)
                self.buffer_lst[i] = thearray
            else:
                self.buffer_lst[i] = thelist

        # Reference cleanup nanny work
        avro_value_decref(&record)
        avro_value_iface_decref(iface)
        avro_schema_decref(wschema)

        # Reset file reader
        avro_file_reader_close(filereader)
        if self.fp_reader_buffer != NULL:
            self.init_reader_buffer()
        else:
            self.init_reader()

    def read_chunk(self):
        """Reads a chunk of records from the avro file.

        If the avro file is completed being read, this will return an empty dictionary.  It is the responsibility of the
        caller to then call :meth:`close`

        Returns
        -------
        out : dict
            A dictionary mapping avro record field names to a numpy array / list

        Examples
        --------
        >>> reader = AvroReader(...)
        >>> ...
        >>> import pandas as pd
        >>> chunk = pd.DataFrame(reader.read_chunk())

        """
        if self.empty_file:
            return dict()

        cdef:
            size_t counter = 0
            avro_file_reader_t filereader = self._reader
            avro_schema_t wschema
            avro_value_iface_t *iface
            avro_value_t record

        # Get avro reader schema from the file
        wschema = avro_file_reader_get_writer_schema(filereader)
        iface = avro_generic_class_from_schema(wschema)
        avro_generic_value_new(iface, &record)

        while True:
            rval = avro_file_reader_read_value(filereader, &record)
            if rval != 0:
                break
            # decompose record into Python types
            read_record(record, self.buffer_lst, counter)
            avro_value_reset(&record)
            counter += 1
            if counter == self.chunk_size:
                break

        # set sizing for output buffers
        out = dict()
        for name, avro_type, array in zip(self.field_names, self.field_types, self.refholder):
            #print((name, avro_type, type(array)))
            if avro_type == AVRO_BOOLEAN:
                out[name] = array[:counter].astype('bool')
            else:
                out[name] = array[:counter]

        # Reference count cleanup
        avro_value_decref(&record)
        avro_value_iface_decref(iface)
        avro_schema_decref(wschema)

        return out


cdef reader_from_bytes_c(void *buffer, int length):
    """Returns an AvroReader based on a buffer of bytes.
    Useful for other cython extentions that already have a `void *`
    Caller should call `init_buffers`
    """
    reader = AvroReader()
    reader._should_free_buffer = False
    reader.fp_reader_buffer = buffer
    reader.fp_reader_buffer_length = length
    reader.init_reader_buffer()
    return reader


cdef int read_record(const avro_value_t val, list container, size_t row) except -1:
    """Main read function for the root level record type.

    Dispatches to the correct reader function for each of the fields in the record.

    Mutates `container`.

    Parameters
    ----------
    val : avro_value_t
        Record to be read into python types
    container : list of list/array
        List of length `n_fields`.  Each element of this list is another container
    row : size_t
        Row number.  This is used to directly index into the list/array
    """
    cdef:
        size_t i
        list subcontainerlist
        np.ndarray subcontainerarray
        avro_type_t avro_type
        avro_value_t child

    for i in range(len(container)):
        avro_value_get_by_index(&val, i, &child, NULL)
        avro_type = avro_value_get_type(&child)

        if avro_type in {AVRO_INT32, AVRO_INT64, AVRO_FLOAT, AVRO_DOUBLE, AVRO_BOOLEAN, AVRO_ENUM}:
            subcontainerarray = <np.ndarray> container[i]
        else:
            subcontainerlist = <list> container[i]
        if avro_type == AVRO_STRING:
            read_string(child, subcontainerlist, row)
        elif avro_type == AVRO_BYTES:
            read_bytes(child, subcontainerlist, row)
        elif avro_type == AVRO_FIXED:
            read_fixed(child, subcontainerlist, row)
        elif avro_type == AVRO_INT32:
            read_int32(child, subcontainerarray, row)
        elif avro_type == AVRO_INT64:
            read_int64(child, subcontainerarray, row)
        elif avro_type == AVRO_FLOAT:
            read_float32(child, subcontainerarray, row)
        elif avro_type == AVRO_DOUBLE:
            read_float64(child, subcontainerarray, row)
        elif avro_type == AVRO_BOOLEAN:
            read_bool(child, subcontainerarray, row)
        elif avro_type == AVRO_NULL:
            read_null(child, subcontainerlist, row)
        elif avro_type == AVRO_ENUM:
            read_enum(child, subcontainerarray, row)
        elif (avro_type == AVRO_MAP) or (avro_type == AVRO_RECORD):
            read_map(child, subcontainerlist, row)
        elif avro_type == AVRO_ARRAY:
            read_array(child, subcontainerlist, row)
        elif avro_type == AVRO_UNION:
            read_union(child, subcontainerlist, row)
        else:
            raise Exception('Unexpected type ({})'.format(avro_type))

    return 0

# Primitive read functions.  These all take the same basic form
#
# * call the appropriate read method from libavro
# * convert to a python type if needed
# * assign that read value to the subcontainer if present
# * return the value.

cdef read_string(const avro_value_t val, list subcontainer, size_t row):
    cdef:
        size_t strlen
        const char* c_string = NULL
        list l
    avro_value_get_string(&val, &c_string, &strlen)
    if subcontainer is not None:
        l = subcontainer
        l[row] = c_string.decode('utf8')
    else:
        return c_string.decode('utf8')


cdef read_bytes(const avro_value_t val, list subcontainer, size_t row):
    cdef:
        size_t strlen
        const char* c_string = NULL
        list l
        bytes py_bytes
    avro_value_get_bytes(&val, <void**> &c_string, &strlen)
    py_bytes = c_string
    if subcontainer is not None:
        l = subcontainer
        l[row] = py_bytes
    else:
        return py_bytes


cdef read_fixed(const avro_value_t val, list subcontainer, size_t row):
    cdef:
        size_t strlen
        const char* c_string = NULL
        list l
        bytes py_bytes
    avro_value_get_fixed(&val,  <void**> &c_string, &strlen)
    py_bytes = c_string[:strlen]
    if subcontainer is not None:
        l = subcontainer
        l[row] = py_bytes
    else:
        return py_bytes

# numpy primitive read functions.  These all write to ndarrays.

cdef int32_t read_int32(const avro_value_t val, np.ndarray subcontainer, size_t row):
    cdef int32_t out
    avro_value_get_int(&val, &out)
    cdef int32_t[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef double read_int64(const avro_value_t val, np.ndarray subcontainer, size_t row):
    cdef int64_t out
    avro_value_get_long(&val, &out)
    cdef int64_t[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef double read_float64(const avro_value_t val, np.ndarray subcontainer, size_t row):
    cdef double out
    avro_value_get_double(&val, &out)
    cdef double[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef float read_float32(avro_value_t val, np.ndarray subcontainer, size_t row):
    cdef float out
    avro_value_get_float(&val, &out)
    cdef float[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef int32_t read_bool(const avro_value_t val, np.ndarray subcontainer, size_t row):
    """Booleans are stored in an int32_t numpy array.

    uint8 caused annoying issues with memoryviews. This was deemed the lesser evil.
    """
    cdef int32_t out
    cdef int temp
    avro_value_get_boolean(&val, &temp)
    out = temp
    cdef int32_t[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef int read_enum(const avro_value_t val, np.ndarray subcontainer, size_t row):
    cdef int32_t out
    cdef int temp
    avro_value_get_enum(&val, &temp)
    out = temp
    cdef int32_t[: ] arr_view = subcontainer
    if subcontainer is not None:
        arr_view[row] = out
    return out


cdef read_null(const avro_value_t val, list subcontainer, size_t row):
    return None

# Read methods for union types.

cdef read_union(const avro_value_t val, list subcontainer, size_t row):
    """Pick active avro union branch (chosen stored type) and returns value"""
    cdef avro_value_t child
    avro_value_get_current_branch(&val, &child)
    retval = generic_read(child)

    if subcontainer is not None:
        subcontainer[row] = retval

    return retval

# Read methods for complex types. Method sketch
#
# * initialize container
# * for each value in the complex type
#   * dispatch to the appropriate read method
# * assign that read value to the container
# * return the container.

cdef read_map(const avro_value_t val, list subcontainer, size_t row):
    """For map types the value of an avro_value_t is similar to a record.

    These are fetched one at a time since maps are stored as an ordered list of values.
    """
    out  = {}
    if subcontainer is not None:
        subcontainer[row] = out
    cdef size_t actual_size
    cdef size_t i
    avro_value_get_size(&val, &actual_size)
    cdef avro_type_t avro_type

    for i in range(actual_size):
        key, value = read_map_elem(val, i)
        out[key] = value

    return out


cdef read_map_elem(const avro_value_t val, size_t i):
    cdef const char* map_key = NULL
    cdef avro_value_t child
    avro_value_get_by_index(&val, i, &child, &map_key)
    key = map_key.decode('utf8')
    retval = generic_read(child)
    return key, retval



cdef read_record_generic(const avro_value_t val, list subcontainer, size_t row):
    """record types and map types are functionally identical in avro"""
    return read_map(val, subcontainer, row)


cdef read_array(const avro_value_t val, list subcontainer, size_t row):
    out = []
    if subcontainer is not None:
        subcontainer[row] = out
    cdef:
        size_t actual_size
        size_t i
        const char* map_key = NULL
        avro_value_t child

    avro_value_get_size(&val, &actual_size)

    for i in range(actual_size):
        avro_value_get_by_index(&val, i,  &child, &map_key)
        out.append(generic_read(child))

    return out


cdef generic_read(const avro_value_t val):
    """Generic avro type read dispatcher. Dispatches to the various specializations by AVRO_TYPE.

    This is used by the various readers for complex types"""
    cdef avro_type_t avro_type
    avro_type = avro_value_get_type(&val)
    if avro_type == AVRO_STRING:
        return read_string(val, None, 0)
    elif avro_type == AVRO_BYTES:
        return read_bytes(val, None, 0)
    elif avro_type == AVRO_INT32:
        return read_int32(val, None, 0)
    elif avro_type == AVRO_INT64:
        return read_int64(val, None, 0)
    elif avro_type == AVRO_FLOAT:
        return read_float32(val, None, 0)
    elif avro_type == AVRO_DOUBLE:
        return read_float64(val, None, 0)
    elif avro_type == AVRO_BOOLEAN:
        return read_bool(val, None, 0)
    elif avro_type == AVRO_NULL:
        return read_null(val, None, 0)
    elif avro_type == AVRO_ENUM:
        return read_enum(val, None, 0)
    elif avro_type == AVRO_FIXED:
        return read_fixed(val, None, 0)
    elif avro_type == AVRO_MAP:
        return read_map(val, None, 0)
    elif avro_type == AVRO_RECORD:
        return read_record_generic(val, None, 0)
    elif avro_type == AVRO_ARRAY:
        return read_array(val, None, 0)
    elif avro_type == AVRO_UNION:
        return read_union(val, None, 0)
    else:
        raise Exception('Unexpected type ({})'.format(avro_type))


###################################################################################################
#
#   Writer Methods and classes
#
###################################################################################################

# TypeDef for dispatching write methods.  This is used for an array of function pointer for the
# specific writer type
ctypedef void(*write_method_t)(avro_value_t, void *, Py_ssize_t, object)


cdef class AvroWriter(object):
    """
    Class use for writing pandas dataframes to an avro file.

    Parameters
    ----------
    filename : str
        Output filename to be written.
    codec : str
        Codec to use fot the file.  Valid arguments are {'null', 'deflate', 'snappy'}.  This corresponds to which
        compression library the encoder will use.
    schema : str
        Avro Schema as a string.
    block_size : size_t
        Some of the compression libraries allow specifying what the block size to use is.  This specifies the block size
        in bytes.

    Examples
    --------
    >>> writer = AvroWriter('out.avro', 'null', avroschema)
    >>> writer.write(df)
    >>> writer.close()

    """
    cdef avro_file_writer_t _writer
    cdef avro_schema_t _schema
    cdef object schema_json
    cdef int is_closed

    def __init__(self, str filename, str codec, str schema, size_t block_size=0):

        import json
        self.is_closed = 0
        self.schema_json = json.loads(schema)

        py_byte_string = schema.encode('UTF-8')
        cdef:
            char* c_json = py_byte_string
            avro_schema_t writer_schema
            avro_schema_error_t error = NULL
            avro_schema_error_t writer_error

        # Initialize avro writer schema
        if avro_schema_from_json(c_json, len(py_byte_string), &writer_schema, &error) != 0:
            avro_error = avro_strerror().decode('UTF-8')
            raise Exception('Error creating schema : {}'.format(avro_error))
        self._schema = writer_schema

        py_byte_string2 = filename.encode('UTF-8')
        cdef char *c_filename = py_byte_string2

        py_byte_string3 = codec.encode('UTF-8')
        cdef const char *c_codec = py_byte_string3
        cdef avro_file_writer_t writer

        # Creating avro writer
        if avro_file_writer_create_with_codec(c_filename, writer_schema, &writer,
            c_codec, block_size) != 0:
            avro_error = avro_strerror().decode('UTF-8')
            raise Exception('Error creating file writer : {}'.format(avro_error))
        self._writer = writer

    def write(self, df):
        """Writes a dataframe to the file created.

        This currently does type coercion on write based on the schema.

        Parameters
        ----------
        df : `pd.DataFrame`

        """
        if self.is_closed:
            raise Exception('Writer closed.')

        cdef Py_ssize_t i, j
        field_names = [x['name'] for x in self.schema_json['fields']]
        field_types = [x['type'] for x in self.schema_json['fields']]

        # list of the numpy arrays
        array_list = [fixup_pandas_type(df, name, json_type) for name, json_type in zip(field_names, field_types)]
        cyarr = cvarray(shape=(len(array_list),), itemsize=sizeof(void*), format="L")
        cdef size_t[:] cy_array_view = cyarr

        # Create an array of buffer views for the underlying dataframe columns.  This does not copy.
        cdef np.ndarray temparray
        for i in range(len(field_names)):
            temparray = array_list[i]
            cy_array_view[i] = <long> np.PyArray_DATA(temparray)

        # Initialze a converter per avro field in the root record.
        cdef write_method_t writemethod
        cdef write_method_t* writemethods = <write_method_t*> malloc(len(field_names) * sizeof(write_method_t))
        for i in range(len(field_types)):
            field_type = field_types[i]
            writemethods[i] = get_write_method(field_type, array_list[i])

        cdef avro_value_iface_t  *iface
        cdef avro_value_t record
        cdef avro_value_t field

        # Primary loop that walks the input dataframe and writes the values
        try:
            iface = avro_generic_class_from_schema(self._schema)
            avro_generic_value_new(iface, &record)
            try:
                for j in range(df.shape[0]):
                    for i in range(len(field_names)):
                        # Retrieve avro field and populate using appropriate write method
                        avro_value_get_by_index(&record, i, &field, NULL)
                        writemethod = writemethods[i]
                        writemethod(field, <void*>cy_array_view[i], j, field_types[i])

                    avro_file_writer_append_value(self._writer, &record)
                    avro_value_reset(&record)
            finally:
                # avro is reference counted so we need to release our reference.
                avro_value_decref(&record)
                avro_value_iface_decref(iface)
        finally:
            free(writemethods)

    def close(self):
        """Closes the writer object and flushes the file.

        """
        avro_file_writer_flush(self._writer)
        avro_file_writer_close(self._writer)
        self.is_closed = 1


cdef convert_string_to_unicode(stringobj):
    # Python 2.7 helper for unicode fixup
    if isinstance(stringobj, unicode):
        return stringobj
    return unicode(stringobj, 'utf-8')


def prepare_pandas_df_for_write(df, schema, copy=True):
    """Prepare a dataframe for writing to a given schema.

    Parameters
    ----------
    df : `pd.DataFrame`
    schema : dict or string
        An avro schema (as a string or as a dict).
    copy : bool
        Return a copy of the dataframe, or modify the original dataframe, inplace.

    Returns
    -------
    df : `pd.DataFrame`:
        A pandas dataframe with the dtypes corrected where needed.

    """

    if isinstance(schema, string_types):
        import json
        json_schema = json.loads(schema)
    elif isinstance(schema, dict):
        json_schema = schema
    else:
        raise TypeError('schema should either be a string or a dictionary')

    field_names = [x['name'] for x in json_schema['fields']]
    field_types = [x['type'] for x in json_schema['fields']]

    if copy:
        out = df.copy()
    else:
        out = df

    for name, tp in zip(field_names, field_types):
        out[name] = fixup_pandas_type(out, name, tp)

    return out


def fixup_pandas_type(df, name, json_type):
    """Makes a pandas dataframe correct for the avro writer.

    Parameters
    ----------
    df : `pd.DataFrame`
    name : object
        Column name
    json_type : object
        avro type required.

    Returns
    -------
    s : `pd.Series`
        Casted and coerced appropriately for writing to avro.

    """
    NULL_MESSAGE = """
    Could not convert pandas series to the appropriate type for conversion.

    Unexpected null values found in field `{}`.
      Avro field type {}.

    Consider changing your schema to use union type that allows null.
    """

    series = df[name]
    # fallback
    col = series.values
    # primitives
    if (json_type == 'long') and (col.dtype != np.dtype('int64')):
        if series.isnull().sum():
            raise ValueError(NULL_MESSAGE.format(name, json_type))
        col = col.astype(np.dtype('int64'), subok=False, copy=True)
    elif (json_type == 'int') and (col.dtype != np.dtype('int32')):
        if series.isnull().sum():
            raise ValueError(NULL_MESSAGE.format(name, json_type))
        col = col.astype(np.dtype('int32'), subok=False, copy=True)
    elif (json_type == 'double') and (col.dtype != np.dtype('float64')):
        if series.isnull().sum():
            raise ValueError(NULL_MESSAGE.format(name, json_type))
        col = col.astype(np.dtype('float64'), subok=False, copy=True)
    elif (json_type == 'float') and (col.dtype != np.dtype('float32')):
        if series.isnull().sum():
            raise ValueError(NULL_MESSAGE.format(name, json_type))
        col = col.astype(np.dtype('float32'), subok=False, copy=True)
    elif (json_type == 'bool') or (json_type == 'boolean'):
        if series.isnull().sum():
            raise ValueError(NULL_MESSAGE.format(name, json_type))
        col = col.astype(np.dtype('int32'), subok=False, copy=True)
    # complex types
    elif isinstance(json_type, list):
        all_strings = False
        for elem in json_type:
            if not isinstance(elem, string_types):
                break
        else:
            all_strings = True
        # Union types.
        if all_strings:
            evtset = set(json_type)
            if evtset == {'null', 'double'}:
                col = col.astype(np.dtype('float64'), subok=False, copy=True)
            elif evtset == {'null', 'float'}:
                col = col.astype(np.dtype('float32'), subok=False, copy=True)
            elif evtset == {'null', 'long'}:
                # Do some special handling for datetime64 kinds
                if series.dtype.type is np.datetime64:
                    pass
                else:
                    col = series.fillna(SENTINEL_INT64_NULL).\
                        values.\
                        astype(np.dtype('int64'), subok=False, copy=True)
            elif evtset == {'null', 'int'}:
                col = series.fillna(SENTINEL_INT32_NULL).\
                        values.\
                        astype(np.dtype('int32'), subok=False, copy=True)
    return col

# Write methods for primitives (These are all `write_method_t`)
#
# They all follow the same pattern
#
# * Cast the array pointer to the appropriate type.
# * Get value from array pointer by offset
# * Call the appropriate libavro write method

cdef void write_boolean(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int32_t* view = <int32_t*> array
    cdef int val = view[offset]
    avro_value_set_boolean(&field, view[offset])

cdef void write_int32(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int32_t* view = <int32_t*> array
    avro_value_set_int(&field, view[offset])

cdef void write_int64(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int64_t* view = <int64_t*> array
    avro_value_set_long(&field, view[offset])

cdef void write_pandasdatetime(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int64_t* view = <int64_t*> array
    cdef int64_t val = view[offset]
    avro_value_set_long(&field, val / 1000000)

cdef void write_float32(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef float* view = <float*> array
    avro_value_set_float(&field, view[offset])

cdef void write_float64(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef double* view = <double*> array
    avro_value_set_double(&field, view[offset])

cdef void write_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    avro_value_set_null(&field)

cdef void write_string(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef void** view = <void**> array
    cdef object stringobj = <object> view[offset]
    py_byte_string = convert_string_to_unicode(stringobj).encode('UTF-8')
    cdef const char *c_string = py_byte_string
    # The null terminator is included in the length of the string
    avro_value_set_string_len(&field, c_string, len(py_byte_string)+1)

cdef void write_bytes(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef void** view = <void**> array
    cdef object stringobj = <object> view[offset]
    cdef bytes py_byte_string = stringobj
    cdef const char *c_string = py_byte_string
    avro_value_set_bytes(&field, c_string, len(py_byte_string))

cdef void write_fixed(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef void** view = <void**> array
    cdef object stringobj = <object> view[offset]
    cdef bytes py_byte_string = stringobj
    cdef const char *c_string = py_byte_string
    avro_value_set_fixed(&field, c_string, len(py_byte_string))


# null capable specializations of writer methods.  These drastically improve performance for commonly used
# union types like ["long", "null"]
from libc.math cimport isnan

cdef void get_type_branch(avro_value_t *field, avro_value_t *branch, avro_type_t desired_type):
    """For a given avro_value_t set the avro_branch to be for the desired avro type.

    Modifies *branch
    """
    cdef size_t count, i
    cdef avro_schema_t schema
    schema = avro_value_get_schema(field)
    cdef avro_type_t avro_type

    count = avro_schema_union_size(schema)
    for i in xrange(count):
        avro_value_set_branch(field, i, branch)
        avro_value_get_current_branch(field, branch)
        avro_type = avro_value_get_type(branch)
        if avro_type == desired_type:
            break


cdef void write_float32_or_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef float * view = <float*> array
    cdef float val = view[offset]
    cdef avro_value_t branch
    if isnan(val):
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    else:
        get_type_branch(&field, &branch, AVRO_FLOAT)
        avro_value_set_float(&branch, val)


cdef void write_float64_or_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef double * view = <double *> array
    cdef double val = view[offset]
    cdef avro_value_t branch
    if isnan(val):
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    else:
        get_type_branch(&field, &branch, AVRO_DOUBLE)
        avro_value_set_double(&branch, val)


cdef void write_pandasdatetime_or_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int64_t* view = <int64_t*> array
    cdef int64_t val = view[offset]
    cdef avro_value_t branch
    if val == PANDAS_NaT:
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    else:
        get_type_branch(&field, &branch, AVRO_INT64)
        avro_value_set_long(&branch, val / 1000000)


cdef void write_long_or_sentinel_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int64_t* view = <int64_t*> array
    cdef int64_t val = view[offset]
    cdef avro_value_t branch
    if val == SENTINEL_INT64_NULL:
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    else:
        get_type_branch(&field, &branch, AVRO_INT64)
        avro_value_set_long(&branch, val)


cdef void write_int_or_sentinel_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef int32_t* view = <int32_t*> array
    cdef int32_t val = view[offset]
    cdef avro_value_t branch
    if val == SENTINEL_INT32_NULL:
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    else:
        get_type_branch(&field, &branch, AVRO_INT32)
        avro_value_set_int(&branch, val)


cdef void write_string_or_null(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    cdef void** view = <void**> array
    cdef object stringobj = <object> view[offset]
    cdef avro_value_t branch
    cdef const char *c_string
    if (stringobj is None) or (isinstance(stringobj, string_types) and (len(stringobj) == 0)):
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)
    elif isinstance(stringobj, string_types):
        get_type_branch(&field, &branch, AVRO_STRING)
        py_byte_string = convert_string_to_unicode(stringobj).encode('UTF-8')
        c_string = py_byte_string
        # the trailing null terminator must be included in the length.
        avro_value_set_string_len(&branch, c_string, len(py_byte_string) + 1)
    else:
        # Deal with other unexpected types like numpy NaN values
        get_type_branch(&field, &branch, AVRO_NULL)
        avro_value_set_null(&branch)

cdef void write_generic(avro_value_t field, void * array, Py_ssize_t offset, object schema_type):
    """Generic write method based on the JSON spec.
    This will perform implicit type coersion if needed.
    Exists because write_generic_pyval does not follow the standard signature.
    """
    cdef void ** view = <void **> array
    cdef object val = <object> view[offset]
    write_generic_pyval(field, val, schema_type)


cdef matches_type(val, avro_type):
    """Matching method used to determine union branches.
    """
    cdef double doubleval
    if avro_type == 'null':
        if val is None:
            return True
        elif hasattr(val, '__len__'):
            return len(val) == 0
        elif isinstance(val, float):
            doubleval = val
            return bool(isnan(doubleval))
    elif avro_type == 'string':
        if isinstance(val, string_types):
            return len(val) > 0
    elif avro_type in {'bytes', 'fixed'}:
        if isinstance(val, binary_type):
            return len(val) > 0
    elif avro_type == 'int':
        return isinstance(val, int)
    elif avro_type == 'long':
        return isinstance(val, int)
    elif avro_type == 'float':
        return isinstance(val, float)
    elif avro_type == 'double':
        return isinstance(val, float)
    elif avro_type == 'bool':
        return isinstance(val, bool)
    elif avro_type == 'boolean':
        return isinstance(val, bool)
    elif isinstance(avro_type, list):
        return False
    elif isinstance(avro_type, dict):
        avt = avro_type['type']
        if (avt == 'map') or (avt == 'record'):
            if hasattr(val, '_asdict'):
                return True
            return isinstance(val, dict)
        elif avt == 'array':
            return hasattr(val, '__iter__')
    return False


cdef write_generic_pyval_map_element(avro_value_t field, object key, object val, object schema_type):
    keyval = convert_string_to_unicode(key)
    py_byte_string = keyval.encode('UTF-8')
    cdef const char *c_string = py_byte_string
    cdef avro_value_t child
    cdef size_t index
    cdef int is_new
    avro_value_add(&field, c_string, &child, &index, &is_new)
    write_generic_pyval(child, val, schema_type)


cdef write_generic_pyval(avro_value_t field, object val, object schema_type):
    """Generic method used to write PyObject types to avro.  This is to write complex types.
    """
    cdef:
        int32_t val_int32_t
        int64_t val_int64_t
        float val_float32
        double val_float64
        const char *c_string
        int val_boolean
        Py_ssize_t i
        size_t union_branch
        avro_value_t branch
        avro_value_t child
        size_t new_index

    # Primitive types
    if isinstance(schema_type, string_types):
        if schema_type == 'int':
            val_int32_t = val
            avro_value_set_int(&field, val_int32_t)
        elif schema_type == 'long':
            val_int64_t = val
            avro_value_set_long(&field, val_int64_t)
        elif schema_type == 'float':
            val_float32 = val
            avro_value_set_float(&field, val_float32)
        elif schema_type == 'double':
            val_float64 = val
            avro_value_set_double(&field, val_float64)
        elif schema_type == 'string':
            py_byte_string = convert_string_to_unicode(val).encode('UTF-8')
            c_string = py_byte_string
            # The trailing null pointer must be included as part of the length
            avro_value_set_string_len(&field, c_string, len(py_byte_string) + 1)
        elif schema_type == 'bytes':
            py_byte_string = val
            c_string = py_byte_string
            avro_value_set_bytes(&field, c_string, len(py_byte_string))
        elif (schema_type == 'bool') or (schema_type == 'boolean'):
            val_boolean = int(val)
            avro_value_set_boolean(&field, val_boolean)
        elif schema_type == 'null':
            avro_value_set_null(&field)
        else:
            raise NotImplementedError('Unknown primitive {0!r}'.format(schema_type))
    # Union types
    elif isinstance(schema_type, list):
        union_branch = -1
        for i in range(len(schema_type)):
            if matches_type(val, schema_type[i]):
                union_branch = i
                break
        if union_branch != -1:
            avro_value_set_branch(&field, union_branch, &branch)
            avro_value_get_current_branch(&field, &branch)
            write_generic_pyval(branch, val, schema_type[i])
    # Compound types
    elif isinstance(schema_type, dict):
        compound_type = schema_type['type']
        # Array
        if compound_type == 'array':
            array_type = schema_type['items']
            if hasattr(val, '__iter__'):
                for elem in val:
                    avro_value_append(&field, &child, &new_index)
                    write_generic_pyval(child, elem, array_type)
            else:
                raise Exception('Schema requires array, Object {0!r} is not iterable'.format(val))
        # Map
        elif compound_type == 'map':
            map_type = schema_type['values']
            if hasattr(val, '_asdict'): # Named tuple support
                val = val._asdict()
            if isinstance(val, dict):
                for key, elem in iteritems(val):
                    write_generic_pyval_map_element(field, key, elem, map_type)
            else:
                raise Exception('Schema requires map, Object {0!r} is not dictionary'.format(val))
        # Subrecord
        elif compound_type == 'record':
            if hasattr(val, '_asdict'): # Named tuple support
                val = val._asdict()
            if not isinstance(val, dict):
                raise NotImplemented('record types expect a dict or something with a _asdict method')
            for union_branch, record_field in enumerate(schema_type['fields']):
                field_name = record_field['name']
                avro_value_get_by_index(&field, union_branch, &child, NULL)
                write_generic_pyval(child, val[field_name], record_field['type'])
        # Fixed length byte arrays
        elif compound_type == 'fixed':
            print(repr(val), type(val))
            py_byte_string = val
            c_string = py_byte_string
            avro_value_set_fixed(&field, c_string, len(py_byte_string))
        else:
            raise NotImplementedError('Unknown compound {0!r}'.format(schema_type))
    else:
        raise NotImplementedError('Unknown type {0!r}'.format(schema_type))


cdef write_method_t get_write_method(object json_type, object array):
    """Map from an avro json_type to the appropriate `write_method_t`.

    This is only called once per field written
    """
    if json_type == 'int':
        return write_int32
    elif json_type == 'long':
        if array.dtype.type is np.datetime64:
            return write_pandasdatetime
        else:
            return write_int64
    elif json_type == 'float':
        return write_float32
    elif json_type == 'double':
        return write_float64
    elif json_type == 'string':
        return write_string
    elif json_type == 'bool':
        return write_boolean
    elif json_type == 'boolean':
        return write_boolean
    elif json_type == 'bytes':
        return write_bytes
    elif json_type == 'fixed':
        return write_fixed
    elif isinstance(json_type, list):
        # Deal with union types
        all_strings = True
        for elem in json_type:
            all_strings = all_strings and isinstance(elem, string_types)
            if not all_strings:
                break
        if all_strings:
            evtset = set(json_type)
            if 'null' in json_type:
                if evtset == {'null', 'double'}:
                    return write_float64_or_null
                elif evtset == {'null', 'float'}:
                    return write_float32_or_null
                elif evtset == {'null', 'long'}:
                    # Do some special handling for datetime64 kinds
                    if array.dtype.type is np.datetime64:
                        return write_pandasdatetime_or_null
                    else:
                        return write_long_or_sentinel_null
                elif evtset == {'null', 'int'}:
                    return write_int_or_sentinel_null
                elif evtset == {'null', 'string'}:
                    return write_string_or_null
    elif isinstance(json_type, dict):
        compound_type = json_type['type']
        if compound_type == 'fixed':
            return write_fixed
    return write_generic
