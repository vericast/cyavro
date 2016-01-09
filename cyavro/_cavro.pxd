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
# THIS SOFTWARE IS PROVIDED BY THE COPYRIG`HT HOLDERS AND CONTRIBUTORS "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES,
# INCLUDING, BUT NOT LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
# DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
# SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY,
# WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

# This is a set of headers wrapping "avro.h" provided from libavro.
# These are required for the c api exposed by libavro to be importable in Cython.

cimport numpy as np

from libc.stdint cimport int32_t, int64_t
from libc.stdlib cimport *
from libc.stdio cimport *


cdef extern from 'avro.h':
    #'basics.h':
    cdef enum avro_type_t:
        AVRO_STRING,
        AVRO_BYTES,
        AVRO_INT32,
        AVRO_INT64,
        AVRO_FLOAT,
        AVRO_DOUBLE,
        AVRO_BOOLEAN,
        AVRO_NULL,
        AVRO_RECORD,
        AVRO_ENUM,
        AVRO_FIXED,
        AVRO_MAP,
        AVRO_ARRAY,
        AVRO_UNION,
        AVRO_LINK
    ctypedef avro_type_t avro_type_t

    cdef enum avro_class_t:
        AVRO_SCHEMA,
        AVRO_DATUM
    ctypedef avro_class_t avro_class_t

    #'schema.h':
    struct avro_obj_t:
        pass
    ctypedef avro_obj_t *avro_schema_t

    size_t avro_schema_union_size(const avro_schema_t union_schema)

    int avro_schema_decref(avro_schema_t schema)

    struct avro_schema_error_t_:
        pass
    ctypedef avro_schema_error_t_ *avro_schema_error_t

    int avro_schema_from_json(const char *jsontext, int32_t unused1,
              avro_schema_t *schema, avro_schema_error_t *unused2)

    #'value.h':
    struct avro_value_iface:
        pass
    ctypedef avro_value_iface avro_value_iface_t

    cdef struct avro_value:
        avro_value_iface_t  *iface
        void  *self
    ctypedef avro_value avro_value_t

    # Reference Counting
    void avro_value_incref(avro_value_t *value)
    void avro_value_decref(avro_value_t *value)

    # General methods
    avro_type_t avro_value_get_type(avro_value_t *value)
    avro_schema_t avro_value_get_schema(avro_value_t *value)
    int avro_value_reset(avro_value_t *value)

    # Primitive Getters

    int avro_value_get_boolean(avro_value_t *value, int *out)
    int avro_value_get_bytes(avro_value_t *value, const void **buf, size_t *size)
    int avro_value_get_double(avro_value_t *value, double *out)
    int avro_value_get_float(avro_value_t *value, float *out)
    int avro_value_get_int(avro_value_t *value, int32_t *out)
    int avro_value_get_long(avro_value_t *value, int64_t *out)
    int avro_value_get_null(avro_value_t *value)

    #''' /* The result will be NUL-terminated the size will INCLUDE the
    # * NUL terminator.  str will never be NULL unless there's an
    # * error. */'''

    int avro_get_bytes(avro_value_t *value, const void **buf, size_t *size)
    int avro_value_get_string(avro_value_t *value, const char **str, size_t *size)

    int avro_value_get_enum(avro_value_t *value, int *out)
    int avro_value_get_fixed(avro_value_t *value, const void **buf, size_t *size)

    # Compound Getters
    int avro_value_get_size(avro_value_t *value, size_t *size)
    int avro_value_get_by_index(avro_value_t *value, size_t index, avro_value_t *child, const char **name)
    int avro_value_get_by_name(avro_value_t *value, const char *name, avro_value_t *child, size_t *index)

    # Union Types
    int avro_value_get_discriminant(avro_value_t *value, int *out)
    int avro_value_get_current_branch(avro_value_t *value, avro_value_t *branch)

    ## Setters
    int avro_value_set_boolean(avro_value_t *value, int val)
    int avro_value_set_double(avro_value_t *value, double val)
    int avro_value_set_float(avro_value_t *value, float val)
    int avro_value_set_int(avro_value_t *value, int32_t val)
    int avro_value_set_long(avro_value_t *value, int64_t val)
    int avro_value_set_null(avro_value_t *value)
    int avro_value_set_bytes(avro_value_t *value, void *buf, size_t size)
    int avro_value_set_fixed(avro_value_t *value, void *buf, size_t size)
    int avro_value_set_string_len(avro_value_t *value, const char *str, size_t size)
    int avro_value_set_enum(const avro_value_t *value, int val)

    # Compound value setters
    int avro_value_append(avro_value_t *value, avro_value_t *child_out, size_t *new_index)
    int avro_value_add(avro_value_t *value, const char *key, avro_value_t *child, size_t *new_index, int *is_new)

    # union types
    int avro_value_set_branch(avro_value_t *value, int discriminant, avro_value_t *branch)

    #'generic.h':

    avro_value_iface_t * avro_generic_class_from_schema(avro_schema_t schema)

    #/**
    #* Allocate a new instance of the given generic value class.  @a iface
    #* must have been created by @ref avro_generic_class_from_schema.
    #*/

    int avro_generic_value_new(avro_value_iface_t *iface, avro_value_t *dest)

    void avro_value_iface_decref(avro_value_iface_t *iface)
    void avro_value_iface_incref(avro_value_iface_t *iface)

    #"io.h":
    # Struct infer from header
    struct avro_reader_t_:
        pass
    ctypedef avro_reader_t_ *avro_reader_t

    struct avro_writer_t_:
        pass
    ctypedef avro_writer_t_ *avro_writer_t

    struct avro_file_reader_t_:
        pass
    ctypedef avro_file_reader_t_ *avro_file_reader_t

    struct avro_file_writer_t_:
        pass
    ctypedef avro_file_writer_t_ *avro_file_writer_t

    int avro_file_writer_create_with_codec(const char *path,
                avro_schema_t schema, avro_file_writer_t * writer,
                const char *codec, size_t block_size)

    int avro_file_writer_create(const char *path, avro_schema_t schema,
                avro_file_writer_t * writer)

    int avro_file_reader(const char *path, avro_file_reader_t * reader)
    int avro_file_reader_fp(FILE *fp, const char *path, int should_close, avro_file_reader_t * reader)

    avro_schema_t avro_file_reader_get_writer_schema(avro_file_reader_t reader)

    int avro_file_writer_sync(avro_file_writer_t writer)
    int avro_file_writer_flush(avro_file_writer_t writer)
    int avro_file_writer_close(avro_file_writer_t writer)

    int avro_file_reader_close(avro_file_reader_t reader)

    int avro_file_reader_read_value(avro_file_reader_t reader, avro_value_t *dest)
    int avro_file_writer_append_value(avro_file_writer_t writer, avro_value_t *src)

    void avro_reader_reset(avro_reader_t reader)
    void avro_writer_reset(avro_writer_t writer)

    #error handling
    const char* avro_strerror()
