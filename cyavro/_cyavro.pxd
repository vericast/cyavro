from _cavro cimport *

cdef class AvroReader:
    cdef avro_file_reader_t _reader
    cdef public int chunk_size
    cdef public list refholder, field_names, field_types, buffer_lst
    cdef public str filename
    cdef int empty_file
    cdef int reset_reader

    cdef from_bytes(self, void *buffer, int length)
