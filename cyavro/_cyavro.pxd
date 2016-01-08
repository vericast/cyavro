from _cavro cimport *

cdef class AvroReader:
    cdef avro_file_reader_t _reader
    cdef public int chunk_size
    cdef public list refholder, field_names, field_types, buffer_lst
    cdef public str filename
    cdef int empty_file
    cdef void *fp_reader_buffer
    cdef int fp_reader_buffer_length

    cdef init_reader_buffer(self)

cdef reader_from_bytes_c(void *buffer, int length)
