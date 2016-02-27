from _cavro cimport *

cdef enum AvroReaderType:
    avro_reader_type_unset,
    avro_reader_type_bytes,
    avro_reader_type_file

cdef class AvroReader:
    cdef avro_file_reader_t _reader
    cdef public int chunk_size
    cdef public list refholder, field_names, field_types, buffer_lst
    cdef public str filename
    cdef int initialized, empty_file, _should_free_buffer
    cdef void *fp_reader_buffer
    cdef int fp_reader_buffer_length

    cdef public bytes filedata
    cdef int filedatalength
    cdef AvroReaderType reader_type

    cdef init_reader_buffer(self)
    cdef init_file_reader(self)

    cdef init_memory_reader(self)



cdef reader_from_bytes_c(void *buffer, int length)
