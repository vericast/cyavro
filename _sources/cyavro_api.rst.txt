.. currentmodule:: cyavro
.. _publicapi


*****************
cyavro public API
*****************

cyavro exposes a simple functional api for reading and writing avro files.  This api is particularly focused on pandas support.

Reading Single Avro Files
-------------------------

.. autosummary::
    :toctree: generated/

    read_avro_file_as_dataframe_iter
    read_avro_file_as_dataframe


Reading Paths containing Avro Files
-----------------------------------

Frequently avro files are generated in large quantities by spark/mapreduce.  As such it is useful to have a convenience method to read a whole path of avro files.

.. autosummary::
    :toctree: generated/

    read_avro_path_as_dataframe_iter
    read_avro_path_as_dataframe

Reading file-like objects
-------------------------

These functions are useful for reading avro from a utility that provides you with an in-memory copy of the file.  These functions do make a copy of the file internally so if you do not have enough memory to read the entire buffer into memory use the file-based methods.

.. autosummary::
    :toctree: generated/

    read_avro_bytesio_as_dataframe_iter
    read_avro_bytesio_as_dataframe


Conversion Utilities
--------------------

.. autosummary::
    :toctree: generated/

    infer_avro_schema_for_dataframe
    avro_file_to_hdf5
    avro_path_to_hdf5
