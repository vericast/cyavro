.. currentmodule:: cyavro
.. _publicapi


*****************
cyavro public API
*****************

cyavro exposes a simple functional api for reading and writing avro files.  This api is particularly focused on
pandas support.

Reading Single Avro Files
-------------------------

.. autosummary::
    :toctree: generated/

    read_avro_file_as_dataframe_iter
    read_avro_file_as_dataframe


Reading Paths containing Avro Files
-----------------------------------

.. autosummary::
    :toctree: generated/

    read_avro_path_as_dataframe_iter
    read_avro_path_as_dataframe


Conversion Utilities
--------------------

.. autosummary::
    :toctree: generated/

    infer_avro_schema_for_dataframe
    avro_file_to_hdf5
    avro_path_to_hdf5
