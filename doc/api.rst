.. currentmodule:: cyavro
.. _api

**********************
Extender API Reference
**********************

Class reference for users who want to extend the behavior of cyavro.

To change the default reader and writer classes used by the cyavro functional api.

.. code-block:: python

    cyavro.avro_reader_cls = MyReaderSubclass
    cyavro.avro_writer_cls = MyWriterSubclass

.. autosummary::
    :toctree: generated/

    AvroReader
    AvroWriter
    AvroToHDF
