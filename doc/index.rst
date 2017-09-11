.. cyavro documentation master file, created by
   sphinx-quickstart on Fri Jun 20 08:15:58 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

cyavro
======

.. raw:: html

   <table>
   <tr>
     <td>Latest Release</td>
     <td><img src="https://img.shields.io/pypi/v/cyavro.svg" alt="latest release" /></td>
   </tr>
   <tr>
     <td>License</td>
     <td>
       <a href="https://github.com/maxpoint/cyavro/blob/master/LICENSE.txt">
       <img src="https://img.shields.io/github/license/maxpoint/cyavro.svg" alt="Bokeh license" />
       </a>
     </td>
   </tr>
   <tr>
     <td>Build Status</td>
     <td>
       <a href="https://travis-ci.org/maxpoint/cyavro">
       <img src="https://travis-ci.org/maxpoint/cyavro.svg" alt="build status" />
       </a>
     </td>
   </tr>
   <tr>
     <td>PyPI</td>
     <td>
       <a href="https://pypi.python.org/pypi/cyavro/">
       <img src="https://img.shields.io/pypi/dm/cyavro.svg" alt="pypi downloads" />
       </a>
     </td>
   </tr>
   </table>


This package provides a substantial speed improvement when reading and writing avro files over the
pure python implementation

Installation
------------
Installing cyavro requires several c libraries to be present.  The simplest way install cyavro
is via conda (for linux and osx):


.. code-block:: bash

    conda install cyavro -c conda-forge

Simple Usage
------------
Read file into one Pandas dataframe:

.. code-block:: python
    df = cyavro.read_avro_file_as_dataframe("/path/to/somefile.avro")

Iterate over chunks of a file:

.. code-block:: python
    it = cyavro.read_avro_file_as_dataframe_iter("/path/to/somefile.avro")
    for df in it:
        work_on(df)

Read file to `dask.dataframe`_  (URL can be glob string and include various
protocols such as s3:, gcs:). This feature is experimental.

.. code-block:: python
    from cython import dask_reader
    df = dask_reader.read_avro('s3://mybucket/path/files.*.avro')

.. _dask.dataframe: http://dask.pydata.org/en/latest/dataframe-api.html

Contents
========

.. toctree::
   :maxdepth: 2

   cyavro_api
   api

Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`

