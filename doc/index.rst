.. cyavro documentation master file, created by
   sphinx-quickstart on Fri Jun 20 08:15:58 2014.
   You can adapt this file completely to your liking, but it should at least
   contain the root `toctree` directive.

cyavro
======

..raw:: html

   <table>
   <tr>
     <td>Latest Release</td>
     <td><img src="https://img.shields.io/pypi/v/cyavro.svg" alt="latest release" /></td>
   </tr>
   <tr>
     <td>License</td>
     <td>
       <a href="https://github.com/bokeh/bokeh/blob/master/LICENSE.txt">
       <img src="https://img.shields.io/github/license/MaxPoint/cyavro.svg" alt="Bokeh license" />
       </a>
     </td>
   </tr>
   <tr>
     <td>Build Status</td>
     <td>
       <a href="https://travis-ci.org/MaxPoint/cyavro">
       <img src="https://travis-ci.org/MaxPoint/cyavro.svg" alt="build status" />
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
Installing cyavro requires several c libraries to be present.  The simplest way to build and install cyavro
is by using the conda recipes provided.  Building these should work on linux and mac.
Windows is unsupported.


.. code-block:: bash

    cd conda-recipes
    conda build cyavro

Simple Usage
------------

.. code-block:: python
    import cyavro
    cyavro.read_avro_file_as_dataframe("/path/to/somefile.avro")


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

